package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/redis/go-redis/v9"
)

// deviceIdPattern valida o :deviceId da URL antes de interpolá-lo nas queries
// do Influx e nas chaves do Redis.
var deviceIdPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type SensorMessage struct { // para o payload do RabbitMQ
	UserId        string      `json:"userId"`
	ApplicationId string      `json:"applicationId"`
	DeviceType    string      `json:"deviceType"`
	DevAddr       string      `json:"devAddr"`
	DevEUI        string      `json:"devEUI"`
	Payload       interface{} `json:"payload"` // interface{} permite receber qualquer JSON interno
}

func main() {
	// Configurações via Variáveis de Ambiente
	influxURL := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	org := os.Getenv("INFLUX_ORG")
	bucket := os.Getenv("INFLUX_BUCKET")
	redisAddr := os.Getenv("REDIS_ADDR")
	authDomain := os.Getenv("AUTH_DOMAIN")
	authAudience := os.Getenv("AUTH_AUDIENCE")

	if authDomain == "" || authAudience == "" {
		log.Fatal("AUTH_DOMAIN e AUTH_AUDIENCE são obrigatórios")
	}

	// Conector InfluxDB
	client := influxdb2.NewClient(influxURL, token)
	queryAPI := client.QueryAPI(org)
	defer client.Close()

	// Conector Redis. Exige senha porque o Redis passou a escutar no IP privado da
	// VM (para ser alcançável pelo Container Apps), e não só na rede do compose.
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	// Testa conexão
	_, err := rdb.Ping(context.Background()).Result()

	if err != nil {
		log.Fatal("Erro ao conectar no Redis:", err)
	}

	// Cadastro de sensores (Supabase) — mesma tabela que o mqtt-sub consulta na
	// ingestão. Opcional: sem credenciais a API sobe do mesmo jeito e as rotas
	// que dependem do cadastro respondem 503.
	supa, err := newSupabaseClient()
	if err != nil {
		log.Fatal("Erro ao inicializar o cliente do Supabase:", err)
	}
	if supa == nil {
		log.Println("AVISO: SUPABASE_URL/SUPABASE_KEY ausentes — cadastro de sensores desabilitado")
	} else {
		log.Println("Cadastro de sensores (Supabase) habilitado")
	}

	// Chaves públicas do Auth0 para validar os JWTs. Retry no boot porque o
	// container pode subir antes da rede/DNS estarem prontos.
	var jwks keyfunc.Keyfunc
	for attempt := 1; ; attempt++ {
		jwks, err = newJWKS(authDomain)
		if err == nil {
			break
		}
		if attempt >= 3 {
			log.Fatal("Erro ao inicializar JWKS:", err)
		}
		log.Printf("JWKS tentativa %d falhou (%v), tentando de novo...", attempt, err)
		time.Sleep(time.Duration(attempt) * 5 * time.Second)
	}

	log.Println("Conexões estabelecidas com sucesso")

	app := fiber.New()

	// CORS precisa vir ANTES do auth: o preflight OPTIONS do browser não
	// carrega o header Authorization.
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,OPTIONS",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
	}))

	// Healthcheck sem autenticação (monitoramento e smoke tests).
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

	// Todas as rotas registradas a partir daqui exigem JWT válido do Auth0.
	// Nos handlers, use GetAuthUser(c) para obter o usuário autenticado.
	app.Use(authMiddleware(jwks, authAudience, authDomain))

	// Cadastro: quais sensores pertencem ao usuário autenticado.
	// O front usa isto para montar a lista sem depender de haver leitura no
	// cache — um sensor recém-cadastrado, que ainda não publicou nada, aparece.
	app.Get("/api/sensors/devices", func(c *fiber.Ctx) error {
		user := GetAuthUser(c)

		if supa == nil {
			return c.Status(503).JSON(fiber.Map{
				"error": "cadastro de sensores indisponível: Supabase não configurado",
			})
		}

		devices, err := fetchDevicesForUser(supa, user.UserID)
		if err != nil {
			return c.Status(502).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"usuario": user.UserID,
			"total":   len(devices),
			"devices": devices,
		})
	})

	// 1. GET Histórico (InfluxDB)
	// O userId NÃO vem da URL: sai da claim do JWT validado pelo authMiddleware.
	// Assim um usuário não consegue ler os sensores de outro trocando o parâmetro.
	app.Get("/api/sensors/influx/:days/:devAddr", func(c *fiber.Ctx) error {
		user := GetAuthUser(c)
		days := c.Params("days")
		devAddr := c.Params("devAddr")

		if _, err := strconv.Atoi(days); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "days deve ser um número inteiro"})
		}
		if !deviceIdPattern.MatchString(devAddr) {
			return c.Status(400).JSON(fiber.Map{"error": "devAddr inválido"})
		}

		// O identificador do dispositivo aparece com dois nomes de tag no bucket:
		// `devAddr`, gravado pelo connector do fluxo LoRa, e `deviceId`, gravado
		// pelo fluxo do broker próprio. Filtrar só por um esconde metade do
		// histórico, então a query aceita os dois.
		//
		// Converte para float para evitar erro de agregação com strings.
		query := fmt.Sprintf(`from(bucket: "%s")
        |> range(start: -%sd)
        |> filter(fn: (r) => r["_measurement"] == "telemetria")
        |> filter(fn: (r) => r["userId"] == "%s")
        |> filter(fn: (r) => r["devAddr"] == "%s" or r["deviceId"] == "%s")
        |> filter(fn: (r) => r["_field"] == "soil_temperature" or r["_field"] == "soil_moisture" or r["_field"] == "air_humidity" or r["_field"] == "luminosity" or r["_field"] == "air_temperature" or r["_field"] == "battery" or r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "validity")
        |> map(fn: (r) => ({ r with _value: float(v: r._value) }))`, bucket, days, user.UserID, devAddr, devAddr)

		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			// Retorna o erro real vindo do SDK para facilitar o debug
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		// Mapa para agrupar diferentes métricas (campos) que possuem o mesmo timestamp
		// Chave: string do timestamp | Valor: Mapa com os dados do sensor
		groupedData := make(map[string]fiber.Map)

		for result.Next() {
			record := result.Record()
			t := record.Time().Format(time.RFC3339) // Converte o timestamp para RFC3339 para usar como chave (string) no mapa

			// Se ainda não iniciamos esse timestamp no mapa, criamos a estrutura base
			if _, ok := groupedData[t]; !ok {
				// Pontos do fluxo do broker próprio só têm a tag `deviceId`;
				// sem este fallback o front recebe devAddr/devEUI vazios.
				deviceID := record.ValueByKey("deviceId")
				devAddrOut := record.ValueByKey("devAddr")
				if devAddrOut == nil || devAddrOut == "" {
					devAddrOut = deviceID
				}
				devEUIOut := record.ValueByKey("devEUI")
				if devEUIOut == nil || devEUIOut == "" {
					devEUIOut = deviceID
				}

				groupedData[t] = fiber.Map{
					"userId":        record.ValueByKey("userId"),
					"timestamp":     record.Time().Unix(), // Exibe o timestamp como inteiro (Unix) para facilitar o uso no frontend
					"applicationId": record.ValueByKey("applicationId"),
					"devAddr":       devAddrOut,
					"deviceType":    record.ValueByKey("deviceType"),
					"devEUI":        devEUIOut,
					"value":         make(map[string]interface{}),
				}
			}

			// Adiciona a métrica atual (umidade, temp, etc) dentro do campo value
			value := groupedData[t]["value"].(map[string]interface{})
			value[record.Field()] = record.Value()
		}

		// Converte o mapa para um slice (lista) para o JSON final ficar ordenado
		var finalResponse []fiber.Map
		for _, val := range groupedData {
			finalResponse = append(finalResponse, val)
		}

		return c.JSON(finalResponse)
	})

	// ISSO PEGA DO CACHE (REDIS)
	app.Get("/api/sensors/latest/:devEUI", func(c *fiber.Ctx) error {
		user := GetAuthUser(c)
		devEUI := c.Params("devEUI")

		if !deviceIdPattern.MatchString(devEUI) {
			return c.Status(400).JSON(fiber.Map{"error": "devEUI inválido"})
		}

		cacheKey := fmt.Sprintf("userId:%s:devEUI:%s:history", user.UserID, devEUI)

		// Pega todos os itens da lista (do 0 ao -1 significa "tudo")
		vals, err := rdb.LRange(c.Context(), cacheKey, 0, -1).Result()
		if err != nil || len(vals) == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Sem dados no cache"})
		}

		// Como as strings no Redis já são JSONs, vamos montar um array de JSONs manualmente
		// ou decodificar e enviar. O mais simples para o Fiber:
		return c.SendString("[" + strings.Join(vals, ",") + "]")
	})

	app.Get("/api/sensors/all", func(c *fiber.Ctx) error {
		user := GetAuthUser(c)

		// 1. As chaves saem do cadastro no Supabase quando ele está disponível.
		// Assim não é preciso varrer o keyspace do Redis — que é compartilhado
		// por todas as fazendas — a cada requisição de um único usuário.
		var keys []string
		if supa != nil {
			devices, err := fetchDevicesForUser(supa, user.UserID)
			if err != nil {
				log.Printf("cadastro indisponível (%v); usando varredura do cache", err)
			}
			for _, d := range devices {
				keys = append(keys, fmt.Sprintf("userId:%s:devEUI:%s:history", user.UserID, d.DevEUI))
			}
		}

		// 2. Sem cadastro (ou cadastro vazio): varre o cache com SCAN, que
		// devolve o controle entre os lotes. Nunca KEYS, que bloqueia o Redis
		// durante a varredura inteira.
		if len(keys) == 0 {
			pattern := fmt.Sprintf("userId:%s:devEUI:*:history", user.UserID)
			iter := rdb.Scan(c.Context(), 0, pattern, 100).Iterator()
			for iter.Next(c.Context()) {
				keys = append(keys, iter.Val())
			}
			if err := iter.Err(); err != nil {
				return c.Status(500).JSON(fiber.Map{"error": "Erro ao escanear dispositivos"})
			}
		}

		if len(keys) == 0 {
			return c.Status(404).JSON(fiber.Map{"message": "Nenhum sensor ativo encontrado"})
		}

		// 3. Última leitura de cada dispositivo num único round-trip, em vez de
		// um LINDEX sequencial por sensor.
		pipe := rdb.Pipeline()
		cmds := make([]*redis.StringCmd, len(keys))
		for i, key := range keys {
			cmds[i] = pipe.LIndex(c.Context(), key, 0)
		}
		if _, err := pipe.Exec(c.Context()); err != nil && err != redis.Nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao ler o cache"})
		}

		var statusGeral []interface{}
		for _, cmd := range cmds {
			val, err := cmd.Result()
			if err != nil {
				// Sensor cadastrado que ainda não publicou nada: sem leitura no cache.
				continue
			}
			var lastRead interface{}
			if json.Unmarshal([]byte(val), &lastRead) == nil {
				statusGeral = append(statusGeral, lastRead)
			}
		}

		return c.JSON(fiber.Map{
			// "usuario" é o nome que o front já consome (fetch-sensors-latest.ts).
			// "userId" vai junto para alinhar com a nomenclatura do develop.
			"usuario":            user.UserID,
			"userId":             user.UserID,
			"total_dispositivos": len(statusGeral),
			"leituras":           statusGeral,
		})
	})

	log.Fatal(app.Listen(":3000"))
}
