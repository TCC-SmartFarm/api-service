package main

import (
	"context"
	"fmt"
	"log"
	"encoding/json" 
	"os"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/influxdata/influxdb-client-go/v2"
	"github.com/redis/go-redis/v9"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SensorMessage struct { // para o payload do RabbitMQ
	UserId     string      `json:"userId"`
	DeviceType string      `json:"deviceType"`
	DeviceId   string      `json:"deviceId"`
	Payload    interface{} `json:"payload"` // interface{} permite receber qualquer JSON interno
}

func main() {
	// Configurações via Variáveis de Ambiente
	influxURL := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	org := os.Getenv("INFLUX_ORG")
	bucket := os.Getenv("INFLUX_BUCKET")
	rabbitURL := os.Getenv("RABBIT_URL")
	redisAddr := os.Getenv("REDIS_ADDR")
	// Auth/JWT
	authDomain := os.Getenv("AUTH_DOMAIN")
	authAudience := os.Getenv("AUTH_AUDIENCE")

	// Conector InfluxDB
	client := influxdb2.NewClient(influxURL, token)
	queryAPI := client.QueryAPI(org)
	defer client.Close()

	// Conector RabbitMQ
	rabbitConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatal("Erro ao conectar no RabbitMQ:", err)
	}
	defer rabbitConn.Close()

	// Conector Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// JWKS do Auth0 (busca chaves públicas RS256 p/ validar o JWT)
	jwks, err := newJWKS(authDomain)
	if err != nil {
		log.Fatal("Erro ao inicializar JWKS:", err)
	}

	// Testa conexão
	_, err = rdb.Ping(context.Background()).Result()

	if err != nil {
		log.Fatal("Erro ao conectar no Redis:", err)
	}

	log.Println("Conexões estabelecidas com sucesso")

	app := fiber.New()
	app.Use(cors.New())

	// Todas as rotas abaixo desse middleware exigirão autenticação JWT válida.
	app.Use(
		authMiddleware(
			jwks,
			authAudience,
			authDomain,
		),
	)


	// 1. GET Histórico (InfluxDB)
	app.Get("/api/sensors/influx/:userID/:days/:deviceId", func(c *fiber.Ctx) error {
		// userID := c.Params("userID")

		// USER ID VINDO DO JWT
		userID := c.Locals("userID").(string)
		
		days := c.Params("days")
		deviceId := c.Params("deviceId")

		// Query corrigida: converte para float para evitar erro de agregação com strings
		query := fmt.Sprintf(`from(bucket: "%s")
        |> range(start: -%sd)
        |> filter(fn: (r) => r["_measurement"] == "telemetria")
        |> filter(fn: (r) => r["userId"] == "%s")
        |> filter(fn: (r) => r["deviceId"] == "%s")
        |> filter(fn: (r) => r["_field"] == "temperatura" or r["_field"] == "umidade" or r["_field"] == "ph")
        |> map(fn: (r) => ({ r with _value: float(v: r._value) }))`, bucket, days, userID, deviceId)

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
				groupedData[t] = fiber.Map{
					"timestamp":  record.Time().Unix(), // Exibe o timestamp como inteiro (Unix) para facilitar o uso no frontend
					"userId":     record.ValueByKey("userId"),
					"deviceId":   record.ValueByKey("deviceId"),
					"deviceType": record.ValueByKey("deviceType"),
					"value":    make(map[string]interface{}),
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
	app.Get("/api/sensors/latest/:userID/:deviceId", func(c *fiber.Ctx) error {
		// userID := c.Params("userID")

		// USER ID VINDO DO JWT
		userID := c.Locals("userID").(string)

		deviceId := c.Params("deviceId")
		cacheKey := fmt.Sprintf("userId:%s:deviceId:%s:history", userID, deviceId)

		// Pega todos os itens da lista (do 0 ao -1 significa "tudo")
		vals, err := rdb.LRange(c.Context(), cacheKey, 0, -1).Result()
		if err != nil || len(vals) == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Sem dados no cache"})
		}

		// Como as strings no Redis já são JSONs, vamos montar um array de JSONs manualmente
		// ou decodificar e enviar. O mais simples para o Fiber:
		return c.SendString("[" + strings.Join(vals, ",") + "]")
	})


	app.Get("/api/sensors/all/:userID", func(c *fiber.Ctx) error {
		// userID := c.Params("userID")

		// USER ID VINDO DO JWT
		userID := c.Locals("userID").(string)
		
		// 1. Padrão de busca para encontrar as listas de todos os dispositivos do usuário
		// O Cache-Service agora salva como: userId:fazenda1:deviceId:XYZ:history
		pattern := fmt.Sprintf("userId:%s:deviceId:*:history", userID)

		// 2. Localiza todas as chaves (dispositivos) que o usuário possui no cache
		keys, err := rdb.Keys(c.Context(), pattern).Result()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao escanear dispositivos"})
		}

		if len(keys) == 0 {
			return c.Status(404).JSON(fiber.Map{"message": "Nenhum sensor ativo encontrado"})
		}

		var statusGeral []interface{}

		// 3. Para cada chave encontrada, pegamos apenas o PRIMEIRO item (índice 0)
		// O LIndex(ctx, chave, 0) pega a leitura mais recente do buffer de 20
		for _, key := range keys {
			val, err := rdb.LIndex(c.Context(), key, 0).Result()
			if err == nil {
				var lastRead interface{}
				json.Unmarshal([]byte(val), &lastRead)
				statusGeral = append(statusGeral, lastRead)
			}
		}

		return c.JSON(fiber.Map{
			"usuario": userID,
			"total_dispositivos": len(statusGeral),
			"leituras": statusGeral,
		})
	})

	log.Fatal(app.Listen(":3000"))
}