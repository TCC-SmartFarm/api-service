package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/influxdata/influxdb-client-go/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// SensorMessage modela o payload publicado pelo mqtt-sub (e cacheado pelo cache-service).
type SensorMessage struct {
	UserId     string      `json:"userId"`
	DeviceType string      `json:"deviceType"`
	DeviceId   string      `json:"deviceId"`
	Payload    interface{} `json:"payload"`
}

func main() {
	// Configurações via Variáveis de Ambiente
	influxURL := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	org := os.Getenv("INFLUX_ORG")
	bucket := os.Getenv("INFLUX_BUCKET")
	rabbitURL := os.Getenv("RABBIT_URL")
	// Auth/JWT
	authDomain := os.Getenv("AUTH_DOMAIN")
	authAudience := os.Getenv("AUTH_AUDIENCE")
	// Redis (cache populado pelo cache-service)
	redisAddr := os.Getenv("REDIS_ADDR")


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

	// Conector Redis (lê o buffer circular mantido pelo cache-service)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatal("Erro ao conectar no Redis:", err)
	}

	// JWKS do Auth0 (busca chaves públicas RS256 p/ validar o JWT)
	jwks, err := newJWKS(authDomain)
	if err != nil {
		log.Fatal("Erro ao inicializar JWKS:", err)
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
	// Removemos ":userID" da URL. Agora o usuário vem do JWT autenticado.
	// Isso impede acesso a dados de outros usuários.
	app.Get("/api/sensors/:days/:deviceId", func(c *fiber.Ctx) error {

		// USER ID VINDO DO JWT

		userID := c.Locals("userID").(string)

		// PARAMS

		days := c.Params("days")
		deviceId := c.Params("deviceId")

		// VALIDAÇÃO DE INPUT

		// Evita injection e inputs inválidos
		if _, err := strconv.Atoi(days); err != nil {

			return c.Status(400).JSON(
				fiber.Map{
					"error": "days inválido",
				},
			)
		}
 
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

	// 2. GET últimas leituras de todos os dispositivos do usuário (lê do cache Redis).
	// O cache-service mantém um buffer circular de 20 leituras por device em chaves
	// no padrão "userId:<sub>:deviceId:<deviceId>:history".
	app.Get("/api/sensors/latest", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(string)

		pattern := fmt.Sprintf("userId:%s:deviceId:*:history", userID)
		keys, err := rdb.Keys(c.Context(), pattern).Result()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao buscar dados do cache"})
		}

		if len(keys) == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Fila do usuário não encontrada"})
		}

		var allMessages []SensorMessage
		for _, key := range keys {
			val, err := rdb.LIndex(c.Context(), key, 0).Result()
			if err != nil {
				continue
			}
			var sensorData SensorMessage
			if err := json.Unmarshal([]byte(val), &sensorData); err == nil {
				allMessages = append(allMessages, sensorData)
			}
		}

		return c.JSON(fiber.Map{
			"status":         "success",
			"messages_count": len(allMessages),
			"queue_total":    len(allMessages),
			"data":           allMessages,
		})
	})

	log.Fatal(app.Listen(":3000"))
}