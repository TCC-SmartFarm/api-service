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
	UserId	string      `json:"userId"`
	ApplicationId     string      `json:"applicationId"`
	DeviceType string      `json:"deviceType"`
	DevAddr   string      `json:"devAddr"`
	DevEUI	   string      `json:"devEUI"`
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

	// Testa conexão
	_, err = rdb.Ping(context.Background()).Result()

	if err != nil {
		log.Fatal("Erro ao conectar no Redis:", err)
	}

	log.Println("Conexões estabelecidas com sucesso")

	app := fiber.New()
	app.Use(cors.New())


	// 1. GET Histórico (InfluxDB)
	app.Get("/api/sensors/influx/:userId/:days/:devAddr", func(c *fiber.Ctx) error {
		userId := c.Params("userId")
		days := c.Params("days")
		devAddr := c.Params("devAddr")

		// Query corrigida: converte para float para evitar erro de agregação com strings
		query := fmt.Sprintf(`from(bucket: "%s")
        |> range(start: -%sd)
        |> filter(fn: (r) => r["_measurement"] == "telemetria")
        |> filter(fn: (r) => r["userId"] == "%s")
        |> filter(fn: (r) => r["devAddr"] == "%s")
        |> filter(fn: (r) => r["_field"] == "soil_temperature" or r["_field"] == "soil_moisture" or r["_field"] == "air_humidity" or r["_field"] == "luminosity" or r["_field"] == "air_temperature" or r["_field"] == "battery" or r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "validity")
        |> map(fn: (r) => ({ r with _value: float(v: r._value) }))`, bucket, days, userId, devAddr)

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
					"userId":    record.ValueByKey("userId"),
					"timestamp":  record.Time().Unix(), // Exibe o timestamp como inteiro (Unix) para facilitar o uso no frontend
					"applicationId":     record.ValueByKey("applicationId"),
					"devAddr":   record.ValueByKey("devAddr"),
					"deviceType": record.ValueByKey("deviceType"),
					"devEUI":       record.ValueByKey("devEUI"),
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
	app.Get("/api/sensors/latest/:userId/:devEUI", func(c *fiber.Ctx) error {
		userId := c.Params("userId")
		devEUI := c.Params("devEUI")
		cacheKey := fmt.Sprintf("userId:%s:devEUI:%s:history", userId, devEUI)

		// Pega todos os itens da lista (do 0 ao -1 significa "tudo")
		vals, err := rdb.LRange(c.Context(), cacheKey, 0, -1).Result()
		if err != nil || len(vals) == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Sem dados no cache"})
		}

		// Como as strings no Redis já são JSONs, vamos montar um array de JSONs manualmente
		// ou decodificar e enviar. O mais simples para o Fiber:
		return c.SendString("[" + strings.Join(vals, ",") + "]")
	})

	app.Get("/api/sensors/all/:userId", func(c *fiber.Ctx) error {
		userId := c.Params("userId")

		// 1. Padrão de busca para encontrar as listas de todos os dispositivos do usuário
		// O Cache-Service agora salva como: userId:XYZ:devEUI:XYZ:history
		pattern := fmt.Sprintf("userId:%s:devEUI:*:history", userId)

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
			"userId":          userId,
			"total_dispositivos": len(statusGeral),
			"leituras": statusGeral,
		})
	})

	log.Fatal(app.Listen(":3000"))
}