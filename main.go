package main

import (
	"context"
	"fmt"
	"log"
	"encoding/json" 
	"os"
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
	app.Get("/api/sensors/influx/:userID/:days/:deviceId", func(c *fiber.Ctx) error {
		userID := c.Params("userID")
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

	// ISSO PEGA DIRETO DO BARRAMENTO DE EVENTOS (RabbitMQ)
	// 2. GET todas as mensagens da respectiva fila (RabbitMQ - "Espiar" Fila) 
	app.Get("/api/sensors/latest/:userID", func(c *fiber.Ctx) error {
		userID := c.Params("userID")
		queueName := fmt.Sprintf("sensor.%s.#", userID) // se não funcionar tenho que colocar o nome exato da fila (ex: sensor.fazenda1.12345) deviceType.userId.DeviceId

		ch, err := rabbitConn.Channel()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao acessar barramento"})
		}
		defer ch.Close()

		// Inspeciona a fila para saber o estado atual sem retirar nada
		qInfo, err := ch.QueueInspect(queueName)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Fila não encontrada"})
		}

		var allMessages []SensorMessage
		
		// Definimos um limite para não travar a API caso a fila esteja gigante
		limit := int(qInfo.Messages)
		if limit > 50 {
			limit = 50
		}

		// Loop controlado pela quantidade de mensagens existentes
		for i := 0; i < limit; i++ {
			msg, ok, err := ch.Get(queueName, false)
			if err != nil || !ok {
				break 
			}

			var sensorData SensorMessage
			if err := json.Unmarshal(msg.Body, &sensorData); err == nil {
				allMessages = append(allMessages, sensorData)
			}

			// Em vez de Nack imediato (que joga pro topo), 
			// usei uma lógica de requeue que permite avançar.
			// No RabbitMQ padrão, para "espiar" a fila toda, o ideal é 
			// fechar o canal após coletar, mas o Nack(true) sempre 
			// trará a mesma se o loop for síncrono.
			defer ch.Nack(msg.DeliveryTag, false, true)
			// log.Printf("Espiado mensagem da fila %s: %s", queueName, string(msg.Body))
		}

		return c.JSON(fiber.Map{
			"status":         "success",
			"messages_count": len(allMessages),
			"queue_total":    qInfo.Messages,
			"data":           allMessages,
		})
	})




	// ISSO PEGA DO CACHE (REDIS)
	app.Get("/api/sensors/latest/:userID/:deviceId", func(c *fiber.Ctx) error {
		userID := c.Params("userID")
		deviceId := c.Params("deviceId")
		log.Printf("Recebendo requisição para userID: %s, deviceId: %s", userID, deviceId)

		// Montamos a mesma chave que o Cache-Service usou
		cacheKey := fmt.Sprintf("userId:%s:deviceId:%s:latest", userID, deviceId)

		// Buscamos direto no Redis (Velocidade de memória RAM)
		val, err := rdb.Get(context.Background(), cacheKey).Result()
		if err != nil {
			return c.Status(404).JSON(fiber.Map{
				"error": "Dispositivo offline ou sem dados recentes no cache",
			})
		}

		// Como salvamos o JSON inteiro no Redis, podemos retornar direto
		// O Fiber SendString envia o conteúdo bruto sem re-encodar
		c.Set("Content-Type", "application/json")
		return c.SendString(val)
	})


	app.Get("/api/sensors/latest/:userID/all", func(c *fiber.Ctx) error {
        userID := c.Params("userID")
        
        pattern := fmt.Sprintf("userId:%s:deviceId:*:latest", userID)

        // CORREÇÃO: Usar c.Context() ou context.Background() em vez de 'context'
        keys, err := rdb.Keys(c.Context(), pattern).Result() 
        if err != nil {
            return c.Status(500).JSON(fiber.Map{"error": "Erro ao buscar chaves no cache"})
        }

        if len(keys) == 0 {
            return c.Status(404).JSON(fiber.Map{"message": "Nenhum sensor em cache para este usuário"})
        }

        var allData []interface{}
        for _, key := range keys {
            // CORREÇÃO: Usar c.Context() ou context.Background() aqui também
            val, err := rdb.Get(c.Context(), key).Result() 
            if err == nil {
                var sensorMsg interface{}
                json.Unmarshal([]byte(val), &sensorMsg)
                allData = append(allData, sensorMsg)
            }
        }

        return c.JSON(fiber.Map{
            "status": "success",
            "total":  len(allData),
            "data":   allData,
        })
    })

	log.Fatal(app.Listen(":3000"))
}