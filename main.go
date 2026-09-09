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
	// amqp "github.com/rabbitmq/amqp091-go"
)

type SensorMessage struct { // para o payload do RabbitMQ
	UserId	string      `json:"userId"`
	ApplicationId     string      `json:"applicationId"`
	DeviceType string      `json:"deviceType"`
	DevAddr   string      `json:"devAddr"`
	DevEUI	   string      `json:"devEUI"`
	Payload    interface{} `json:"payload"` // interface{} permite receber qualquer JSON interno
}

func reverseArray(arr []interface{}) []interface{} {
    for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
        arr[i], arr[j] = arr[j], arr[i]
    }
    return arr
}

func main() {
	// Configurações via Variáveis de Ambiente
	influxURL := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")
	org := os.Getenv("INFLUX_ORG")
	bucket := os.Getenv("INFLUX_BUCKET")
	// rabbitURL := os.Getenv("RABBIT_URL")
	redisAddr := os.Getenv("REDIS_ADDR")


	// Conector InfluxDB
	client := influxdb2.NewClient(influxURL, token)
	queryAPI := client.QueryAPI(org)
	defer client.Close()

	// // Conector RabbitMQ
	// rabbitConn, err := amqp.Dial(rabbitURL)
	// if err != nil {
	// 	log.Fatal("Erro ao conectar no RabbitMQ:", err)
	// }
	// defer rabbitConn.Close()

	// Conector Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// // Testa conexão
	// _, err = rdb.Ping(context.Background()).Result()

	// if err != nil {
	// 	log.Fatal("Erro ao conectar no Redis:", err)
	// }

	log.Println("Conexões estabelecidas com sucesso")

	app := fiber.New()
	app.Use(cors.New())


	// 1. GET Histórico (InfluxDB)
	app.Get("/api/sensors/influx/:userId/:days/:devEUI", func(c *fiber.Ctx) error {
		userId := c.Params("userId")
		days := c.Params("days")
		devEUI := c.Params("devEUI")

		// Query corrigida: converte para float para evitar erro de agregação com strings
		query := fmt.Sprintf(`from(bucket: "%s")
        |> range(start: -%sd)
        |> filter(fn: (r) => r["_measurement"] == "telemetria")
        |> filter(fn: (r) => r["userId"] == "%s")
        |> filter(fn: (r) => r["devEUI"] == "%s")
        |> filter(fn: (r) => r["_field"] == "soil_temperature" or r["_field"] == "soil_moisture" or r["_field"] == "air_humidity" or r["_field"] == "luminosity" or r["_field"] == "air_temperature" or r["_field"] == "battery" or r["_field"] == "latitude" or r["_field"] == "longitude" or r["_field"] == "validity")
        |> map(fn: (r) => ({ r with _value: float(v: r._value) }))`, bucket, days, userId, devEUI)

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

		// ETAPA A: Tenta buscar no Redis (Cache Hit)
		vals, err := rdb.LRange(c.Context(), cacheKey, 0, -1).Result()
		if err == nil && len(vals) > 0 {
			log.Printf("CACHE HIT: Dados servidos do Redis para %s", devEUI)
			return c.SendString("[" + strings.Join(vals, ",") + "]")
		}

		// ETAPA B: Cache Miss - Busca do InfluxDB
		log.Printf("CACHE MISS: Redis vazio. Repopulando via InfluxDB para %s...", devEUI)
		
		query := fmt.Sprintf(`from(bucket: "%s")
        |> range(start: -30d)
        |> filter(fn: (r) => r["_measurement"] == "telemetria")
        |> filter(fn: (r) => r["devEUI"] == "%s")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc: false)
        |> tail(n: 20)`, bucket, devEUI)

		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao consultar banco de séries temporais"})
		}

		var records []interface{}
		pipe := rdb.Pipeline() // Prepara os comandos para enviar ao Redis de uma vez só

		for result.Next() {
			record := result.Record()

			// Extrai as métricas (temperatura, umidade, etc)
			payloadData := map[string]interface{}{
				"timestamp": record.Time().Unix(),
			}
			for k, v := range record.Values() {
				if !strings.HasPrefix(k, "_") && k != "table" && k != "result" {
					payloadData[k] = v
				}
			}

			// Constrói o objeto no mesmo formato esperado pelo React
			msg := map[string]interface{}{
				"userId":        record.ValueByKey("userId"),
				"applicationId": record.ValueByKey("applicationId"),
				"devAddr":       record.ValueByKey("devAddr"),
				"devEUI":        record.ValueByKey("devEUI"),
				"payload":       payloadData,
			}

			msgBytes, _ := json.Marshal(msg)
			records = append(records, msg)

			// Injeta no Redis
			pipe.LPush(c.Context(), cacheKey, string(msgBytes))
		}

		if len(records) == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "Nenhum dado encontrado para este sensor"})
		}

		// ETAPA C: Completa a retroalimentação aplicando o corte de 20 itens no Redis
		pipe.LTrim(c.Context(), cacheKey, 0, 19)
		_, err = pipe.Exec(c.Context())
		if err != nil {
			log.Printf("Aviso: Falha ao gravar no Redis durante o repopulamento: %v", err)
		}

		// Inverte a array para enviar a mais recente primeiro para o Front-end
		return c.JSON(reverseArray(records))
	})

	app.Get("/api/sensors/all/:userId", func(c *fiber.Ctx) error {
		userId := c.Params("userId")

		// 1. Padrão de busca para encontrar as listas de todos os dispositivos do usuário
		// O Cache-Service agora salva como: userId:XYZ:devEUI:XYZ:history
		pattern := fmt.Sprintf("userId:%s:devEUI:*:history", userId)

		// 2. Localiza todas as chaves (dispositivos) que o usuário possui no cache
		// ETAPA A: Tenta buscar os dispositivos do usuário no Redis
		keys, err := rdb.Keys(c.Context(), pattern).Result()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao escanear dispositivos no cache"})
		}

		var statusGeral []interface{}

		// CACHE HIT: Se encontrou as chaves, coleta o último dado (índice 0) de cada sensor
		if len(keys) > 0 {
			for _, key := range keys {
				val, err := rdb.LIndex(c.Context(), key, 0).Result()
				if err == nil {
					var lastRead interface{}
					json.Unmarshal([]byte(val), &lastRead)
					statusGeral = append(statusGeral, lastRead)
				}
			}

			log.Printf("CACHE HIT ALL: Servindo %d sensores do usuário %s via Redis", len(statusGeral), userId)
			return c.JSON(fiber.Map{
				"userId":             userId,
				"total_dispositivos": len(statusGeral),
				"leituras":           statusGeral,
			})
		}

		// ETAPA B: CACHE MISS - Redis vazio para este usuário. Busca massiva no InfluxDB.
		log.Printf("CACHE MISS ALL: Repopulando todos os sensores do usuário %s via InfluxDB...", userId)

		// Query agrupada por devEUI, garantindo a captura do histórico de todos os sensores do usuário
		query := fmt.Sprintf(`from(bucket: "%s")
        |> range(start: -30d)
        |> filter(fn: (r) => r["_measurement"] == "telemetria")
        |> filter(fn: (r) => r["userId"] == "%s")
        |> group(columns: ["devEUI", "devAddr", "applicationId"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"], desc: false)
        |> tail(n: 20)`, bucket, userId)

		result, err := queryAPI.Query(context.Background(), query)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Erro ao consultar banco de séries temporais"})
		}

		pipe := rdb.Pipeline()
		
		// Mapas auxiliares para capturar o dado mais recente de cada sensor e coordenar os cortes no cache
		latestReads := make(map[string]map[string]interface{})
		keysToTrim := make(map[string]bool)

		for result.Next() {
			record := result.Record()
			devEUI, _ := record.ValueByKey("devEUI").(string)
			cacheKey := fmt.Sprintf("userId:%s:devEUI:%s:history", userId, devEUI)
			keysToTrim[cacheKey] = true

			// Estruturação do Payload
			payloadData := map[string]interface{}{
				"timestamp": record.Time().Unix(),
			}
			for k, v := range record.Values() {
				if !strings.HasPrefix(k, "_") && k != "table" && k != "result" {
					payloadData[k] = v
				}
			}

			// Montagem da estrutura JSON padrão
			msg := map[string]interface{}{
				"userId":        userId,
				"applicationId": record.ValueByKey("applicationId"),
				"devAddr":       record.ValueByKey("devAddr"),
				"devEUI":        devEUI,
				"payload":       payloadData,
			}

			msgBytes, _ := json.Marshal(msg)
			
			// ETAPA C: Retroalimenta o Redis
			pipe.LPush(c.Context(), cacheKey, string(msgBytes))

			// Como a ordenação é crescente, o último registro processado no loop para um dado devEUI será o atual
			latestReads[devEUI] = msg
		}

		if len(latestReads) == 0 {
			return c.Status(404).JSON(fiber.Map{"message": "Nenhum sensor ativo encontrado para este produtor"})
		}

		// Finaliza a organização de memória do cache (Mantém limite de 20 registros por sensor)
		for key := range keysToTrim {
			pipe.LTrim(c.Context(), key, 0, 19)
		}
		
		_, err = pipe.Exec(c.Context())
		if err != nil {
			log.Printf("Aviso: Falha ao gravar no Redis durante repopulamento massivo: %v", err)
		}

		// Converte os dados processados para a resposta final do endpoint
		for _, lastRead := range latestReads {
			statusGeral = append(statusGeral, lastRead)
		}

		return c.JSON(fiber.Map{
			"userId":             userId,
			"total_dispositivos": len(statusGeral),
			"leituras":           statusGeral,
		})
	})

	log.Fatal(app.Listen(":3000"))
}