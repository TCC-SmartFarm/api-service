package main

import (
	"context"
	"fmt"
	"log"
	"encoding/json" 
	"os"
	"time"
	"strconv"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/influxdata/influxdb-client-go/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SensorMessage struct { // para o payload do RabbitMQ
	UserId     string      `json:"userId"`
	DeviceType string      `json:"deviceType"`
	DeviceId   string      `json:"deviceId"`
	Payload    interface{} `json:"payload"` // interface{} permite receber qualquer JSON interno
}

// Struct das claims do JWT.
//
// RegisteredClaims já inclui:
// - sub (Subject / ID do usuário)
// - iss (Issuer)
// - aud (Audience)
// - exp (Expiration)
// - iat
// - nbf
type CustomClaims struct {
	jwt.RegisteredClaims
}

func newJWKS(domain string) (keyfunc.Keyfunc, error) {

	// Monta a URL do endpoint JWKS
	url := fmt.Sprintf("https://%s/.well-known/jwks.json", domain)

	// Cria o gerenciador automático de chaves
	return keyfunc.NewDefault([]string{url})
}

func authMiddleware(
	jwks keyfunc.Keyfunc,
	audience string,
	domain string,
) fiber.Handler {

	return func(c *fiber.Ctx) error {

		authHeader := c.Get("Authorization")

		if !strings.HasPrefix(authHeader, "Bearer ") {

			return c.Status(fiber.StatusUnauthorized).JSON(
				fiber.Map{
					"error": "token ausente",
				},
			)
		}

		// Extrai token removendo "Bearer "

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		// Faz parse e validação do JWT

		token, err := jwt.ParseWithClaims(

			// JWT recebido
			tokenStr,

			// Struct das claims
			&CustomClaims{},

			// Função que resolve as chaves públicas JWKS
			jwks.Keyfunc,


			// Validações adicionais

			// Valida audience
			jwt.WithAudience(audience),

			// Valida issuer
			jwt.WithIssuer(
				fmt.Sprintf("https://%s/", domain),
			),

			// Exige claim de expiração
			jwt.WithExpirationRequired(),

			// Aceita apenas algoritmo RS256
			// Muito importante para evitar ataques de troca de algoritmo.
			jwt.WithValidMethods([]string{"RS256"}),
		)

		// Verifica validade do token

		if err != nil || !token.Valid {

			return c.Status(fiber.StatusUnauthorized).JSON(
				fiber.Map{
					"error": "token inválido ou expirado",
				},
			)
		}

		// Extrai claims

		claims := token.Claims.(*CustomClaims)

		// Salva ID do usuário autenticado no contexto
		c.Locals("userID", claims.Subject)
		// Continua

		return c.Next()
	}
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

	// 2. GET todas as mensagens da respectiva fila (RabbitMQ - "Espiar" Fila)
	app.Get("/api/sensors/latest", func(c *fiber.Ctx) error {
		userID := c.Locals("userID").(string)

		queueName := fmt.Sprintf("fila_%s", userID)

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

	log.Fatal(app.Listen(":3000"))
}