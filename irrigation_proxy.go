package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

const (
	irrigationMSSPath      = "/api/v1/irrigation"
	irrigationProxyTimeout = 12 * time.Second
	maxIrrigationJSONBytes = 4 << 20 // 4 MiB; a resposta normal é bem menor.
)

// irrigationProxyHandler expõe a rota pública no API Service e encaminha a
// consulta ao Irrigacao-MSS. O JSON e o status HTTP do MSS são preservados.
// O endereço do MSS deve vir da variável IRRIGATION_MSS_URL, nunca do cliente.
func irrigationProxyHandler(mssBaseURL string) fiber.Handler {
	client := &http.Client{Timeout: irrigationProxyTimeout}
	base := strings.TrimSpace(mssBaseURL)

	return func(c *fiber.Ctx) error {
		userID := strings.TrimSpace(c.Query("userId"))
		devEUI := strings.TrimSpace(c.Query("devEUI"))
		timezone := strings.TrimSpace(c.Query("timezone"))

		if userID == "" || devEUI == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "informe userId e devEUI na query string",
			})
		}

		if base == "" {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "Irrigacao-MSS não configurado no API Service",
			})
		}

		target, err := buildIrrigationMSSURL(base, userID, devEUI, timezone)
		if err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "URL interna do Irrigacao-MSS inválida; confira IRRIGATION_MSS_URL",
			})
		}

		ctx, cancel := context.WithTimeout(c.UserContext(), irrigationProxyTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "não foi possível preparar a consulta ao Irrigacao-MSS",
			})
		}
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			status := fiber.StatusBadGateway
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				status = fiber.StatusGatewayTimeout
			}
			return c.Status(status).JSON(fiber.Map{
				"error": "não foi possível consultar o Irrigacao-MSS",
			})
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxIrrigationJSONBytes+1))
		if err != nil {
			return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
				"error": "falha ao ler a resposta do Irrigacao-MSS",
			})
		}
		if len(body) > maxIrrigationJSONBytes {
			return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
				"error": "resposta do Irrigacao-MSS excedeu o tamanho permitido",
			})
		}

		contentType := resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/json; charset=utf-8"
		}
		c.Set("Content-Type", contentType)
		c.Set("Cache-Control", "no-store")
		return c.Status(resp.StatusCode).Send(body)
	}
}

// buildIrrigationMSSURL aceita apenas uma URL interna configurada no servidor
// e encaminha os parâmetros permitidos; o cliente nunca escolhe o destino.
func buildIrrigationMSSURL(base, userID, devEUI, timezone string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil {
		return "", fmt.Errorf("IRRIGATION_MSS_URL inválida: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("IRRIGATION_MSS_URL deve ser uma URL HTTP(S) base, sem credenciais, query ou fragmento")
	}

	// A variável representa a origem/base do serviço, por exemplo http://irrigacao-mss:8456.
	u.Path = strings.TrimRight(u.Path, "/") + irrigationMSSPath
	u.RawPath = ""
	query := url.Values{}
	query.Set("userId", userID)
	query.Set("devEUI", devEUI)
	if timezone != "" {
		query.Set("timezone", timezone)
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}
