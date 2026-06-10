package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

const localsAuthUser = "authUser"

// AuthUser é a entidade do usuário autenticado, disponível em qualquer rota
// registrada depois do authMiddleware. Recupere com GetAuthUser(c).
type AuthUser struct {
	Sub    string // identificador do Auth0, ex: "auth0|681f..."
	UserID string // id da fazenda vindo da claim customizada, ex: "fazenda1"
	Email  string
	Name   string
}

// customClaims mapeia as claims padrão do JWT + as claims customizadas que a
// Action post-login do Auth0 adiciona ao access token.
type customClaims struct {
	jwt.RegisteredClaims
	UserID string `json:"https://smartfarm-api/userId"`
	Email  string `json:"https://smartfarm-api/email"`
	Name   string `json:"https://smartfarm-api/name"`
}

// newJWKS baixa as chaves públicas do Auth0 (JWKS) e mantém cache com
// atualização automática em background — necessário porque o Auth0 rotaciona
// as chaves de assinatura periodicamente.
func newJWKS(domain string) (keyfunc.Keyfunc, error) {
	url := fmt.Sprintf("https://%s/.well-known/jwks.json", domain)
	return keyfunc.NewDefault([]string{url})
}

// authMiddleware valida o JWT do header Authorization e disponibiliza o
// AuthUser via c.Locals. Respostas: 401 para token ausente/inválido/expirado,
// 403 para token válido sem fazenda associada (ex: token machine-to-machine,
// que não passa pela Action post-login).
func authMiddleware(jwks keyfunc.Keyfunc, audience, domain string) fiber.Handler {
	issuer := fmt.Sprintf("https://%s/", domain)
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "token ausente"})
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		claims := &customClaims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, jwks.Keyfunc,
			jwt.WithAudience(audience),
			jwt.WithIssuer(issuer),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(30*time.Second), // tolerância a clock skew entre containers
			jwt.WithValidMethods([]string{"RS256"}),
		)
		if err != nil || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "token inválido ou expirado"})
		}

		if claims.UserID == "" {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "usuário sem fazenda associada"})
		}

		c.Locals(localsAuthUser, AuthUser{
			Sub:    claims.Subject,
			UserID: claims.UserID,
			Email:  claims.Email,
			Name:   claims.Name,
		})
		return c.Next()
	}
}

// GetAuthUser retorna o usuário autenticado da requisição. Só funciona em
// rotas registradas depois do authMiddleware; fora disso retorna AuthUser
// zerado (sem panic).
func GetAuthUser(c *fiber.Ctx) AuthUser {
	user, _ := c.Locals(localsAuthUser).(AuthUser)
	return user
}
