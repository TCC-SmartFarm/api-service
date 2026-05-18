package main

import (
	"fmt"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

type CustomClaims struct {
	jwt.RegisteredClaims
}

func newJWKS(domain string) (keyfunc.Keyfunc, error) {
	url := fmt.Sprintf("https://%s/.well-known/jwks.json", domain)
	return keyfunc.NewDefault([]string{url})
}

func authMiddleware(jwks keyfunc.Keyfunc, audience, domain string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "token ausente"})
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.ParseWithClaims(tokenStr, &CustomClaims{}, jwks.Keyfunc,
			jwt.WithAudience(audience),
			jwt.WithIssuer(fmt.Sprintf("https://%s/", domain)),
			jwt.WithExpirationRequired(),
		)
		if err != nil || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "token inválido ou expirado"})
		}

		claims := token.Claims.(*CustomClaims)
		c.Locals("userID", claims.Subject)
		return c.Next()
	}
}