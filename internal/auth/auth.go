package auth

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// ParseBearer extracts a bearer token from an Authorization header.
func ParseBearer(header string) (string, bool) {
	if header == "" {
		return "", false
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return "", false
	}
	if !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	if parts[1] == "" {
		return "", false
	}

	return parts[1], true
}

// Middleware enforces bearer token authentication on all requests.
func Middleware(expectedToken string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expectedToken == "" {
			writeAuthError(w, http.StatusInternalServerError, "misconfigured_auth", "API token is not configured")
			return
		}

		gotToken, ok := ParseBearer(r.Header.Get("Authorization"))
		if !ok || subtle.ConstantTimeCompare([]byte(gotToken), []byte(expectedToken)) != 1 {
			writeAuthError(w, http.StatusUnauthorized, "unauthorized", "Missing or invalid bearer token")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func writeAuthError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":   code,
		"message": message,
	})
}
