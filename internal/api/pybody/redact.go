package pybody

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// Redacted replaces a submitted value that must not leave the server.
const Redacted = "[redacted]"

var secretNameParts = []string{
	"password", "passwd", "passphrase", "secret", "token", "credential",
	"api_key", "apikey", "private_key", "authorization",
}

func secretName(name string) bool {
	lower := strings.ToLower(name)
	for _, part := range secretNameParts {
		if strings.Contains(lower, part) {
			return true
		}
	}
	return false
}

// RedactInput is the "input" a 422 error may echo. FastAPI echoes the
// submitted value; here a value that sits under a secret-named location, and
// every secret-named member inside an echoed object, is replaced, so a
// validation error never carries a submitted password, token or secret.
// Non-secret values keep their Python-parity echo.
func RedactInput(loc []pyjson.Value, input pyjson.Value) pyjson.Value {
	for _, part := range loc {
		if name, ok := part.(string); ok && secretName(name) {
			return Redacted
		}
	}
	return redactValue(input)
}

func redactValue(value pyjson.Value) pyjson.Value {
	switch typed := value.(type) {
	case *pyjson.Object:
		if typed == nil {
			return value
		}
		out := pyjson.NewObject()
		for _, key := range typed.Keys() {
			member, _ := typed.Get(key)
			if secretName(key) {
				out.Set(key, Redacted)
				continue
			}
			out.Set(key, redactValue(member))
		}
		return out
	case []pyjson.Value:
		out := make([]pyjson.Value, len(typed))
		for index, item := range typed {
			out[index] = redactValue(item)
		}
		return out
	default:
		return value
	}
}
