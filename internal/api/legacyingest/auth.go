package legacyingest

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// rejected counts refusals by reason, as the Python api's
// record_ingest_legacy_auth_rejected does (a fixed vocabulary, never
// request contents).
var rejected = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/legacyingest").Int64Counter(
		"dev_health_api_ingest_legacy_auth_rejected_total",
		metric.WithDescription("Legacy /api/v1/ingest/* requests refused for their credentials, by reason"))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("dev_health_api_ingest_legacy_auth_rejected_total")
	}
	return counter
}()

// latin1 is what Starlette makes of a header value: every byte one
// character. Comparing against an environment value (decoded as UTF-8) or
// storing it as a key (encoded as UTF-8) therefore differs from comparing
// the raw bytes for a non-ASCII value.
func latin1(value string) string {
	for index := 0; index < len(value); index++ {
		if value[index] >= 0x80 {
			runes := make([]rune, len(value))
			for position := 0; position < len(value); position++ {
				runes[position] = rune(value[position])
			}
			return string(runes)
		}
	}
	return value
}

func nonASCII(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] >= 0x80 {
			return true
		}
	}
	return false
}

// apiKeys is _get_api_keys: INGEST_API_KEYS split on commas, each stripped,
// blanks dropped; nothing when the variable is blank.
func apiKeys(getenv func(string) string) []string {
	raw := getenv("INGEST_API_KEYS")
	if pythonparity.Strip(raw) == "" {
		return nil
	}
	var keys []string
	for _, part := range strings.Split(raw, ",") {
		if key := pythonparity.Strip(part); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

// developmentEnvironment is is_development_environment: ENVIRONMENT, else
// APP_ENV, else ENV (an empty value falls through), else "production",
// stripped and lower-cased, one of development, dev, local.
func developmentEnvironment(getenv func(string) string) bool {
	name := "production"
	for _, key := range []string{"ENVIRONMENT", "APP_ENV", "ENV"} {
		if value := getenv(key); value != "" {
			name = value
			break
		}
	}
	switch pythonparity.Lower(pythonparity.Strip(name)) {
	case "development", "dev", "local":
		return true
	}
	return false
}

// reject answers a 401 and counts it.
func reject(w http.ResponseWriter, r *http.Request, reason, detail string) {
	rejected.Add(r.Context(), 1, metric.WithAttributes(attribute.String("reason", reason)))
	policy.WriteDetail(w, http.StatusUnauthorized, detail, nil)
}

// authenticate is validate_ingest_auth. It reports whether the request may
// go on; when it does not, the response is written. Either configured
// credential alone is enough to configure a check; neither configured is a
// 401 outside a development environment (CHAOS-4720, fail closed).
func (h handler) authenticate(w http.ResponseWriter, r *http.Request, raw []byte) bool {
	keys := apiKeys(h.getenv)
	secret := h.getenv("INGEST_SIGNING_SECRET")
	if len(keys) == 0 && secret == "" && !developmentEnvironment(h.getenv) {
		h.logger.WarnContext(r.Context(), "Neither INGEST_API_KEYS nor INGEST_SIGNING_SECRET is configured outside a development environment - rejecting ingest request (CHAOS-4720)")
		reject(w, r, "no_credential_configured", "Invalid API key")
		return false
	}
	if len(keys) > 0 {
		presented := latin1(r.Header.Get("X-API-Key"))
		found := false
		for _, key := range keys {
			if presented == key {
				found = true
				break
			}
		}
		if presented == "" || !found {
			reject(w, r, "invalid_api_key", "Invalid API key")
			return false
		}
	}
	if secret != "" {
		signature := r.Header.Get("X-Signature-256")
		valid := false
		if strings.HasPrefix(signature, "sha256=") {
			expected := signature[len("sha256="):]
			// hmac.compare_digest on two str refuses a non-ASCII one with a
			// TypeError: the unhandled 500, not a 401.
			if nonASCII(expected) {
				policy.WriteInternal(w)
				return false
			}
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write(raw)
			computed := hex.EncodeToString(mac.Sum(nil))
			valid = subtle.ConstantTimeCompare([]byte(computed), []byte(expected)) == 1
		}
		if !valid {
			reject(w, r, "invalid_signature", "Invalid signature")
			return false
		}
	}
	return true
}
