package legacyingest

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// authRejectedName is the series of the refusals counter.
const authRejectedName = "devhealth_ingest_legacy_auth_rejected_total"

// rejectionReasons is the fixed vocabulary of the counter (never request
// contents), each series present at zero from the first scrape.
var rejectionReasons = []string{"no_credential_configured", "invalid_api_key", "invalid_signature"}

// Metrics counts refusals by reason, as the Python api's
// record_ingest_legacy_auth_rejected does. It is exposed on the operator
// /metrics endpoint by registering it with the health registry
// (RegisterMetrics); a process-global OpenTelemetry counter would count into
// nothing.
type Metrics struct {
	mu       sync.Mutex
	rejected map[string]uint64
}

// NewMetrics returns a Metrics with every reason at zero.
func NewMetrics() *Metrics {
	counts := make(map[string]uint64, len(rejectionReasons))
	for _, reason := range rejectionReasons {
		counts[reason] = 0
	}
	return &Metrics{rejected: counts}
}

func (m *Metrics) count(reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejected[reason]++
}

// WritePrometheus implements health.MetricsSource.
func (m *Metrics) WritePrometheus(writer io.Writer) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	snapshot := make(map[string]uint64, len(m.rejected))
	for reason, value := range m.rejected {
		snapshot[reason] = value
	}
	m.mu.Unlock()
	if _, err := io.WriteString(writer, "# HELP "+authRejectedName+
		" Legacy /api/v1/ingest/* requests refused for their credentials, by reason.\n# TYPE "+authRejectedName+" counter\n"); err != nil {
		return err
	}
	reasons := make([]string, 0, len(snapshot))
	for reason := range snapshot {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		if _, err := fmt.Fprintf(writer, "%s{reason=%q} %d\n", authRejectedName, reason, snapshot[reason]); err != nil {
			return err
		}
	}
	return nil
}

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
func (h handler) reject(w http.ResponseWriter, reason, detail string) {
	h.metrics.count(reason)
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
		h.reject(w, "no_credential_configured", "Invalid API key")
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
			h.reject(w, "invalid_api_key", "Invalid API key")
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
			h.reject(w, "invalid_signature", "Invalid signature")
			return false
		}
	}
	return true
}
