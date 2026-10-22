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
	requests map[requestSeries]uint64
}

// requestsName is the series of the per-route, per-credential-class counter
// that shows whether any caller still uses the routes.
const requestsName = "devhealth_ingest_legacy_requests_total"

// Key classes name which configured credential checks stand in force; the
// outcome is accepted (every configured check passed) or rejected.
var (
	keyClasses = []string{"api_key", "signature", "api_key_signature", "none"}
	outcomes   = []string{"accepted", "rejected", "malformed"}
)

type requestSeries struct{ route, keyClass, outcome string }

// NewMetrics returns a Metrics with every reason at zero.
func NewMetrics() *Metrics {
	counts := make(map[string]uint64, len(rejectionReasons))
	for _, reason := range rejectionReasons {
		counts[reason] = 0
	}
	requests := make(map[requestSeries]uint64)
	for _, item := range routes {
		for _, class := range keyClasses {
			for _, outcome := range outcomes {
				if class == "none" && outcome == "accepted" {
					continue
				}
				requests[requestSeries{item.kind, class, outcome}] = 0
			}
		}
	}
	return &Metrics{rejected: counts, requests: requests}
}

func (m *Metrics) countRequest(route, keyClass, outcome string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[requestSeries{route, keyClass, outcome}]++
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
	requests := make([]requestSeries, 0, len(m.requests))
	requestCounts := make(map[requestSeries]uint64, len(m.requests))
	for series, value := range m.requests {
		requests = append(requests, series)
		requestCounts[series] = value
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
	sort.Slice(requests, func(i, j int) bool {
		a, b := requests[i], requests[j]
		if a.route != b.route {
			return a.route < b.route
		}
		if a.keyClass != b.keyClass {
			return a.keyClass < b.keyClass
		}
		return a.outcome < b.outcome
	})
	if _, err := io.WriteString(writer, "# HELP "+requestsName+
		" Legacy /api/v1/ingest/* requests, by route, configured credential class and outcome.\n# TYPE "+requestsName+" counter\n"); err != nil {
		return err
	}
	for _, series := range requests {
		if _, err := fmt.Fprintf(writer, "%s{route=%q,key_class=%q,outcome=%q} %d\n", requestsName, series.route, series.keyClass, series.outcome, requestCounts[series]); err != nil {
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

// reject answers a 401 and counts it.
func (h handler) reject(w http.ResponseWriter, reason, detail string) {
	h.metrics.count(reason)
	policy.WriteDetail(w, http.StatusUnauthorized, detail, nil)
}

// countMalformed counts a request whose body was refused before any
// credential check.
func (h handler) countMalformed() {
	h.metrics.countRequest(h.route.kind, keyClass(apiKeys(h.getenv), h.getenv("INGEST_SIGNING_SECRET")), "malformed")
}

// keyClass names the configured checks.
func keyClass(keys []string, secret string) string {
	switch {
	case len(keys) > 0 && secret != "":
		return "api_key_signature"
	case len(keys) > 0:
		return "api_key"
	case secret != "":
		return "signature"
	}
	return "none"
}

// matchesAnyKey compares the digest of the presented key with the digest of
// every configured key, without stopping at the first match.
func matchesAnyKey(presented string, keys []string) bool {
	want := sha256.Sum256([]byte(presented))
	matched := 0
	for _, key := range keys {
		have := sha256.Sum256([]byte(key))
		matched |= subtle.ConstantTimeCompare(want[:], have[:])
	}
	return matched == 1
}

// authenticate is validate_ingest_auth. It reports whether the request may
// go on; when it does not, the response is written. Either configured
// credential alone is enough to configure a check; neither configured is a
// 401 in every environment (CHAOS-4720, fail closed).
func (h handler) authenticate(w http.ResponseWriter, r *http.Request, raw []byte) bool {
	keys := apiKeys(h.getenv)
	secret := h.getenv("INGEST_SIGNING_SECRET")
	class := keyClass(keys, secret)
	ok := h.checkCredentials(w, r, raw, keys, secret)
	outcome := "accepted"
	if !ok {
		outcome = "rejected"
	}
	h.metrics.countRequest(h.route.kind, class, outcome)
	return ok
}

func (h handler) checkCredentials(w http.ResponseWriter, r *http.Request, raw []byte, keys []string, secret string) bool {
	if len(keys) == 0 && secret == "" {
		h.logger.WarnContext(r.Context(), "Neither INGEST_API_KEYS nor INGEST_SIGNING_SECRET is configured - rejecting ingest request (CHAOS-4720)")
		h.reject(w, "no_credential_configured", "Invalid API key")
		return false
	}
	if len(keys) > 0 {
		presented := latin1(r.Header.Get("X-API-Key"))
		if presented == "" || !matchesAnyKey(presented, keys) {
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
