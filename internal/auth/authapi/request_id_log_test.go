package authapi

import (
	"bytes"
	"encoding/base32"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// plantedRequestIDLog serves one authorize call with no credential through requestid.Middleware and the production
// JSON logger (the route's and the process default, which the middleware writes to) while the uuid random source
// answers draws in order and then ends. It returns the answer, the log, the number of draws read and whether the route
// ran. The source and the default logger are process-wide, so no cell using it runs in parallel.
func plantedRequestIDLog(t *testing.T, draws ...[16]byte) (*httptest.ResponseRecorder, *bytes.Buffer, int, bool) {
	t.Helper()
	var source []byte
	for _, d := range draws {
		source = append(source, d[:]...)
	}
	planted := bytes.NewReader(source)
	uuid.SetRand(planted)
	t.Cleanup(func() { uuid.SetRand(nil) })
	logs := &bytes.Buffer{}
	logger := logging.NewJSON(logs, slog.LevelInfo)
	t.Cleanup(logging.InstallDefault(logger))
	rec, err := newRecorder(logger, noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	admit := authorizeHandlers{rec: rec}.admit(opAuthorize)
	ran := false
	w := httptest.NewRecorder()
	requestid.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran = true
		admit(w, r)
	})).ServeHTTP(w, httptest.NewRequest(http.MethodPost, PatternAuthorize, strings.NewReader("{}")))
	return w, logs, (len(source) - planted.Len()) / 16, ran
}

// keyShapedRequestDraw is 16 random bytes whose wire id body starts with an Atlassian token prefix, which the log
// redactor replaces with the 16 or more base32 characters after it; it is built at run time so no source literal
// looks like a key.
func keyShapedRequestDraw(t *testing.T) [16]byte {
	t.Helper()
	raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString("AT" + "ATT" + strings.Repeat("Q", 18) + "AAA")
	if err != nil || len(raw) != 16 {
		t.Fatalf("decode the planted draw: %d bytes, %v", len(raw), err)
	}
	return [16]byte(raw)
}

// wireOfDraw is the wire id uuid.New makes of draw (it sets the version and variant bits).
func wireOfDraw(t *testing.T, draw [16]byte) string {
	t.Helper()
	id, err := uuid.NewRandomFromReader(bytes.NewReader(draw[:]))
	if err != nil {
		t.Fatal(err)
	}
	return requestid.WireOf(id)
}

func refusalLogLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("log line %s: %v", line, err)
		}
		if entry["msg"] == "auth session route refused" {
			entries = append(entries, entry)
		}
	}
	return entries
}

// A drawn request id the log redactor would change is never served: the refusal line's audit_request_id, the
// response header and the error.v1 request_id are one id.
func TestAuthRequestIDLogNamesTheServedIDWhenADrawLooksLikeAKey(t *testing.T) {
	keyShaped, clean := keyShapedRequestDraw(t), [16]byte{}
	if wire := wireOfDraw(t, keyShaped); logging.RedactKeyedText("audit_request_id", wire) == wire {
		t.Fatalf("the redactor keeps %s; the cell needs an id it changes", wire)
	}
	want := wireOfDraw(t, clean)
	if logging.RedactKeyedText("audit_request_id", want) != want {
		t.Fatalf("the redactor changes the clean id %s", want)
	}
	w, logs, draws, _ := plantedRequestIDLog(t, keyShaped, clean)
	assertError(t, w, http.StatusUnauthorized, reasonInvalidCredentials)
	var doc struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	entries := refusalLogLines(t, logs)
	if len(entries) != 1 {
		t.Fatalf("%d refusal lines, want 1: %s", len(entries), logs)
	}
	header := w.Header().Get(requestid.Header)
	if entries[0]["audit_request_id"] != doc.RequestID || header != doc.RequestID || doc.RequestID != want {
		t.Fatalf("header %s, body %s, log line %v; want all %s", header, doc.RequestID, entries[0], want)
	}
	if draws != 2 {
		t.Fatalf("%d draws, want 2", draws)
	}
}

// The redraw is bounded: a source that only answers key-shaped ids is a 503 dependency_unavailable after 8 draws,
// before any route runs, with no X-Auth-Request-ID header and no log line of a redacted id. The error.v1 body names
// the fixed nil-uuid wire id, never a draw, and one WARN line without any id records the refusal.
func TestAuthRequestIDRedrawIsBounded(t *testing.T) {
	keyShaped := keyShapedRequestDraw(t)
	drawWire := wireOfDraw(t, keyShaped)
	placeholder := requestid.WireOf(uuid.Nil)
	draws := make([][16]byte, 8)
	for i := range draws {
		draws[i] = keyShaped
	}
	w, logs, read, ran := plantedRequestIDLog(t, draws...)
	assertError(t, w, http.StatusServiceUnavailable, reasonDependency)
	var doc struct {
		SchemaVersion     string `json:"schema_version"`
		Status            int    `json:"status"`
		RequestID         string `json:"request_id"`
		RetryAfterSeconds *int   `json:"retry_after_seconds"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.SchemaVersion != contracts.ErrorSurface || doc.Status != http.StatusServiceUnavailable ||
		doc.RetryAfterSeconds == nil || *doc.RetryAfterSeconds != 1 || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("body %s, Retry-After %q: want an error.v1 503 with retry 1", w.Body, w.Header().Get("Retry-After"))
	}
	if doc.RequestID != placeholder || doc.RequestID == drawWire {
		t.Fatalf("request_id %s, want the fixed %s and never the draw %s", doc.RequestID, placeholder, drawWire)
	}
	var warns []string
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("log line %s: %v", line, err)
		}
		if entry["level"] == slog.LevelWarn.String() {
			warns = append(warns, string(line))
		}
	}
	if len(warns) != 1 {
		t.Fatalf("%d WARN lines, want 1: %s", len(warns), logs)
	}
	if strings.Contains(warns[0], requestid.WirePrefix) || strings.Contains(warns[0], drawWire[len(requestid.WirePrefix):]) ||
		strings.Contains(warns[0], "request_id") {
		t.Fatalf("the WARN line names an id: %s", warns[0])
	}
	if ran {
		t.Fatal("the route ran without a loggable request id")
	}
	if got := w.Header().Get(requestid.Header); got != "" {
		t.Fatalf("header %s on an answer whose id the log would redact", got)
	}
	if strings.Contains(logs.String(), "REDACTED") {
		t.Fatalf("a log line holds a redacted value: %s", logs)
	}
	if read != 8 {
		t.Fatalf("%d draws, want 8", read)
	}
}
