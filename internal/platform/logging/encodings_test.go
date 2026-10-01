package logging_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"html"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// plantedValue has every character an encoder treats specially: a quote, a
// backslash, the HTML characters, the URL characters and a non-ASCII letter.
const plantedValue = "Zq7!k\"x\\<&>+/=?>>>?é~\x01Tw9"

func jsonString(value string, escapeHTML bool) string {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(escapeHTML)
	_ = encoder.Encode(value)
	text := strings.TrimSuffix(buffer.String(), "\n")
	return text[1 : len(text)-1]
}

// secretKinds register plantedValue through each real path a secret enters the
// registry by. Each is a different way in; every one must give the same cover.
var secretKinds = []struct {
	name     string
	register func(t *testing.T)
}{
	{"dsn password (config.ResolveDSN)", func(t *testing.T) {
		dsn := (&url.URL{Scheme: "clickhouse", User: url.UserPassword("table_login", plantedValue), Host: "ch:9000", Path: "/db"}).String()
		lookup := func(key string) (string, bool) { return dsn, key == "CLICKHOUSE_URI" }
		if _, _, err := config.ResolveDSN(lookup, "CLICKHOUSE_URI", config.ClickHouseSpec); err != nil {
			t.Fatal(err)
		}
	}},
	{"environment secret (GetenvSecret)", func(t *testing.T) {
		t.Setenv("TABLE_API_TOKEN", plantedValue)
		_ = secrets.GetenvSecret("TABLE_API_TOKEN")
	}},
	{"environment secret (ProcessLookup)", func(t *testing.T) {
		t.Setenv("TABLE_SERVICE_SECRET", plantedValue)
		_, _ = secrets.ProcessLookup("TABLE_SERVICE_SECRET")
	}},
	{"resolved setting (ResolveSecret)", func(t *testing.T) {
		lookup := func(key string) (string, bool) { return plantedValue, key == "TABLE_KEY" }
		_, _, _ = secrets.ResolveSecret("TABLE_KEY", lookup)
	}},
	{"decrypted JSON field", func(t *testing.T) {
		encoded, _ := json.Marshal(map[string]string{"token": plantedValue})
		secrets.RegisterDecrypted(encoded)
	}},
	{"decrypted bare value", func(t *testing.T) { secrets.RegisterDecrypted([]byte(plantedValue)) }},
}

// secretEncodings are the forms an echo of the secret takes in a log line. covered
// rows must never reach the log; "not covered" rows are listed with the reason, so
// the gap is written down, not left out.
var secretEncodings = []struct {
	name    string
	text    func(secret string) string
	covered bool
	reason  string
}{
	{name: "raw", covered: true, text: func(s string) string { return s }},
	{name: "percent-encoded (query)", covered: true, text: url.QueryEscape},
	{name: "percent-encoded (path)", covered: true, text: url.PathEscape},
	{name: "JSON-escaped", covered: true, text: func(s string) string { return jsonString(s, false) }},
	{name: "JSON-escaped with HTML escaping", covered: true, text: func(s string) string { return jsonString(s, true) }},
	{name: "JSON inside JSON", covered: true, text: func(s string) string { return jsonString(jsonString(s, false), false) }},
	{name: "double-quoted with Go escapes", covered: true, text: func(s string) string { q := strconv.Quote(s); return q[1 : len(q)-1] }},
	{name: "base64 standard", covered: true, text: func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }},
	{name: "base64 standard unpadded", covered: true, text: func(s string) string { return base64.RawStdEncoding.EncodeToString([]byte(s)) }},
	{name: "base64 URL", covered: true, text: func(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }},
	{name: "base64 URL unpadded", covered: true, text: func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }},
	{name: "hex lower", covered: true, text: func(s string) string { return hex.EncodeToString([]byte(s)) }},
	{name: "hex upper", covered: true, text: func(s string) string { return strings.ToUpper(hex.EncodeToString([]byte(s))) }},
	{name: "HTML entities", text: html.EscapeString, reason: "a response body rendered as HTML is not a form a Go log line takes; the value would need a full HTML decoder"},
	{name: "reversed", text: func(s string) string {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes)
	}, reason: "an arbitrary transformation of the value cannot be listed"},
	{name: "base64 of user:secret", text: func(s string) string { return base64.StdEncoding.EncodeToString([]byte("user:" + s)) },
		reason: "a composite is encoded as a whole; a Basic header is redacted by its header name in logging.RedactText"},
	{name: "split in two", text: func(s string) string { return s[:len(s)/2] + " " + s[len(s)/2:] },
		reason: "a value split across fields or lines is never one string"},
}

func TestPlantedSecretExercisesEveryEncoder(t *testing.T) {
	// The standard and URL base64 alphabets, the query and path escapes and the
	// JSON forms must differ for this value, or a row would prove nothing.
	std, urlSafe := base64.StdEncoding.EncodeToString([]byte(plantedValue)), base64.URLEncoding.EncodeToString([]byte(plantedValue))
	if std == urlSafe || strings.TrimRight(std, "=") == std || url.QueryEscape(plantedValue) == plantedValue ||
		jsonString(plantedValue, true) == jsonString(plantedValue, false) || jsonString(plantedValue, false) == plantedValue ||
		strconv.Quote(plantedValue)[1:len(strconv.Quote(plantedValue))-1] == jsonString(plantedValue, false) {
		t.Fatalf("the planted secret does not separate the encoders: std=%q url=%q", std, urlSafe)
	}
}

func TestEverySecretKindIsRedactedInEveryListedEncoding(t *testing.T) {
	for _, kind := range secretKinds {
		for _, encoding := range secretEncodings {
			t.Run(kind.name+"/"+encoding.name, func(t *testing.T) {
				if !encoding.covered {
					t.Skipf("not covered: %s", encoding.reason)
				}
				secrets.ResetRegistered()
				t.Cleanup(secrets.ResetRegistered)
				kind.register(t)
				var out bytes.Buffer
				t.Cleanup(logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo)))
				echoed := encoding.text(plantedValue)
				slog.Error("upstream refused", "error", "echoed "+echoed+" then refused")
				var line map[string]any
				if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &line); err != nil {
					t.Fatalf("not one JSON line: %v: %s", err, out.String())
				}
				got, _ := line["error"].(string)
				if !strings.Contains(got, "echoed") || !strings.Contains(got, "refused") {
					t.Fatalf("the log line lost its context: %q", got)
				}
				if strings.Contains(got, echoed) || strings.Contains(got, plantedValue) {
					t.Fatalf("the secret reached the log in the %s form: %q", encoding.name, got)
				}
			})
		}
	}
}
