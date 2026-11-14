package producttelemetry

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// The product telemetry route reads its own body; a lone surrogate in any
// string position is a 422 at that read, with no echo.
func TestProductTelemetryRefusesALoneSurrogateAtTheBoundary(t *testing.T) {
	h := handler{logger: slog.New(slog.DiscardHandler)}
	for _, body := range []string{
		`{"events":[{"name":"a\ud800b"}]}`,
		`{"events":[{"properties":{"k":"\ud800"}}]}`,
		`{"events":[{"properties":{"k\udc00":"v"}}]}`,
		`{"x":["\ud800"]}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/product-telemetry/events", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }()
			h.ServeHTTP(rec, req)
		}()
		text := rec.Body.String()
		lower := strings.ToLower(text)
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(text, `"string_unicode"`) {
			t.Errorf("%s: %d %s, want 422 string_unicode", body, rec.Code, text)
		}
		if !utf8.ValidString(text) || strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(lower, `"input"`) {
			t.Errorf("%s: the refusal echoes the input: %s", body, text)
		}
	}
}
