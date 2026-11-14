package policy

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// Every Guard.Body* route reads its body through one place; a lone surrogate
// is a 422 there, before the handler runs, with no echo.
func TestGuardBodyReadRefusesALoneSurrogateBeforeTheHandler(t *testing.T) {
	reached := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached++; w.WriteHeader(http.StatusOK) })
	guard := NewGuard(nil, slog.New(slog.DiscardHandler))
	handler := guard.BodyAfter(next)
	for _, body := range []string{`{"v":"\ud800"}`, `{"v":["\udc00"]}`, `{"v":{"k":"\ud800"}}`, `{"v":{"k\ud800":"x"}}`} {
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		raw := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(raw, `"string_unicode"`) {
			t.Errorf("%s: %d %s, want 422 string_unicode", body, rec.Code, raw)
		}
		lower := strings.ToLower(raw)
		if !utf8.ValidString(raw) || strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(lower, `"input"`) {
			t.Errorf("%s: the refusal echoes the input: %s", body, raw)
		}
	}
	if reached != 0 {
		t.Errorf("the handler ran %d times for a refused body", reached)
	}
	clean := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"v":"ok","p":"`+"\\ud83d"+"\\ude00"+`"}`))
	clean.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, clean)
	if rec.Code != http.StatusOK || reached != 1 {
		t.Errorf("a clean body: %d, handler ran %d times, want 200 once", rec.Code, reached)
	}
}
