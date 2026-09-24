package policy

import (
	"bytes"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestAnUnserializableBodyIsALoggedUnhandledError pins both writers on a
// body the Python api could not serialize either: a lone surrogate (both
// writers) and a non-finite float (json.dumps refuses it; dump_json writes
// null, so WriteModel answers normally). The failure is logged and answered
// as the unhandled-exception 500, marked for the outer middleware, never a
// silent 500 carrying the route's own status headers.
func TestAnUnserializableBodyIsALoggedUnhandledError(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	// The canary sits in every body: the log line must carry the status and
	// the error, never the body.
	const canary = "CANARY_BODY_VALUE"
	surrogate := pyjson.NewObject()
	surrogate.Set("secret", canary)
	surrogate.Set("x", pyjson.FromRunes([]rune{0xd800}))
	nonFinite := pyjson.NewObject()
	nonFinite.Set("x", pyjson.Float(math.Inf(1)))
	nonFinite.Set("secret", canary)
	for _, tc := range []struct {
		name   string
		write  func(http.ResponseWriter, int, pyjson.Value, http.Header)
		body   pyjson.Value
		failed bool
	}{
		{"json surrogate", WriteJSON, surrogate, true},
		{"model surrogate", WriteModel, surrogate, true},
		{"json non-finite", WriteJSON, nonFinite, true},
		{"model non-finite", WriteModel, nonFinite, false},
	} {
		logs.Reset()
		recorder := httptest.NewRecorder()
		tc.write(recorder, http.StatusCreated, tc.body, http.Header{"Retry-After": {"30"}})
		logged := strings.Contains(logs.String(), "api response: body could not be serialized")
		if strings.Contains(logs.String(), canary) {
			t.Errorf("%s: the log carries the body: %s", tc.name, logs.String())
		}
		if !tc.failed {
			if recorder.Code != http.StatusCreated || recorder.Body.String() != `{"x":null,"secret":"CANARY_BODY_VALUE"}` || logged {
				t.Errorf("%s: %d %s logged=%v", tc.name, recorder.Code, recorder.Body.String(), logged)
			}
			continue
		}
		if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` ||
			recorder.Header().Get(UnhandledErrorHeader) != "1" || recorder.Header().Get("Retry-After") != "" || !logged {
			t.Errorf("%s: %d %s headers=%v logged=%v", tc.name, recorder.Code, recorder.Body.String(), recorder.Header(), logged)
		}
	}
}
