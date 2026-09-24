package orgs

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestWriteJSONIsThePolicyWriter drives the package's writeJSON adapter: a
// serializable body is json.dumps bytes at the given status; a body
// json.dumps refuses is the unhandled-error 500.
func TestWriteJSONIsThePolicyWriter(t *testing.T) {
	body := pyjson.NewObject()
	body.Set("name", "café")
	body.Set("n", pyjson.Float(1.5))
	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusCreated, body)
	if recorder.Code != http.StatusCreated || recorder.Body.String() != `{"name":"café","n":1.5}` ||
		recorder.Header().Get("Content-Type") != "application/json" || recorder.Header().Get(policy.UnhandledErrorHeader) != "" {
		t.Errorf("serializable: %d %s %v", recorder.Code, recorder.Body.String(), recorder.Header())
	}

	refused := pyjson.NewObject()
	refused.Set("n", pyjson.Float(math.NaN()))
	recorder = httptest.NewRecorder()
	writeJSON(recorder, http.StatusOK, refused)
	if recorder.Code != http.StatusInternalServerError || recorder.Body.String() != `{"detail":"Internal Server Error"}` ||
		recorder.Header().Get(policy.UnhandledErrorHeader) != "1" {
		t.Errorf("refused: %d %s %v", recorder.Code, recorder.Body.String(), recorder.Header())
	}
}
