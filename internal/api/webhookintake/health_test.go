package webhookintake

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthBodyIsTheSameForEverySecretState(t *testing.T) {
	const want = `{"status":"ok","celery_available":false}`
	for name, secrets := range map[string]Secrets{
		"none": {},
		"one":  {GitHub: "a"},
		"all":  {GitHub: "a", GitLab: "b", Jira: "c"},
	} {
		recorder := httptest.NewRecorder()
		Deps{Secrets: secrets}.handleHealth()(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/webhooks/health", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s: status %d", name, recorder.Code)
		}
		if got := recorder.Body.String(); got != want && got != want+"\n" {
			t.Fatalf("%s: body %q, want %q", name, got, want)
		}
	}
}
