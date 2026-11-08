package apiservice

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func TestExternalIngestSchemaRoutesKeepThePolicyWriterCheckQuiet(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(Deps{}, quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	before := policy.WriterViolations()
	for _, target := range []string{
		"/api/v1/external-ingest/schemas",
		"/api/v1/external-ingest/schemas/external-ingest.v1",
	} {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s: %d %s", target, recorder.Code, recorder.Body.String())
		}
	}
	if got := policy.WriterViolations() - before; got != 0 {
		t.Errorf("%d writer violations on the schema routes", got)
	}
	if strings.Contains(logs.String(), "wrong body writer") {
		t.Errorf("policy logged a wrong-body-writer line:\n%s", logs.String())
	}
}
