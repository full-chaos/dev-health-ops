package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const nonFiniteConfidenceCounterName = "dev_health_cicd_nonfinite_release_ref_confidence_total"

type confidenceCell struct {
	value   string
	want    float64
	counted bool
}

// -1e999 -> 1 is a known quirk (ParseFloat returns -Inf with ErrRange, the error branch returns 1); pinned, not changed.
var confidenceCells = []confidenceCell{
	{"NaN", 1, true}, {"nan", 1, true}, {"+Inf", 1, true}, {"Infinity", 1, true}, {"-Inf", 0, true},
	{"1e999", 1, true}, {"-1e999", 1, true},
	{"-0.5", 0, false}, {"1.5", 1, false}, {"0.7", 0.7, false}, {"garbage", 1, false},
}

func confidenceRows(t *testing.T, batch CompleteRouteBatch) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, effect := range batch.Effects {
		for _, raw := range effect.Rows {
			var row deploymentRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			out[row.DeploymentID] = row.ReleaseRefConfidence
		}
	}
	return out
}

func assertConfidenceCell(t *testing.T, provider string, cell confidenceCell, batch CompleteRouteBatch, metrics *providerfoundation.Metrics) {
	t.Helper()
	rows := confidenceRows(t, batch)
	if len(rows) != 3 || rows["601"] != 0.5 || rows["603"] != 1 || rows["602"] != cell.want {
		t.Fatalf("%s %q: rows=%v, want 601=0.5 602=%v 603=1", provider, cell.value, rows, cell.want)
	}
	var rendered strings.Builder
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	line := nonFiniteConfidenceCounterName + `{provider="` + provider + `",dataset="deployments"} 1`
	if got := strings.Contains(rendered.String(), line); got != cell.counted {
		t.Fatalf("%s %q: counter line present=%v, want %v", provider, cell.value, got, cell.counted)
	}
	if !cell.counted && strings.Contains(rendered.String(), nonFiniteConfidenceCounterName+"{") {
		t.Fatalf("%s %q: counter series exists for an uncounted value", provider, cell.value)
	}
}

func gitLabConfidenceBody(badFields string) string {
	return `[
		{"id":601,"iid":1,"status":"success","created_at":"2026-07-22T10:00:00Z","release_ref":"v1","release_ref_confidence":"0.5"},
		{"id":602,"iid":2,"status":"success","created_at":"2026-07-22T09:00:00Z",` + badFields + `},
		{"id":603,"iid":3,"status":"success","created_at":"2026-07-22T08:00:00Z","release_ref":"v1"}
	]`
}

func TestGitLabDeploymentsRouteConfidenceNonFinite(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, cell := range confidenceCells {
			name := cell.value
			if nested {
				name = "nested/" + name
			}
			t.Run(name, func(t *testing.T) {
				value, _ := json.Marshal(cell.value)
				bad := `"release_ref":"v1","release_ref_confidence":` + string(value)
				if nested {
					bad = `"payload":{"release_ref":"v1","release_ref_confidence":` + string(value) + `}`
				}
				doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
					{body: gitLabRepositoryFixture}, {body: `[]`}, {body: gitLabConfidenceBody(bad)},
				}}
				client := gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example")
				client.Metrics = providerfoundation.NewMetrics()
				batch, err := (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("gitlab", "deployments"),
					providerfoundation.Credential{}, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
				if err != nil {
					t.Fatalf("one unreadable confidence failed the whole unit: %v", err)
				}
				assertConfidenceCell(t, "gitlab", cell, batch, client.Metrics)
			})
		}
	}
}

type gitHubConfidenceDoer struct{ bad string }

func (doer gitHubConfidenceDoer) Do(request *http.Request) (*http.Response, error) {
	body := `[]`
	switch request.URL.Path {
	case "/repos/acme/api":
		body = gitHubRepositoryFixture
	case "/repos/acme/api/deployments":
		body = `[
			{"id":601,"created_at":"2026-07-22T10:00:00Z","payload":{"release_ref":"v1","release_ref_confidence":"0.5"}},
			{"id":602,"created_at":"2026-07-22T09:00:00Z","payload":{"release_ref":"v1","release_ref_confidence":` + doer.bad + `}},
			{"id":603,"created_at":"2026-07-22T08:00:00Z","payload":{"release_ref":"v1"}}
		]`
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestGitHubDeploymentsRouteConfidenceNonFinite(t *testing.T) {
	for _, cell := range confidenceCells {
		t.Run(cell.value, func(t *testing.T) {
			value, _ := json.Marshal(cell.value)
			client := gitHubRepositoryClient(t, fakehttp.Client(gitHubConfidenceDoer{bad: string(value)}), "https://api.github.com")
			client.Metrics = providerfoundation.NewMetrics()
			batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"),
				providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("one unreadable confidence failed the whole unit: %v", err)
			}
			assertConfidenceCell(t, "github", cell, batch, client.Metrics)
		})
	}
}

func TestGitHubDeploymentsConfidenceKeepsNoTrim(t *testing.T) {
	client := gitHubRepositoryClient(t, fakehttp.Client(gitHubConfidenceDoer{bad: `" 0.25"`}), "https://api.github.com")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"),
		providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got := confidenceRows(t, batch)["602"]; got != 1 {
		t.Fatalf("github confidence with a leading space = %v, want 1 (no trim, as at base)", got)
	}
}

func TestGitLabDeploymentsConfidenceTrimsSpace(t *testing.T) {
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture}, {body: `[]`}, {body: gitLabConfidenceBody(`"release_ref":"v1","release_ref_confidence":" 0.25 "`)},
	}}
	client := gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example")
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("gitlab", "deployments"),
		providerfoundation.Credential{}, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got := confidenceRows(t, batch)["602"]; got != 0.25 {
		t.Fatalf("gitlab confidence with spaces = %v, want 0.25", got)
	}
}
