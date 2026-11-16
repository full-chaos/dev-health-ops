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
)

type gitLabTestReportDoer struct {
	inner providerfoundation.HTTPDoer
	body  string
}

func (doer gitLabTestReportDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/test_report") {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
			ContentLength: -1, Body: io.NopCloser(strings.NewReader(doer.body)), Request: request}, nil
	}
	return doer.inner.Do(request)
}

func nonFiniteReportBody(suiteTime, caseTime string) string {
	suite := func(name, total, execution string) string {
		return `{"name":"` + name + `","total_time":"` + total + `","test_cases":[{"name":"case-` + name +
			`","classname":"tests.T","status":"success","execution_time":"` + execution + `"}]}`
	}
	return `{"test_suites":[` + strings.Join([]string{
		suite("a", "1.25", "1.25"), suite("b", "2.5", "2.5"), suite("c", "3.75", "3.75"), suite("bad", suiteTime, caseTime),
	}, ",") + `]}`
}

type nonFiniteWalk struct {
	batch   CompleteRouteBatch
	metrics *providerfoundation.Metrics
}

func nonFiniteCollect(t *testing.T, route, body string) nonFiniteWalk {
	t.Helper()
	claim := nativeTestClaim("gitlab", "tests")
	inner := &gitLabAllRefDoer{t: t, pipelines: gitLabAllRefPipelines()[:1]}
	client := gitLabRepositoryClient(t, gitLabTestReportDoer{inner: inner, body: body}, "https://gitlab.example")
	client.Metrics = providerfoundation.NewMetrics()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := nonFiniteWalk{metrics: client.Metrics}
	if route == "collect" {
		batch, err := (GitLabTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
		if err != nil {
			t.Fatalf("%s: one non-finite time failed the whole unit: %v", route, err)
		}
		walk.batch = batch
		return walk
	}
	err := (GitLabTestsRouteHandler{}).CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, now, "",
		func(emission ChunkRouteEmission) error {
			walk.batch.Effects = append(walk.batch.Effects, emission.Batch.Effects...)
			return nil
		})
	if err != nil {
		t.Fatalf("%s: one non-finite time failed the whole unit: %v", route, err)
	}
	return walk
}

func nonFiniteDurations(t *testing.T, batch CompleteRouteBatch, destination, nameField string) map[string]*float64 {
	t.Helper()
	out := map[string]*float64{}
	for _, effect := range batch.Effects {
		if effect.Destination != destination {
			continue
		}
		for _, raw := range effect.Rows {
			var row struct {
				Suite    string   `json:"suite_name"`
				Case     string   `json:"case_name"`
				Duration *float64 `json:"duration_seconds"`
			}
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			key := row.Suite
			if nameField == "case_name" {
				key = row.Case
			}
			out[key] = row.Duration
		}
	}
	return out
}

func wantDuration(t *testing.T, label string, got map[string]*float64, name string, want *float64) {
	t.Helper()
	value, present := got[name]
	if !present {
		t.Fatalf("%s: row %q not stored (rows %v)", label, name, got)
	}
	switch {
	case want == nil && value != nil:
		t.Fatalf("%s: %q duration=%v, want NULL", label, name, *value)
	case want != nil && (value == nil || *value != *want):
		t.Fatalf("%s: %q duration=%v, want %v", label, name, value, *want)
	}
}

func nonFiniteFloatPtr(value float64) *float64 { return &value }

func TestGitLabTestsRouteNonFiniteSuiteTimeIsNullAndUnitContinues(t *testing.T) {
	for _, route := range []string{"collect", "chunked"} {
		for _, value := range []string{"NaN", "+Inf", "-Inf", "Infinity", "1e999", "-3", "1.25"} {
			t.Run(route+"/"+value, func(t *testing.T) {
				walk := nonFiniteCollect(t, route, nonFiniteReportBody(value, "0.5"))
				suites := nonFiniteDurations(t, walk.batch, "test_suite_results", "suite_name")
				cases := nonFiniteDurations(t, walk.batch, "test_case_results", "case_name")
				if len(suites) != 4 || len(cases) != 4 {
					t.Fatalf("suites=%d cases=%d, want 4 and 4 (the other suites and runs must be stored)", len(suites), len(cases))
				}
				wantDuration(t, value, suites, "a", nonFiniteFloatPtr(1.25))
				wantDuration(t, value, suites, "b", nonFiniteFloatPtr(2.5))
				wantDuration(t, value, suites, "c", nonFiniteFloatPtr(3.75))
				switch value {
				case "-3":
					wantDuration(t, value, suites, "bad", nonFiniteFloatPtr(-3))
				case "1.25":
					wantDuration(t, value, suites, "bad", nonFiniteFloatPtr(1.25))
				default:
					wantDuration(t, value, suites, "bad", nil)
				}
				wantDuration(t, value, cases, "case-bad", nonFiniteFloatPtr(0.5))
				assertNonFiniteCounter(t, walk, route, value)
			})
		}
	}
}

func TestGitLabTestsRouteNonFiniteCaseTimeIsNullAndUnitContinues(t *testing.T) {
	for _, route := range []string{"collect", "chunked"} {
		for _, value := range []string{"NaN", "+Inf", "-Inf", "Infinity", "1e999", "-3", "1.25"} {
			t.Run(route+"/"+value, func(t *testing.T) {
				walk := nonFiniteCollect(t, route, nonFiniteReportBody("9.5", value))
				suites := nonFiniteDurations(t, walk.batch, "test_suite_results", "suite_name")
				cases := nonFiniteDurations(t, walk.batch, "test_case_results", "case_name")
				if len(suites) != 4 || len(cases) != 4 {
					t.Fatalf("suites=%d cases=%d, want 4 and 4", len(suites), len(cases))
				}
				wantDuration(t, value, suites, "bad", nonFiniteFloatPtr(9.5))
				wantDuration(t, value, cases, "case-a", nonFiniteFloatPtr(1.25))
				switch value {
				case "-3":
					wantDuration(t, value, cases, "case-bad", nonFiniteFloatPtr(-3))
				case "1.25":
					wantDuration(t, value, cases, "case-bad", nonFiniteFloatPtr(1.25))
				default:
					wantDuration(t, value, cases, "case-bad", nil)
				}
				assertNonFiniteCounter(t, walk, route, value)
			})
		}
	}
}
