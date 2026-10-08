package investment

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The outcome set of the served mode is the collector's: one list, in one
// order.
func TestTheServedOutcomeSetIsTheCollectorsSet(t *testing.T) {
	if !slices.Equal(servedOutcomes(), jobruntime.InvestmentServedOutcomes()) {
		t.Fatalf("outcomes differ:\n collector %v\n served    %v", jobruntime.InvestmentServedOutcomes(), servedOutcomes())
	}
}

// servedRunTo runs categorize for every entry with one backend, then finish,
// which reports to the collector. It returns the failures by unit.
func servedRunTo(t *testing.T, served *ServedDecision, collector *jobruntime.MetricsCollector, entries []preprocessed) map[string]*servedFailure {
	t.Helper()
	served.SetObserver(CollectorServedObserver{Collector: collector})
	failures := map[string]*servedFailure{}
	for _, entry := range entries {
		_, err := served.categorize(context.Background(), shadowTestConfig(), entry)
		var failure *servedFailure
		if errors.As(err, &failure) {
			failures[entry.result.Investment.WorkUnitID] = failure
		} else if err != nil {
			t.Fatalf("%s: %v", entry.result.Investment.WorkUnitID, err)
		}
	}
	writer, err := chwrite.NewWriter(&attemptCaptureConn{})
	if err != nil {
		t.Fatal(err)
	}
	served.finish(context.Background(), writer, shadowTestConfig())
	return failures
}

func servedTestClient(t *testing.T, baseURL string, timeout time.Duration, logs *syncBuffer) *categorize.TypeSafeClient {
	t.Helper()
	client, err := categorize.NewTypeSafeClient(categorize.TypeSafeClientConfig{
		APIKey: secrets.NewHidden(shadowTestKeyValue), BaseURL: baseURL, Logger: debugLogger(logs),
		Timeout: timeout, UnsafeAllowAnyBaseURLForTest: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// The state the counter exists to reach: runs against local test endpoints
// with every outcome, and each series holds its count. A refused connection
// sends nothing and is no call in the run's usage; a timeout was sent.
func TestTheServedOutcomeCounterHoldsTheCountOfEachOutcome(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}

	// Run 1: one endpoint, a reply for each unit.
	replies := map[string]func(body []byte) jevReply{
		"u-ok":   func([]byte) jevReply { return okReply() },
		"u-ok2":  func([]byte) jevReply { return okReply() },
		"u-zero": func([]byte) jevReply { return jevReply{supported: map[string]int{}, inputTokens: 1000} },
		"u-none": func([]byte) jevReply { reply := okReply(); reply.evidenceNone = true; return reply },
		"u-bad": func([]byte) jevReply {
			reply := okReply()
			reply.extraAnswers = map[string]string{"support__quality__bugfix": "refusal"}
			return reply
		},
		"u-model": func([]byte) jevReply { reply := okReply(); reply.model = "another-model"; return reply },
		"u-5xx":   func([]byte) jevReply { return jevReply{status: http.StatusServiceUnavailable, body: []byte(`{}`)} },
		"u-429": func([]byte) jevReply {
			return jevReply{status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "0.01"}, body: []byte(`{}`)}
		},
		"u-422": func([]byte) jevReply { return jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{}`)} },
		"u-401": func([]byte) jevReply { return jevReply{status: http.StatusUnauthorized, body: []byte(`{}`)} },
	}
	// u-mixed: the first attempt is rate-limited, the retry is refused as
	// invalid: the LAST attempt names the failure (transport_other).
	mixedAttempts := 0
	replies["u-mixed"] = func([]byte) jevReply {
		mixedAttempts++
		if mixedAttempts == 1 {
			return jevReply{status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "0.01"}, body: []byte(`{}`)}
		}
		return jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{}`)}
	}
	order := []string{"u-ok", "u-ok2", "u-zero", "u-none", "u-bad", "u-model", "u-5xx", "u-429", "u-422", "u-mixed", "u-401"}
	fake := newFakeJev(t, func(_ int, body []byte) jevReply {
		for _, id := range order {
			if strings.Contains(string(body), "Export job times out "+id+" ") || strings.Contains(string(body), "Export job times out "+id+"\\") || strings.Contains(string(body), "Export job times out "+id+`"`) {
				return replies[id](body)
			}
		}
		t.Errorf("a request for no known unit")
		return jevReply{status: http.StatusTeapot, body: []byte(`{}`)}
	})
	served, err := newServedDecision(servedTestClient(t, fake.server.URL, 10*time.Second, logs), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	failures := servedRunTo(t, served, collector, shadowTestEntries(t, order...))

	// Run 2: an adapter defect (a panic in the adapter).
	defect := servedWith(t, fixedServedClassifier{panics: true}, logs)
	servedRunTo(t, defect, collector, shadowTestEntries(t, "u-defect"))

	// Run 3: a timeout (the endpoint never answers; the client timeout wins,
	// two attempts).
	never := make(chan struct{})
	t.Cleanup(func() { close(never) })
	slow := newFakeJev(t, func(int, []byte) jevReply { return jevReply{wait: never} })
	timedOut, err := newServedDecision(servedTestClient(t, slow.server.URL, 150*time.Millisecond, logs), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	for id, failure := range servedRunTo(t, timedOut, collector, shadowTestEntries(t, "u-timeout")) {
		failures[id] = failure
	}

	// Run 4: a refused connection (nothing listens on the address).
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	refused, err := newServedDecision(servedTestClient(t, closedURL, 10*time.Second, logs), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	for id, failure := range servedRunTo(t, refused, collector, shadowTestEntries(t, "u-refused")) {
		failures[id] = failure
	}

	want := map[string]int{
		servedOutcomeOK: 2, servedOutcomeZeroSupport: 1, servedOutcomeEvidenceNone: 1, servedOutcomeInvalidAnswer: 2,
		servedOutcomeAdapterDefect: 1, servedOutcomeTimeout: 1, servedOutcomeRefused: 1, servedOutcomeServerError: 1,
		servedOutcomeRateLimited: 1, servedOutcomeRejected: 1, servedOutcomeTransportOther: 2,
	}
	rendered := collector.PrometheusText()
	for _, outcome := range servedOutcomes() {
		line := fmt.Sprintf(`dev_health_investment_served_outcomes_total{provider="typesafe",model="jev-1.13.0",outcome=%q} %d`, outcome, want[outcome])
		if !strings.Contains(rendered, line+"\n") {
			t.Errorf("missing: %s", line)
		}
	}

	// The failures, by class, and the calls they count in the run's usage.
	for id, wantFailure := range map[string]servedFailure{
		"u-5xx": {class: servedOutcomeServerError, calls: 1}, "u-429": {class: servedOutcomeRateLimited, calls: 1},
		"u-422": {class: servedOutcomeTransportOther, calls: 1}, "u-401": {class: servedOutcomeRejected, calls: 1, deterministic: true},
		"u-mixed":   {class: servedOutcomeTransportOther, calls: 1},
		"u-model":   {class: servedOutcomeInvalidAnswer, calls: 1},
		"u-timeout": {class: servedOutcomeTimeout, calls: 1}, "u-refused": {class: servedOutcomeRefused, calls: 0},
	} {
		got := failures[id]
		if got == nil || got.class != wantFailure.class || got.calls != wantFailure.calls || got.deterministic != wantFailure.deterministic {
			t.Errorf("%s: failure %+v, want %+v", id, got, wantFailure)
		}
	}
	if len(failures) != 8 || mixedAttempts != 2 {
		t.Errorf("%d failures, want 8 (%d attempts of u-mixed): %v", len(failures), mixedAttempts, failures)
	}
	if !strings.Contains(logs.String(), "outcome_refused=1") || !strings.Contains(logs.String(), "outcome_timeout=1") {
		t.Error("the run lines do not hold the outcome counts")
	}
}

// A shadow attempt row of a refused connection carries the class refused: the
// closed set of attempt classes of the shadow sinks holds it.
func TestAShadowAttemptOfARefusedConnectionIsClassedRefused(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	logs := &syncBuffer{}
	phase, err := NewShadowPhase(shadowTestSettings(), servedTestClient(t, closedURL, 10*time.Second, logs), debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = phase.Close() })
	store := &memoryShadowStore{}
	phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u-refused"))
	if len(store.attempts) != 2 {
		t.Fatalf("attempt rows = %d, want 2", len(store.attempts))
	}
	for _, row := range store.attempts {
		if row.ErrorClass != string(categorize.SystemOneClassRefused) || row.HTTPStatus != 0 {
			t.Fatalf("attempt row class %q status %d", row.ErrorClass, row.HTTPStatus)
		}
	}
}
