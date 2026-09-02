package edges

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// TestEveryOutcomeHasAMetricLabel is the guard that makes a new derivation
// outcome impossible to ship unobserved. allOutcomes is the deriver's own
// declared vocabulary, so adding a sixth Outcome without a label fails here.
func TestEveryOutcomeHasAMetricLabel(t *testing.T) {
	for _, outcome := range allOutcomes() {
		label, known := observedOutcome[outcome]
		if !known {
			t.Errorf("outcome %q has no metric label: rows with this disposition would be "+
				"counted by nothing and the tally would not sum to rows read", outcome)
			continue
		}
		if string(label) != string(outcome) {
			t.Errorf("outcome %q maps to label %q; the two vocabularies must read alike "+
				"or a dashboard cannot be traced back to the code", outcome, label)
		}
	}
	if len(observedOutcome) != len(allOutcomes()) {
		t.Errorf("%d labels for %d outcomes: a label with no outcome can never be emitted",
			len(observedOutcome), len(allOutcomes()))
	}
}

// TestTheGoldenTallyIsPublishable runs the real frozen derivation through the
// real collector. A tally that does not partition its input is refused, so this
// asserts the deriver's accounting and the metric's contract at once.
func TestTheGoldenTallyIsPublishable(t *testing.T) {
	document := loadGolden(t)
	buildClock, err := parseGoldenInstant(document.FrozenNow)
	if err != nil {
		t.Fatalf("frozen_now: %v", err)
	}
	result, err := DeriveIssueIssueEdges(goldenDependencyRows(t, document), buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}

	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatalf("collector: %v", err)
	}
	if err := ObserveDerivation(collector, result, len(document.Dependencies), 250*time.Millisecond); err != nil {
		t.Fatalf("the frozen derivation is not publishable: %v", err)
	}

	var exposition strings.Builder
	if err := collector.WritePrometheus(&exposition); err != nil {
		t.Fatalf("write prometheus: %v", err)
	}
	rendered := exposition.String()
	for _, outcome := range allOutcomes() {
		want := `worker_work_graph_issue_edge_rows_total{outcome="` + string(outcome) + `"}`
		if !strings.Contains(rendered, want) {
			t.Errorf("no series for %q; an outcome that never occurred must render as 0, "+
				"not vanish -- absent and zero look identical on a dashboard but only one "+
				"proves the counter is wired", outcome)
		}
	}
}

// TestATallyThatDoesNotPartitionIsRefused. The counters are only meaningful
// together: if they summed to less than the rows read, a dropped row would hide
// behind counters that each still looked sane.
func TestATallyThatDoesNotPartitionIsRefused(t *testing.T) {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatalf("collector: %v", err)
	}
	short := DeriveResult{Counts: map[Outcome]int{OutcomeEmitted: 3}}
	if err := ObserveDerivation(collector, short, 4, time.Second); err == nil {
		t.Fatal("a tally of 3 against 4 rows read was accepted; one row would be unaccounted for")
	}
}

func TestANilObserverIsRefused(t *testing.T) {
	if err := ObserveDerivation(nil, DeriveResult{}, 0, 0); err == nil {
		t.Fatal("a nil observer was accepted: that is how a cutover ships unwired")
	}
}

// TestTheTallyNamesEveryOutcomeEvenAtZero. A derivation in which nothing was
// malformed and nothing was skipped for an empty id must SAY so, rather than
// omitting the keys. Absent and zero are different facts — "this never
// happened" versus "this is not being counted" — and only the second is a
// defect, so they must not look alike to a reader or a dashboard.
func TestTheTallyNamesEveryOutcomeEvenAtZero(t *testing.T) {
	document := loadGolden(t)
	buildClock, err := parseGoldenInstant(document.FrozenNow)
	if err != nil {
		t.Fatalf("frozen_now: %v", err)
	}
	result, err := DeriveIssueIssueEdges(goldenDependencyRows(t, document), buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	for _, outcome := range allOutcomes() {
		if _, named := result.Counts[outcome]; !named {
			t.Errorf("outcome %q is absent from the tally rather than present at zero", outcome)
		}
	}
	if len(result.Counts) != len(allOutcomes()) {
		t.Errorf("tally has %d keys for %d outcomes", len(result.Counts), len(allOutcomes()))
	}
	t.Logf("full tally: %v", result.Counts)
}
