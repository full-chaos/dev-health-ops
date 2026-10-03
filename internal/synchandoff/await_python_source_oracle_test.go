package synchandoff

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type pythonAwaitFamily struct {
	Kind    string    `json:"kind"`
	Name    string    `json:"name"`
	Labels  []string  `json:"labels"`
	Buckets []float64 `json:"buckets"`
}

type pythonAwaitSurface struct {
	Families          map[string]pythonAwaitFamily `json:"families"`
	RecordOutcomes    []string                     `json:"record_outcomes"`
	LabelKeywordsUsed []string                     `json:"label_keywords_used"`
}

// CHAOS-8268: the Go sync_manual_trigger_await_* families against the Python PRODUCTION source. Go tests alone cannot say whether
// the two agree, so the family names, kinds, label keys, histogram buckets and outcome label values are DERIVED from
// src/dev_health_ops/metrics/prometheus.py and src/dev_health_ops/sync/execution_trigger.py (testdata/await_python_source.py, ast,
// nothing hand-written) and compared to what the Go registry actually exposes.
// NAMED LIMIT: the producers are read from their syntax trees, not executed (prometheus_client, SQLAlchemy and the project models are
// not in the live-oracle closure), so a runtime-only change (a label added by a wrapper) is not seen.
// Runs only through ci/check_go.sh live-python-oracles.
func TestAwaitFamiliesMatchThePythonProductionSource(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root %s has no go.mod: %v", root, err)
	}
	python := os.Getenv("PYTHON")
	if python == "" {
		t.Fatal("PYTHON is required")
	}
	out, err := exec.Command(python, "testdata/await_python_source.py", root).Output()
	if err != nil {
		t.Fatalf("derive the Python surface: %v", err)
	}
	var surface pythonAwaitSurface
	if err := json.Unmarshal(out, &surface); err != nil {
		t.Fatalf("decode the Python surface: %v", err)
	}
	counter, histogram := surface.Families["SYNC_MANUAL_TRIGGER_AWAIT_OUTCOME_TOTAL"], surface.Families["SYNC_MANUAL_TRIGGER_AWAIT_LATENCY_SECONDS"]

	// What Go exposes after one observation of every Wait state.
	metrics := newAwaitMetrics()
	for _, state := range []State{StatePending, StateMaterialized, StateQuarantined, StateTerminal} {
		metrics.observe(state, 120*time.Millisecond)
	}
	var b strings.Builder
	if err := metrics.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	text := b.String()

	if counter.Name != awaitOutcomeMetric || histogram.Name != awaitLatencyMetric {
		t.Errorf("family names: python %q / %q, go %q / %q", counter.Name, histogram.Name, awaitOutcomeMetric, awaitLatencyMetric)
	}
	for _, want := range []string{"# TYPE " + counter.Name + " " + strings.ToLower(counter.Kind), "# TYPE " + histogram.Name + " " + strings.ToLower(histogram.Kind)} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("the Go exposition has no %q line", want)
		}
	}
	if !reflect.DeepEqual(counter.Labels, []string{"outcome"}) || !reflect.DeepEqual(histogram.Labels, []string{"outcome"}) {
		t.Errorf("python label keys %v / %v, the Go families use only outcome", counter.Labels, histogram.Labels)
	}
	if !reflect.DeepEqual(surface.LabelKeywordsUsed, []string{"outcome"}) {
		t.Errorf("python .labels() keywords %v, want [outcome]", surface.LabelKeywordsUsed)
	}
	if !reflect.DeepEqual(histogram.Buckets, awaitBuckets) {
		t.Errorf("histogram buckets: python %v, go %v", histogram.Buckets, awaitBuckets)
	}
	sorted := append([]string(nil), awaitOutcomeNames...)
	sort.Strings(sorted)
	if !reflect.DeepEqual(surface.RecordOutcomes, sorted) {
		t.Errorf("outcome label values: python _record(...) calls %v, go %v", surface.RecordOutcomes, sorted)
	}
	// Every outcome the Go side can write is one Python writes, and every Python outcome appears in the Go exposition.
	pythonOutcomes := map[string]bool{}
	for _, o := range surface.RecordOutcomes {
		pythonOutcomes[o] = true
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, awaitOutcomeMetric+`{outcome="`) {
			label := strings.SplitN(strings.TrimPrefix(line, awaitOutcomeMetric+`{outcome="`), `"`, 2)[0]
			if !pythonOutcomes[label] {
				t.Errorf("go writes outcome %q, which no python _record(...) call writes", label)
			}
			delete(pythonOutcomes, label)
		}
	}
	if len(pythonOutcomes) != 0 {
		t.Errorf("python outcomes with no Go series after observing every state: %v", pythonOutcomes)
	}
	if !t.Failed() {
		if err := os.WriteFile(filepath.Join(proofDirectory, "synchandoff-await-python-source"), []byte("executed"), 0o644); err != nil {
			t.Fatalf("write live-python-oracle proof: %v", err)
		}
	}
}
