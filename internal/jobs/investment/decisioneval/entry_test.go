package decisioneval

// Env-gated entry points. A plain `go test ./...` skips both: they run only when
// DECISIONEVAL_RUN=1 or DECISIONEVAL_SCORE=1 is set, and a live run needs
// DECISIONEVAL_LIVE=1 and a spend cap > 0 for each provider.

import (
	"context"
	"os"
	"testing"
)

func TestRunExperiment(t *testing.T) {
	if !isOne(os.Getenv(EnvRun)) {
		t.Skipf("set %s=1 to run the experiment runner (dry run or live)", EnvRun)
	}
	cfg, err := RunConfigFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Out = os.Stdout
	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if stopped := summary.StoppedArms(); len(stopped) > 0 {
		t.Fatalf("arms stopped early: %v (summary %+v)", stopped, summary.Arms)
	}
	for arm, a := range summary.Arms {
		t.Logf("arm=%s planned=%d sent=%d rendered=%d skipped_resume=%d gate=%d not_run=%d states=%v", arm, a.Planned, a.Sent, a.Rendered, a.SkippedResume, a.Gate, a.NotRun, a.States)
	}
	t.Logf("spend_usd=%v run_id=%s", summary.Spend, summary.RunID)
}

func TestScoreExperiment(t *testing.T) {
	if !isOne(os.Getenv(EnvScore)) {
		t.Skipf("set %s=1 to run the scorer over a ledger", EnvScore)
	}
	cfg, err := ScoreConfigFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	m, err := Score(context.Background(), cfg)
	if err != nil {
		t.Fatal(err) // INVALID metrics: non-zero exit with the named reasons
	}
	t.Logf("metrics written to %s (arms %v)", cfg.ReportDir, m.ArmOrder)
}

func TestFullExperiment(t *testing.T) {
	if !isOne(os.Getenv(EnvFull)) {
		t.Skipf("set %s=1 to score an unlabeled full run (coverage, cost, latency, disagreement export)", EnvFull)
	}
	cfg, err := FullConfigFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	m, err := ScoreFull(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("full run: population %d, union stratum %d, sample %d, report in %s", m.FullPopulation, m.UnionStratum, len(m.Sample), cfg.ReportDir)
}

func TestDecideExperiment(t *testing.T) {
	if !isOne(os.Getenv(EnvDecide)) {
		t.Skipf("set %s=1 to apply the estimator and the gates of design 9.5 and 9.6", EnvDecide)
	}
	cfg, err := DecideConfigFromEnv(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Decide(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range d.Candidates {
		t.Logf("candidate %s: %s", c.Arm, c.Outcome)
	}
}

// TestRealFixturesRoundTrip checks the fixture parser and both request builders
// over a real export (no network, no provider). It runs only when
// DECISIONEVAL_REAL_FIXTURES names a fixtures JSONL.
func TestRealFixturesRoundTrip(t *testing.T) {
	path := os.Getenv("DECISIONEVAL_REAL_FIXTURES")
	if path == "" {
		t.Skip("set DECISIONEVAL_REAL_FIXTURES=<local-bundles.jsonl> to check the parser over a real export")
	}
	fixtures, err := LoadFixtures(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	r := testRubric(t)
	gated, below, maxTokens, sumTokens := 0, 0, 0, 0
	for _, f := range fixtures {
		b, err := f.Bundle()
		if err != nil {
			t.Fatalf("%s: %v", f.BundleID, err)
		}
		if GateStatus(b) != "" {
			below++
			continue
		}
		gated++
		jev, err := BuildJevRequest(r, DefaultJevModel, b)
		if err != nil {
			t.Fatalf("%s: %v", f.BundleID, err)
		}
		dec, err := BuildDecisionsRequest(r, DefaultLunaModel, b)
		if err != nil {
			t.Fatalf("%s: %v", f.BundleID, err)
		}
		if jev.QuestionCount() != 21 || dec.QuestionCount() != 21 || jev.SpansDropped != 0 {
			t.Fatalf("%s: questions %d/%d dropped %d", f.BundleID, jev.QuestionCount(), dec.QuestionCount(), jev.SpansDropped)
		}
		sumTokens += jev.EstimatedInputTokens
		maxTokens = max(maxTokens, dec.EstimatedInputTokens)
	}
	t.Logf("fixtures=%d gate-pass=%d below-gate=%d mean jev est tokens=%d max decisions est tokens=%d", len(fixtures), gated, below, sumTokens/max(gated, 1), maxTokens)
}
