//go:build integration

package investment

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// maskedRows is every row of the served tables, the run ids replaced by
// their order, sorted.
func (h *shadowHarness) maskedRows(t *testing.T, runIDs ...string) []string {
	t.Helper()
	var all []string
	for _, table := range servedTables {
		for _, row := range h.dump(t, table) {
			for index, id := range runIDs {
				row = strings.ReplaceAll(row, id, "RUN"+string(rune('1'+index)))
			}
			all = append(all, table+"\x00"+row)
		}
	}
	return all
}

func requireSameRows(t *testing.T, got, want []string) {
	t.Helper()
	if len(want) == 0 {
		t.Fatal("the baseline wrote no rows")
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rows differ:\n batch %q\n  sync %q", got, want)
	}
}

func (h *shadowHarness) runMaterializer(t *testing.T, provider categorize.Provider, cfg Config) Stats {
	t.Helper()
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMaterializer(h.reader, writer, provider, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := m.Run(h.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func (h *shadowHarness) batchConfig(runID string, at time.Time, mode string) Config {
	cfg := h.config(runID, at)
	cfg.ProviderName = "openai"
	cfg.LLMConcurrency = 2
	cfg.LLMBatchMode = mode
	cfg.LLMBatchPollInterval = time.Millisecond
	return cfg
}

// A batch timeout writes the rows a synchronous transport failure writes.
func TestBatchModeTimeoutWritesTheRowsOfASynchronousTransportFailure(t *testing.T) {
	h := newShadowHarness(t)
	at := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 12, 0, 0, 0, time.UTC)

	syncStats := h.runMaterializer(t, fakeProvider(deadEndpoint(t)), h.batchConfig("sync-run", at, LLMBatchModeSync))
	if syncStats.LLMFailures == 0 {
		t.Fatalf("the baseline had no transport failure: %+v", syncStats)
	}
	want := h.maskedRows(t, "sync-run")
	h.truncate(t, servedTables...)

	fake, server := newFakeOpenAI(t)
	fake.statuses = []string{"in_progress"}
	cfg := h.batchConfig("batch-run", at, LLMBatchModeProvider)
	cfg.LLMBatchTimeout = 50 * time.Millisecond
	batchStats := h.runMaterializer(t, fakeProvider(server.URL), cfg)
	requireSameRows(t, h.maskedRows(t, "batch-run"), want)
	if fake.cancelled != 1 || batchStats.LLMFailures != syncStats.LLMFailures {
		t.Fatalf("cancelled %d, failures batch %d sync %d", fake.cancelled, batchStats.LLMFailures, syncStats.LLMFailures)
	}
}

// Through Execute, with the env setting: a provider batch run writes the rows
// a synchronous run with the same answers writes.
func TestBatchModeExecuteWritesTheRowsOfTheSynchronousRun(t *testing.T) {
	h := newShadowHarness(t)
	for _, name := range []string{"LLM_PROVIDER", "TYPESAFE_API_KEY", "INVESTMENT_SHADOW_PROVIDER", "INVESTMENT_SHADOW_ORG_IDS",
		"TYPESAFE_MODEL", "LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL", "LLM_MODEL_OPENAI", envLLMBatchTimeout} {
		t.Setenv(name, "")
	}
	fake, server := newFakeOpenAI(t)
	t.Setenv("LLM_PROVIDER", "openai")
	t.Setenv("OPENAI_API_KEY", "batch-mode-test-placeholder")
	t.Setenv("OPENAI_BASE_URL", server.URL)
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewNativeExecutor(h.reader, writer, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	at := time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC)
	executor.now = func() time.Time { return at }
	scope := `{"force":true,"from_date":"` + h.windowStart.Format("2006-01-02") + `","to_date":"` + h.windowEnd.Format("2006-01-02") +
		`","llm_batch_poll_interval_seconds":0.001}`
	run := func(id string) []string {
		claim := workgraph.Claim{Request: workgraph.Request{
			ID: id, OrganizationID: hierarchyCascadeTestOrg, Kind: workgraph.KindMaterialize, Scope: []byte(scope),
		}, Token: "batch-claim"}
		if _, err := executor.Execute(h.ctx, claim); err != nil {
			t.Fatal(err)
		}
		var runID string
		if err := h.conn.QueryRow(h.ctx, `SELECT any(categorization_run_id) FROM work_unit_investments`).Scan(&runID); err != nil {
			t.Fatal(err)
		}
		return h.maskedRows(t, runID)
	}

	t.Setenv(envLLMBatchMode, "sync")
	want := run("batch-sync")
	syncCalls := fake.syncCalls
	if syncCalls == 0 || len(fake.uploaded) != 0 {
		t.Fatalf("the synchronous baseline made %d calls and uploaded %d lines", syncCalls, len(fake.uploaded))
	}
	h.truncate(t, servedTables...)

	t.Setenv(envLLMBatchMode, "provider_batch")
	got := run("batch-provider")
	requireSameRows(t, got, want)
	if fake.syncCalls != syncCalls || len(fake.uploaded) != syncCalls {
		t.Fatalf("batch run: %d synchronous calls, %d lines uploaded (sync run made %d calls)", fake.syncCalls-syncCalls, len(fake.uploaded), syncCalls)
	}
}
