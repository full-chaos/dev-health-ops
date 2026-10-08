//go:build integration

package investment

// The switch-off probe of the served decision mode (CHAOS-8874, CHAOS-8914):
// with today's provider values the generative path must write the same rows
// from the same requests as before the decision backend existed. Each test
// prints ONE sha256 over every generative request and every row of the four
// served tables after two runs (forced, then not forced). The digest is the
// evidence; it is compared between two commits, not pinned here, because the
// seeded dates follow the calendar day (the sinks refuse old rows):
//
//	go test -count=1 -tags=integration -run 'TestSwitchOffProbe' -v ./internal/jobs/investment/
//
// on each commit on the same day (copy this file into a commit that lacks it);
// equal digests = equal requests and rows.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

type probeProvider struct {
	mu       sync.Mutex
	requests []string
	inner    categorize.MockProvider
}

func (p *probeProvider) Complete(ctx context.Context, request categorize.CompletionRequest) (categorize.CompletionResult, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request.SystemMessage+"\x00"+request.Prompt+"\x00"+request.ResponseFormatName)
	p.mu.Unlock()
	return p.inner.Complete(ctx, request)
}
func (p *probeProvider) Close() error  { return nil }
func (p *probeProvider) Model() string { return p.inner.Model() }

func TestSwitchOffProbeMaterializer(t *testing.T) {
	h := newShadowHarness(t)
	logs := &syncBuffer{}
	provider := &probeProvider{}
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	// A fixed clock inside the retention: the first day of the current month.
	now := time.Now().UTC()
	at := time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC)
	digest := sha256.New()
	for run, force := range []bool{true, false} {
		m, err := NewMaterializer(h.reader, writer, provider, debugLogger(logs))
		if err != nil {
			t.Fatal(err)
		}
		cfg := h.config("probe-run-"+string(rune('1'+run)), at.Add(time.Duration(run)*time.Hour))
		cfg.Force = force
		cfg.LLMConcurrency = 1
		stats, err := m.Run(h.ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("run %d: records %d skipped %d llm calls %d", run+1, stats.Records, stats.SkippedExisting, stats.LLMCalls)
	}
	sort.Strings(provider.requests)
	for _, request := range provider.requests {
		digest.Write([]byte(request))
		digest.Write([]byte{1})
	}
	rows := 0
	for _, table := range servedTables {
		for _, row := range h.dump(t, table) {
			digest.Write([]byte(table + "\x00" + row + "\x01"))
			rows++
		}
	}
	if len(provider.requests) == 0 || rows == 0 {
		t.Fatalf("nothing was measured: %d requests, %d rows", len(provider.requests), rows)
	}
	t.Logf("SWITCH_OFF_PROBE requests=%d rows=%d sha256=%s", len(provider.requests), rows, hex.EncodeToString(digest.Sum(nil)))
}

// The worker's executor path with today's LLM_PROVIDER value (mock), through
// Execute: the run ids are fresh for each run, so they are replaced by their
// order before the digest.
func TestSwitchOffProbeExecutor(t *testing.T) {
	h := newShadowHarness(t)
	for _, name := range []string{"LLM_PROVIDER", "TYPESAFE_API_KEY", "INVESTMENT_SHADOW_PROVIDER", "INVESTMENT_SHADOW_ORG_IDS", "TYPESAFE_MODEL"} {
		t.Setenv(name, "")
	}
	t.Setenv("LLM_PROVIDER", "mock")
	writer, err := chwrite.NewWriter(h.conn)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewNativeExecutor(h.reader, writer, debugLogger(&syncBuffer{}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	at := time.Date(now.Year(), now.Month(), 1, 12, 0, 0, 0, time.UTC)
	for run, scope := range []string{`{"force":true,"from_date":"FROM","to_date":"TO"}`, `{"from_date":"FROM","to_date":"TO"}`} {
		executor.now = func() time.Time { return at.Add(time.Duration(run) * time.Hour) }
		scope = strings.ReplaceAll(scope, "FROM", h.windowStart.Format("2006-01-02"))
		scope = strings.ReplaceAll(scope, "TO", h.windowEnd.Format("2006-01-02"))
		claim := workgraph.Claim{Request: workgraph.Request{
			ID: "probe-" + string(rune('1'+run)), OrganizationID: hierarchyCascadeTestOrg,
			Kind: workgraph.KindMaterialize, Scope: []byte(scope),
		}, Token: "probe-claim"}
		out, err := executor.Execute(h.ctx, claim)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("run %d evidence bytes %d", run+1, len(out))
	}
	runs, err := h.conn.Query(h.ctx, `SELECT categorization_run_id FROM work_unit_investments GROUP BY categorization_run_id ORDER BY min(computed_at)`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for runs.Next() {
		var id string
		_ = runs.Scan(&id)
		ids = append(ids, id)
	}
	_ = runs.Close()
	digest := sha256.New()
	rows := 0
	for _, table := range servedTables {
		// Masked BEFORE sorting: h.dump sorts by the raw text, and the raw run
		// ids are random, so sorting first makes the order depend on them.
		masked := h.dump(t, table)
		for index, row := range masked {
			for i, id := range ids {
				row = strings.ReplaceAll(row, id, "RUN"+string(rune('1'+i)))
			}
			masked[index] = row
		}
		sort.Strings(masked)
		for _, row := range masked {
			digest.Write([]byte(table + "\x00" + row + "\x01"))
			rows++
		}
	}
	if rows == 0 || len(ids) == 0 {
		t.Fatalf("nothing was measured: %d rows, %d runs", rows, len(ids))
	}
	t.Logf("SWITCH_OFF_EXECUTOR_PROBE runs=%d rows=%d sha256=%s", len(ids), rows, hex.EncodeToString(digest.Sum(nil)))
}
