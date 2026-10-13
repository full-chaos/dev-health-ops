//go:build integration

package fixturescli

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// TestDemoWorldEveryWorkUnitPassesTheEvidenceGate loads the demo world as `fixtures generate` does and runs
// the production investment materializer over it: no work unit may fall to the evidence gate (too few
// characters, or no text source), or the demo organization's investment answers are the flat fallback.
func TestDemoWorldEveryWorkUnitPassesTheEvidenceGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	world, err := LoadFrozenWorld(demoWorld.Params)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := LoadWorld(ctx, conn, world, demoWorld.OrgID, now); err != nil {
		t.Fatal(err)
	}
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := investment.NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := materializer.Run(ctx, investment.Config{
		OrgID: demoWorld.OrgID, FromTS: now.AddDate(0, 0, -demoDepthDays), ToTS: now,
		RunID: "demo-world-evidence-gate", ComputedAt: now, ProviderName: "mock",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.Components == 0 {
		t.Fatalf("the materializer built no work unit from the demo world: %+v", stats)
	}
	rows, err := conn.Query(ctx, `SELECT categorization_status, count() FROM work_unit_investments FINAL WHERE org_id = ? GROUP BY categorization_status`, demoWorld.OrgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	written := 0
	for rows.Next() {
		var status string
		var count uint64
		if err := rows.Scan(&status, &count); err != nil {
			t.Fatal(err)
		}
		written += int(count)
		if status == categorize.StatusInsufficientChars || status == categorize.StatusNoTextSources {
			t.Errorf("%d work unit(s) fell to the evidence gate (%s): their text is too short for the categorizer", count, status)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if written == 0 || stats.LLMCalls < stats.Components {
		t.Errorf("components %d, investment rows %d, categorizer calls %d: every unit must reach the categorizer", stats.Components, written, stats.LLMCalls)
	}
	t.Logf("components %d, investment rows %d, categorizer calls %d", stats.Components, written, stats.LLMCalls)
}
