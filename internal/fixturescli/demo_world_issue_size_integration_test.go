//go:build integration

package fixturescli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// issueSizeSummary is one provider's units in the demo world after a materialization: how many, how many
// carry a size, and the sum of their sizes.
type issueSizeSummary struct {
	Units      int     `json:"units"`
	SizedUnits int     `json:"sized_units"`
	SizeSum    float64 `json:"size_sum"`
}

// demoWorldIssueSizeGolden is the per-provider size summary of the demo world (seed 9440, every provider)
// materialized by the real materializer. The jira, gitlab and linear issues of the world carry story points;
// its github issues carry none, so every github unit weighs one.
var demoWorldIssueSizeGolden = map[string]issueSizeSummary{
	"github": {Units: 14, SizedUnits: 0, SizeSum: 0},
	"gitlab": {Units: 41, SizedUnits: 25, SizeSum: 88},
	"jira":   {Units: 52, SizedUnits: 24, SizeSum: 93},
	"linear": {Units: 45, SizedUnits: 31, SizeSum: 114},
	"none":   {Units: 8, SizedUnits: 0, SizeSum: 0},
}

// TestDemoWorldIssueSizeIsTheSumOfTheUnitIssuesStoryPoints runs the real materializer over the demo world
// (jira, gitlab, linear and github) and checks every unit's work_unit_issue_size row against an independent
// computation from the stored work items: the sum of the positive story points of the unit's issues. It then
// changes one issue's size and runs again: the unit's investment row is skipped as unchanged, and its size
// row still follows the new size.
func TestDemoWorldIssueSizeIsTheSumOfTheUnitIssuesStoryPoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	prCommitLoader, err := prcommit.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	prCommitWriter, err := prcommit.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	prCommitService, err := prcommit.NewService(prCommitLoader, prCommitWriter, conn, logger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prCommitService.ProduceLinks(ctx, demoWorld.OrgID, prcommit.Window{}); err != nil {
		t.Fatal(err)
	}
	if _, err := prCommitService.ProduceEdges(ctx, demoWorld.OrgID, prcommit.Window{}); err != nil {
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
	materializer, err := investment.NewMaterializer(reader, writer, categorize.MockProvider{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	cfg := investment.Config{
		OrgID: demoWorld.OrgID, FromTS: now.AddDate(0, 0, -demoDepthDays), ToTS: now,
		RunID: "demo-world-issue-size-1", ComputedAt: now, ProviderName: "mock",
	}
	first, err := materializer.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	type unit struct {
		provider string
		issues   []string
	}
	loadUnits := func() map[string]unit {
		rows, err := conn.Query(ctx, `SELECT work_unit_id, ifNull((argMax(tuple(provider), computed_at)).1, 'none'), argMax(structural_evidence_json, computed_at)
FROM work_unit_investments WHERE org_id = ? GROUP BY work_unit_id`, demoWorld.OrgID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]unit{}
		for rows.Next() {
			var id, provider, evidence string
			if err := rows.Scan(&id, &provider, &evidence); err != nil {
				t.Fatal(err)
			}
			var parsed struct {
				Issues []string `json:"issues"`
			}
			if err := json.Unmarshal([]byte(evidence), &parsed); err != nil {
				t.Fatalf("unit %s evidence: %v", id, err)
			}
			out[id] = unit{provider: provider, issues: parsed.Issues}
		}
		return out
	}
	loadPoints := func() map[string]float64 {
		rows, err := conn.Query(ctx, `SELECT work_item_id, story_points FROM work_items FINAL WHERE org_id = ? AND story_points IS NOT NULL`, demoWorld.OrgID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]float64{}
		for rows.Next() {
			var id string
			var points *float64
			if err := rows.Scan(&id, &points); err != nil {
				t.Fatal(err)
			}
			out[id] = *points
		}
		return out
	}
	type sizeRow struct {
		size          float64
		sized, issues uint32
	}
	loadSizes := func() map[string]sizeRow {
		rows, err := conn.Query(ctx, `SELECT work_unit_id, argMax(issue_size, computed_at), argMax(sized_issue_count, computed_at), argMax(issue_count, computed_at)
FROM work_unit_issue_size WHERE org_id = ? GROUP BY work_unit_id`, demoWorld.OrgID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]sizeRow{}
		for rows.Next() {
			var id string
			var row sizeRow
			if err := rows.Scan(&id, &row.size, &row.sized, &row.issues); err != nil {
				t.Fatal(err)
			}
			out[id] = row
		}
		return out
	}
	// The independent computation: distinct issues, positive finite points only.
	expected := func(issues []string, points map[string]float64) sizeRow {
		var row sizeRow
		distinct := map[string]bool{}
		for _, id := range issues {
			distinct[id] = true
		}
		ids := make([]string, 0, len(distinct))
		for id := range distinct {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			row.issues++
			if p, ok := points[id]; ok && p > 0 && !math.IsInf(p, 1) {
				row.size += p
				row.sized++
			}
		}
		return row
	}
	check := func(label string) map[string]issueSizeSummary {
		units := loadUnits()
		points := loadPoints()
		sizes := loadSizes()
		if len(units) == 0 {
			t.Fatalf("%s: the materializer wrote no unit", label)
		}
		summary := map[string]issueSizeSummary{}
		for id, u := range units {
			got, ok := sizes[id]
			if !ok {
				t.Errorf("%s: unit %s has no work_unit_issue_size row", label, id)
				continue
			}
			want := expected(u.issues, points)
			if math.Abs(got.size-want.size) > 1e-9 || got.sized != want.sized || got.issues != want.issues {
				t.Errorf("%s: unit %s (%s) size row %+v, want %+v", label, id, u.provider, got, want)
			}
			entry := summary[u.provider]
			entry.Units++
			if got.size > 0 {
				entry.SizedUnits++
			}
			entry.SizeSum += got.size
			summary[u.provider] = entry
		}
		if len(sizes) != len(units) {
			t.Errorf("%s: %d size rows for %d units", label, len(sizes), len(units))
		}
		return summary
	}

	summary := check("first run")
	if first.IssueSizeRecords != first.Records+first.SkippedExisting || first.IssueSizeRecords == 0 {
		t.Errorf("first run: %d size rows for %d written + %d skipped units", first.IssueSizeRecords, first.Records, first.SkippedExisting)
	}
	for _, provider := range []string{"jira", "gitlab", "linear", "github"} {
		if summary[provider].Units == 0 {
			t.Errorf("the demo world has no %s unit; the provider matrix is not covered", provider)
		}
	}
	for _, provider := range []string{"jira", "gitlab", "linear"} {
		if summary[provider].SizedUnits == 0 {
			t.Errorf("no %s unit carries a size, but the world's %s issues carry story points", provider, provider)
		}
	}
	encoded, _ := json.Marshal(summary)
	t.Logf("demo world issue size summary: %s", encoded)
	if !reflect.DeepEqual(summary, demoWorldIssueSizeGolden) {
		t.Errorf("issue size summary = %s, golden %v", encoded, demoWorldIssueSizeGolden)
	}

	// Change the size of one sized issue, then run again with the same window: the unit's investment row is
	// unchanged and skipped, and its size row follows the new size.
	var changedItem, changedUnit string
	before := loadSizes()
	points := loadPoints()
	for id, u := range loadUnits() {
		if u.provider != "jira" {
			continue
		}
		for _, issue := range u.issues {
			if points[issue] > 0 {
				changedItem, changedUnit = issue, id
				break
			}
		}
		if changedItem != "" {
			break
		}
	}
	if changedItem == "" {
		t.Fatal("no sized jira issue to change")
	}
	if err := conn.Exec(ctx, `INSERT INTO work_items SELECT * REPLACE (story_points + 10 AS story_points, last_synced + INTERVAL 1 HOUR AS last_synced)
FROM work_items FINAL WHERE org_id = ? AND work_item_id = ?`, demoWorld.OrgID, changedItem); err != nil {
		t.Fatal(err)
	}
	cfg.RunID = "demo-world-issue-size-2"
	cfg.ComputedAt = now.Add(time.Minute)
	second, err := materializer.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if second.SkippedExisting == 0 {
		t.Fatalf("second run skipped no unit (stats %+v); the skip path is not exercised", second)
	}
	check("second run")
	var investmentAt time.Time
	if err := conn.QueryRow(ctx, `SELECT max(computed_at) FROM work_unit_investments WHERE org_id = ? AND work_unit_id = ?`, demoWorld.OrgID, changedUnit).Scan(&investmentAt); err != nil {
		t.Fatal(err)
	}
	if !investmentAt.Equal(now) {
		t.Fatalf("the changed unit's investment row was rewritten at %v; the test needs it skipped as unchanged", investmentAt)
	}
	if got, want := loadSizes()[changedUnit].size, before[changedUnit].size+10; math.Abs(got-want) > 1e-9 {
		t.Errorf("the skipped unit's size is %v after +10 points on one of its issues, want %v", got, want)
	}
}
