//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func capabilityRetireClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func seedCapabilityRow(t *testing.T, ctx context.Context, conn driver.Conn, org, provider, measure string, tracked uint8, items uint32, windowEnd, computedAt time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO work_item_measure_capability
    (org_id, provider, measure, tracked, evidence_count, item_count, window_start, window_end, computed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		org, provider, measure, tracked, uint32(tracked), items, windowEnd.AddDate(0, 0, -89), windowEnd, computedAt); err != nil {
		t.Fatalf("seed capability %s %s: %v", provider, measure, err)
	}
}

func seedCapabilityWorkItem(t *testing.T, ctx context.Context, conn driver.Conn, org, provider, id string, createdAt time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO work_items
    (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, native_team_key, project_name,
     assignees, reporter, created_at, updated_at, labels, story_points, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id)
    VALUES (?, ?, ?, 'item', 'bug', 'in_progress', 'In Progress', 'P', 'p-1', '', 'Project', [], '', ?, ?, [], 2, '', '', '', '', '', ?, ?)`,
		uuid.Nil, id, provider, createdAt, createdAt, createdAt, org); err != nil {
		t.Fatalf("seed work item %s: %v", id, err)
	}
}

type capabilityAnswer struct {
	tracked, evidence, items uint32
}

// capabilityAnswers is the exact-day read of the reader contract over FINAL:
// the newest row of each (provider, measure) key with window_end = day.
func capabilityAnswers(t *testing.T, ctx context.Context, conn driver.Conn, org string, day time.Time) map[string]capabilityAnswer {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT provider, measure, toUInt32(tracked), evidence_count, item_count
FROM work_item_measure_capability FINAL WHERE org_id = ? AND window_end = ? ORDER BY provider, measure`, org, day)
	if err != nil {
		t.Fatalf("read capability: %v", err)
	}
	defer rows.Close()
	answers := map[string]capabilityAnswer{}
	for rows.Next() {
		var provider, measure string
		var answer capabilityAnswer
		if err := rows.Scan(&provider, &measure, &answer.tracked, &answer.evidence, &answer.items); err != nil {
			t.Fatalf("scan capability: %v", err)
		}
		answers[provider+"/"+measure] = answer
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate capability: %v", err)
	}
	return answers
}

// A provider whose items left the window keeps no answer for the day: the
// compute of a run (one with repositories) writes the answer of the providers
// it reads and retires the keys of the day it no longer produces. Other
// organizations and other window ends are not touched, and a second compute
// writes no retirement row.
func TestWorkItemMeasureCapabilityRetiresAKeyWhoseSourceIsGone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := capabilityRetireClickHouse(t, ctx)
	day := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, -1)
	org := "00000000-0000-4000-8000-000000140521"
	other := "00000000-0000-4000-8000-000000140522"
	stored := day.Add(30 * time.Hour)
	for _, seed := range []struct {
		org string
		day time.Time
	}{{org, day}, {org, before}, {other, day}} {
		for _, provider := range []string{"github", "jira"} {
			seedCapabilityRow(t, ctx, conn, seed.org, provider, "bug_completed_ratio", 1, 4, seed.day, stored)
			seedCapabilityRow(t, ctx, conn, seed.org, provider, "story_points_completed", 1, 4, seed.day, stored)
		}
	}
	seedCapabilityWorkItem(t, ctx, conn, org, "github", "gh-1", day.Add(-48*time.Hour))
	controls := map[string]map[string]capabilityAnswer{
		"before": capabilityAnswers(t, ctx, conn, org, before),
		"other":  capabilityAnswers(t, ctx, conn, other, day),
	}

	executor, err := NewWorkItemMeasureCapabilityExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	executor.nowUTC = func() time.Time { return day.Add(40 * time.Hour) }
	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}
	written, err := executor.ComputeFinalizeFamily(ctx, run)
	answers := capabilityAnswers(t, ctx, conn, org, day)
	t.Logf("first compute: written=%d err=%v answers=%v", written, err, answers)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	for _, measure := range []string{"bug_completed_ratio", "story_points_completed"} {
		if got := answers["github/"+measure]; got.items != 1 || got.tracked != 1 {
			t.Errorf("github/%s = %+v, want the computed answer of the one item (tracked 1, item_count 1)", measure, got)
		}
		if got := answers["jira/"+measure]; got != (capabilityAnswer{}) {
			t.Errorf("jira/%s = %+v, want a retired key (tracked 0, evidence 0, item_count 0): its source is gone", measure, got)
		}
	}
	for name, org := range map[string]string{"before": org, "other": other} {
		controlDay := day
		if name == "before" {
			controlDay = before
		}
		got := capabilityAnswers(t, ctx, conn, org, controlDay)
		for key, want := range controls[name] {
			if got[key] != want {
				t.Errorf("control %s %s = %+v, want the stored %+v", name, key, got[key], want)
			}
		}
	}

	executor.nowUTC = func() time.Time { return day.Add(41 * time.Hour) }
	again, err := executor.ComputeFinalizeFamily(ctx, run)
	t.Logf("second compute: written=%d err=%v", again, err)
	if err != nil || again != 2 {
		t.Errorf("second compute: written=%d err=%v, want the 2 computed github rows and no retirement row", again, err)
	}
}
