//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// Throwaway repeat leg (do not merge): the body of
// TestDailyRunOfListedRepositoriesComputesASharedWorkScopeOverEveryRepository/a_repository_is_listed
// run many times, with every stored issue_type_metrics_daily version logged.

type repeatIssueTypeVersion struct {
	key    string
	at     time.Time
	values string
}

func (version repeatIssueTypeVersion) String() string {
	return fmt.Sprintf("%s@%s=%s", version.key, version.at.Format("15:04:05"), version.values)
}

func repeatIssueTypeVersions(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) []repeatIssueTypeVersion {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT ifNull(toString(repo_id), 'NULL'), toString(provider), toString(team_id), toString(issue_type_norm), computed_at,
       toString(tuple(created_count, completed_count, active_count, cycle_p50_hours, cycle_p90_hours, lead_p50_hours))
FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?
ORDER BY 1, 2, 3, 4, computed_at, 6`, orgID, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var versions []repeatIssueTypeVersion
	for rows.Next() {
		var repo, provider, team, kind, values string
		var at time.Time
		if err := rows.Scan(&repo, &provider, &team, &kind, &at, &values); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, repeatIssueTypeVersion{key: strings.Join([]string{repo, provider, team, kind}, "|"), at: at.UTC(), values: values})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return versions
}

// repeatNewVersions is after minus before, as multisets.
func repeatNewVersions(before, after []repeatIssueTypeVersion) []repeatIssueTypeVersion {
	held := map[repeatIssueTypeVersion]int{}
	for _, version := range before {
		held[version]++
	}
	var added []repeatIssueTypeVersion
	for _, version := range after {
		if held[version] > 0 {
			held[version]--
			continue
		}
		added = append(added, version)
	}
	return added
}

// repeatNullHashRows counts the newest rows of a table whose cityHash64 of
// every column but computed_at is NULL (a row the snapshot hash cannot see).
func repeatNullHashRows(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) string {
	t.Helper()
	var parts []string
	for _, source := range []struct{ table, from string }{
		{"issue_type_metrics_daily", `(SELECT * FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?
			ORDER BY computed_at DESC LIMIT 1 BY repo_id, provider, team_id, issue_type_norm)`},
		{"work_item_completion_daily", `(SELECT * FROM work_item_completion_daily WHERE org_id = ? AND day = ?
			ORDER BY computed_at DESC LIMIT 1 BY repo_id, team_id)`},
	} {
		var rows, nullRows uint64
		if err := conn.QueryRow(ctx, `SELECT count(), countIf(isNull(cityHash64(* EXCEPT (computed_at)))) FROM `+source.from, orgID, day).Scan(&rows, &nullRows); err != nil {
			t.Fatalf("null hash rows %s: %v", source.table, err)
		}
		parts = append(parts, fmt.Sprintf("%s=%d/%d", source.table, nullRows, rows))
	}
	return strings.Join(parts, " ")
}

func TestRepeatDailyRunOfAListedRepositoryIssueTypeVersions(t *testing.T) {
	const iterations = 120
	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	ran, mismatches, staleTies, mismatchWithoutTie, listedVersusFull := 0, 0, 0, 0, 0
	for iteration := 1; iteration <= iterations; iteration++ {
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < time.Minute {
			t.Logf("REPEAT-STOP at iteration %d: under one minute of the budget left", iteration)
			break
		}
		name := fmt.Sprintf("a_repository_is_listed/iteration-%03d", iteration)
		t.Run(name, func(t *testing.T) {
			ran++
			orgID, repoA, repoB := uuid.NewString(), uuid.New(), uuid.New()
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
			insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "github")
			insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
				partitionKeyItem{repo: repoA, id: "gh:acme/api#1", scope: "scope-shared"},
				partitionKeyItem{repo: repoB, id: "gh:acme/web#1", scope: "scope-shared"},
				partitionKeyItem{repo: uuid.Nil, id: "gh:none#1", scope: "scope-shared"},
				partitionKeyItem{repo: repoB, id: "gh:acme/web#2", scope: "scope-web-only"})
			rig.fullRecompute(t, ctx, orgID, day)
			first := repeatIssueTypeVersions(t, ctx, rig.conn, orgID, day)

			insertPartitionKeyItems(t, ctx, rig.conn, orgID, day,
				partitionKeyItem{repo: repoA, id: "gh:new#1", scope: "scope-shared"})
			rig.listedRecompute(t, ctx, orgID, day, repoA)
			afterListed := derivedDaySnapshot(t, ctx, rig.conn, orgID, day)
			afterListedVersions := repeatIssueTypeVersions(t, ctx, rig.conn, orgID, day)
			listedRows := repeatNewVersions(first, afterListedVersions)
			if got, want := readWorkScopeRow(t, ctx, rig.conn, orgID, "scope-shared", day), (workScopeRow{4, 3, 12, 4, 4, 0}); got != want {
				t.Errorf("shared work scope after the run of the listed repository = %+v, want %+v", got, want)
			}

			rig.fullRecompute(t, ctx, orgID, day)
			full := derivedDaySnapshot(t, ctx, rig.conn, orgID, day)
			finalRows := repeatNewVersions(afterListedVersions, repeatIssueTypeVersions(t, ctx, rig.conn, orgID, day))

			// A stale tie: a version of the listed run with the second and the
			// key of a first-run version that holds other values.
			tie := false
			for _, listed := range listedRows {
				for _, older := range first {
					if older.key == listed.key && older.at.Equal(listed.at) && older.values != listed.values {
						tie = true
					}
				}
			}
			// The listed run must write what the full recompute writes for the
			// same key; another value is a wrong answer of the listed run.
			for _, listed := range listedRows {
				for _, final := range finalRows {
					if final.key == listed.key && final.values != listed.values {
						listedVersusFull++
						t.Errorf("LISTED-VS-FULL key %s: listed run %s, full recompute %s", listed.key, listed.values, final.values)
					}
				}
			}
			mismatch := afterListed["issue_type_metrics_daily"] != full["issue_type_metrics_daily"]
			if tie {
				staleTies++
			}
			if mismatch {
				mismatches++
				if !tie {
					mismatchWithoutTie++
				}
			}
			t.Logf("CELL %s tie=%v issue_type_mismatch=%v first=%v listed=%v final=%v", name, tie, mismatch, first, listedRows, finalRows)
			if iteration == 1 {
				t.Logf("NULL-HASH-ROWS %s", repeatNullHashRows(t, ctx, rig.conn, orgID, day))
			}
			if !reflect.DeepEqual(afterListed, full) {
				t.Errorf("after the run of the listed repository\n got %v\nfull recompute %v", afterListed, full)
			}
		})
	}
	t.Logf("REPEAT-SUMMARY N=%d ran=%d issue_type_mismatch=%d stale_tie=%d mismatch_without_tie=%d listed_vs_full=%d",
		iterations, ran, mismatches, staleTies, mismatchWithoutTie, listedVersusFull)
}
