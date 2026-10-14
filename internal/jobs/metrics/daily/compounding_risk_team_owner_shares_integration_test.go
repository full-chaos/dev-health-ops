//go:build integration

package daily

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9620, D5998, against a real store. The owner's share is the WEIGHT of a
// repository in the team's aggregate:
//
//  1. team-t owns repo-a whole (share 1.0) and repo-b at 0.3 (repo-m is its main owner at 0.7):
//     its inputs are the hand-computed share-weighted means, its score is the
//     same formula on them, its counts are the sums of count x share;
//  2. team-m owns repo-b alone at 0.7: the repository's own intensive values,
//     0.7 of its counts (a share never scales an intensity);
//  3. team-s owns repo-c whole: unchanged.
func TestCompoundingRiskTeamRowsWeightRepositoriesByShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, statement := range compoundingRiskTeamSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org   = "00000000-0000-4000-8000-0000000000e0"
		repoA = "00000000-0000-4000-8000-0000000000e1"
		repoB = "00000000-0000-4000-8000-0000000000e2"
		repoC = "00000000-0000-4000-8000-0000000000e3"
	)
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	own := func(team, repo, name string, primary int, share float64) string {
		return "('" + org + "', 'native', '" + team + "', toUUID('" + repo + "'), '" + name + "', 'exact', 'inferred', " +
			map[int]string{1: "1", 2: "2"}[primary] + ", 0, 0, " + formatShare(share) + ", '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00')"
	}
	for _, statement := range []string{
		`INSERT INTO repos (id, repo, created_at, org_id, provider, last_synced) VALUES
(toUUID('` + repoA + `'), 'acme/a', now(), '` + org + `', 'native', now()),
(toUUID('` + repoB + `'), 'acme/b', now(), '` + org + `', 'native', now()),
(toUUID('` + repoC + `'), 'acme/c', now(), '` + org + `', 'native', now())`,
		`INSERT INTO teams (id, team_uuid, name, org_id, updated_at) VALUES
('team-t', generateUUIDv4(), 'T', '` + org + `', now64(6)),
('team-m', generateUUIDv4(), 'M', '` + org + `', now64(6)),
('team-s', generateUUIDv4(), 'S', '` + org + `', now64(6))`,
		`INSERT INTO team_repo_ownership
(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at) VALUES
` + own("team-t", repoA, "acme/a", 1, 1.0) + `,
` + own("team-m", repoB, "acme/b", 1, 0.7) + `,
` + own("team-t", repoB, "acme/b", 2, 0.3) + `,
` + own("team-s", repoC, "acme/c", 1, 1.0),
		`INSERT INTO repo_metrics_daily
(repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id) VALUES
(toUUID('` + repoA + `'), '2026-08-24', 0.2, 0.3, 0.4, 2, 10.0, '2026-08-24 09:00:00', '` + org + `'),
(toUUID('` + repoB + `'), '2026-08-24', 0.6, 0.5, 0.8, 4, 10.0, '2026-08-24 09:00:00', '` + org + `'),
(toUUID('` + repoC + `'), '2026-08-24', 0.1, 0.2, 0.3, 1, 10.0, '2026-08-24 09:00:00', '` + org + `')`,
		// repo-a: 4 merged, 2 reviewed; repo-b: 10 merged, 5 reviewed; repo-c: none.
		`INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, first_review_at, last_synced, org_id)
SELECT toUUID('` + repoA + `'), number, toDateTime64('2026-08-10 00:00:00', 3, 'UTC'), toDateTime64('2026-08-11 00:00:00', 3, 'UTC'),
       if(number <= 2, toDateTime64('2026-08-10 10:00:00', 3, 'UTC'), NULL), toDateTime64('2026-08-11 01:00:00', 3, 'UTC'), '` + org + `'
FROM numbers(1, 4)`,
		`INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, first_review_at, last_synced, org_id)
SELECT toUUID('` + repoB + `'), number, toDateTime64('2026-08-12 00:00:00', 3, 'UTC'), toDateTime64('2026-08-13 00:00:00', 3, 'UTC'),
       if(number <= 5, toDateTime64('2026-08-12 06:00:00', 3, 'UTC'), NULL), toDateTime64('2026-08-13 01:00:00', 3, 'UTC'), '` + org + `'
FROM numbers(1, 10)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	executor, err := NewCompoundingRiskTeamExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	if written, err := executor.ComputeFinalizeFamily(ctx, Run{ID: "run-shares", OrganizationID: org, TargetDay: targetDay}); err != nil || written != 3 {
		t.Fatalf("wrote %d team rows, err %v, want one per team (t, m, s)", written, err)
	}

	type teamRow struct {
		score, churn, ownershipGini, single, latency, bus *float64
		merged, reviewed, share                           *float64
		severity                                          string
	}
	rows, err := conn.Query(ctx, `
SELECT scope_id, compounding_risk, rework_churn, ownership_gini, single_owner_ratio, review_latency_p90h, bus_factor,
       merged_prs_30d, reviewed_prs_30d, review_share_30d, toString(severity)
FROM compounding_risk_daily WHERE org_id = ? AND day = ? AND scope = 'team' ORDER BY scope_id`, org, targetDay.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]teamRow{}
	for rows.Next() {
		var scopeID string
		var row teamRow
		if err := rows.Scan(&scopeID, &row.score, &row.churn, &row.ownershipGini, &row.single, &row.latency, &row.bus,
			&row.merged, &row.reviewed, &row.share, &row.severity); err != nil {
			t.Fatal(err)
		}
		got[scopeID] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	near := func(value *float64, want float64) bool { return value != nil && math.Abs(*value-want) < 1e-9 }

	// 1. team-t: repo-a at 1.0, repo-b at 0.3.
	tRow := got["team-t"]
	if !near(tRow.churn, (0.2+0.6*0.3)/1.3) || !near(tRow.ownershipGini, (0.4+0.8*0.3)/1.3) || !near(tRow.single, (0.3+0.5*0.3)/1.3) {
		t.Errorf("team-t weighted inputs churn %v gini %v single %v, want %v %v %v", tRow.churn, tRow.ownershipGini, tRow.single,
			(0.2+0.6*0.3)/1.3, (0.4+0.8*0.3)/1.3, (0.3+0.5*0.3)/1.3)
	}
	if !near(tRow.merged, 4+10*0.3) || !near(tRow.reviewed, 2+5*0.3) || !near(tRow.share, (2+5*0.3)/(4+10*0.3)) {
		t.Errorf("team-t counts %v/%v share %v, want %v/%v", tRow.merged, tRow.reviewed, tRow.share, 4+10*0.3, 2+5*0.3)
	}
	// The score is the same formula on the persisted (weighted) inputs.
	recomputed := compoundingrisk.Compute(targetDay, "team-t", org, compoundingrisk.Inputs{
		ReworkChurn: tRow.churn, SingleOwnerRatio: tRow.single, OwnershipGini: tRow.ownershipGini, ReviewLatencyP90H: tRow.latency,
	}, time.Now(), compoundingrisk.DefaultWeights, compoundingrisk.DefaultThresholds, compoundingrisk.DefaultReferences)
	if tRow.score == nil || recomputed.CompoundingRisk == nil || !near(tRow.score, *recomputed.CompoundingRisk) || tRow.severity != recomputed.Severity {
		t.Errorf("team-t score %v severity %q, want the formula on the weighted inputs: %v %q", tRow.score, tRow.severity, recomputed.CompoundingRisk, recomputed.Severity)
	}

	// 2. team-m: repo-b alone at 0.7 -- the repository's own intensive values, 0.7 of its counts.
	mRow := got["team-m"]
	if !near(mRow.churn, 0.6) || !near(mRow.ownershipGini, 0.8) || !near(mRow.single, 0.5) {
		t.Errorf("team-m inputs churn %v gini %v single %v, want the repository's own 0.6 0.8 0.5", mRow.churn, mRow.ownershipGini, mRow.single)
	}
	if !near(mRow.merged, 7) || !near(mRow.reviewed, 3.5) || !near(mRow.share, 0.5) {
		t.Errorf("team-m counts %v/%v share %v, want 7/3.5 and 0.5 (0.7 of 10 and 5)", mRow.merged, mRow.reviewed, mRow.share)
	}
	repoBRecord := compoundingrisk.Compute(targetDay, "repo-b", org, compoundingrisk.Inputs{
		ReworkChurn: mRow.churn, SingleOwnerRatio: mRow.single, OwnershipGini: mRow.ownershipGini, ReviewLatencyP90H: mRow.latency,
	}, time.Now(), compoundingrisk.DefaultWeights, compoundingrisk.DefaultThresholds, compoundingrisk.DefaultReferences)
	if mRow.score == nil || repoBRecord.CompoundingRisk == nil || !near(mRow.score, *repoBRecord.CompoundingRisk) {
		t.Errorf("team-m score %v, want the repository's own %v (a share never scales a score)", mRow.score, repoBRecord.CompoundingRisk)
	}

	// 3. team-s: sole owner, unchanged.
	sRow := got["team-s"]
	if !near(sRow.churn, 0.1) || !near(sRow.ownershipGini, 0.3) || !near(sRow.single, 0.2) || !near(sRow.merged, 0) {
		t.Errorf("team-s inputs churn %v gini %v single %v merged %v, want the repository's own 0.1 0.3 0.2 and 0", sRow.churn, sRow.ownershipGini, sRow.single, sRow.merged)
	}
}

func formatShare(share float64) string {
	return map[float64]string{1.0: "1.0", 0.7: "0.7", 0.3: "0.3"}[share]
}
