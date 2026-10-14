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

// CHAOS-9620, D5988/D5994, against a real store: one repository with a main
// owner (share 0.5) and two lesser owners (0.3, 0.2) gives three team rows. The
// score columns of each are the repository's score times its share, so the
// three sum to the repository's score; the raw inputs and the severity are the
// repository's own.
func TestCompoundingRiskTeamRowsOfOneRepositorySumToItsScore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
		org  = "00000000-0000-4000-8000-0000000000e0"
		repo = "00000000-0000-4000-8000-0000000000e1"
	)
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		`INSERT INTO repos (id, repo, created_at, org_id, provider, last_synced) VALUES (toUUID('` + repo + `'), 'acme/shared', now(), '` + org + `', 'native', now())`,
		`INSERT INTO teams (id, team_uuid, name, org_id, updated_at) VALUES
('main', generateUUIDv4(), 'Main', '` + org + `', now64(6)),
('lesser-1', generateUUIDv4(), 'Lesser 1', '` + org + `', now64(6)),
('lesser-2', generateUUIDv4(), 'Lesser 2', '` + org + `', now64(6))`,
		`INSERT INTO team_repo_ownership
(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at) VALUES
('` + org + `', 'native', 'main',     toUUID('` + repo + `'), 'acme/shared', 'exact', 'inferred', 1, 0, 0, 0.5, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00'),
('` + org + `', 'native', 'lesser-1', toUUID('` + repo + `'), 'acme/shared', 'exact', 'inferred', 2, 0, 0, 0.3, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00'),
('` + org + `', 'native', 'lesser-2', toUUID('` + repo + `'), 'acme/shared', 'exact', 'inferred', 2, 0, 0, 0.2, '2026-01-01 00:00:00', NULL, '2026-01-01 00:00:00')`,
		`INSERT INTO repo_metrics_daily
(repo_id, day, rework_churn_ratio_30d, single_owner_file_ratio_30d, code_ownership_gini, bus_factor, pr_first_review_p90_hours, computed_at, org_id) VALUES
(toUUID('` + repo + `'), '2026-08-24', 0.4, 0.6, 0.5, 1, 10.0, '2026-08-24 09:00:00', '` + org + `')`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	executor, err := NewCompoundingRiskTeamExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	written, err := executor.ComputeFinalizeFamily(ctx, Run{ID: "run-shares", OrganizationID: org, TargetDay: targetDay})
	if err != nil {
		t.Fatal(err)
	}
	if written != 3 {
		t.Fatalf("wrote %d team rows, want one per owner (main, lesser-1, lesser-2)", written)
	}

	churn, single, gini, bus := 0.4, 0.6, 0.5, 1.0
	repoRecord := compoundingrisk.Compute(
		targetDay, repo, org,
		compoundingrisk.Inputs{ReworkChurn: &churn, SingleOwnerRatio: &single, OwnershipGini: &gini, BusFactor: &bus},
		time.Now(), compoundingrisk.DefaultWeights, compoundingrisk.DefaultThresholds, compoundingrisk.DefaultReferences,
	)
	if repoRecord.CompoundingRisk == nil {
		t.Fatal("the repository has no score; the case is not set")
	}

	rows, err := conn.Query(ctx, `
SELECT scope_id, compounding_risk, severity, rework_churn FROM compounding_risk_daily
WHERE org_id = ? AND day = ? AND scope = 'team' ORDER BY scope_id`, org, targetDay.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	shares := map[string]float64{"main": 0.5, "lesser-1": 0.3, "lesser-2": 0.2}
	sum, seen := 0.0, 0
	for rows.Next() {
		var scopeID, severity string
		var score, rework *float64
		if err := rows.Scan(&scopeID, &score, &severity, &rework); err != nil {
			t.Fatal(err)
		}
		share, ok := shares[scopeID]
		if !ok || score == nil || rework == nil {
			t.Fatalf("unexpected row %s score=%v rework=%v", scopeID, score, rework)
		}
		if want := *repoRecord.CompoundingRisk * share; math.Abs(*score-want) > 1e-9 {
			t.Errorf("%s compounding_risk = %v, want the repository's %v x %v = %v", scopeID, *score, *repoRecord.CompoundingRisk, share, want)
		}
		if *rework != churn || severity != repoRecord.Severity {
			t.Errorf("%s rework_churn = %v severity = %q, want the repository's own %v %q", scopeID, *rework, severity, churn, repoRecord.Severity)
		}
		sum += *score
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 3 || math.Abs(sum-*repoRecord.CompoundingRisk) > 1e-9 {
		t.Errorf("%d rows summing to %v, want 3 rows summing to the repository's %v", seen, sum, *repoRecord.CompoundingRisk)
	}
}
