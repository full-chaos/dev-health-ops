//go:build integration

package workgraph

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9227 confirm round 143: a row `dho backfill normalize-investment` writes (categorization_status
// normalized_legacy) is an estimate with no work-graph membership; it is not evidence that the projection
// "should have run", so it must not make a Work Graph answer say MEMBERSHIP_NOT_MATERIALIZED. A canonical
// unit still does.
func TestNormalizedLegacyRowsDoNotDeclareMembershipNotMaterialized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	now := time.Now().UTC()
	unit := func(org, id, status string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, NULL, 'churn_loc', 1, ?, ?, ?, 0.5, 'low', ?, 'v', 'r', ?, ?)`,
			id, now.Add(-time.Hour), now, map[string]float64{"quality": 1.0}, map[string]float64{"quality.bugfix": 1.0}, `{"issues":[],"prs":[]}`, status, now, org); err != nil {
			t.Fatal(err)
		}
	}
	reason := func(org string) string {
		got := detectMembershipDegradedReason(ctx, client, org, newFilterScope(nil, nil))
		if got == nil {
			return "<nil>"
		}
		return *got
	}
	const empty, onlyNormalized, canonical = "org-9227-empty", "org-9227-only-normalized", "org-9227-canonical"
	unit(onlyNormalized, "normalized_legacy:o:jira:A-1", "normalized_legacy")
	unit(canonical, "wu-1", "ok")
	if got := reason(empty); got != "<nil>" {
		t.Errorf("an org with no investment rows: %s", got)
	}
	if got := reason(onlyNormalized); got != "<nil>" {
		t.Errorf("an org whose only rows are normalized estimates: %s, want none (an estimate is no evidence that the membership projection is missing)", got)
	}
	if got := reason(canonical); got != MembershipNotMaterialized {
		t.Errorf("an org with a canonical unit and no completed membership run: %s, want %s", got, MembershipNotMaterialized)
	}
}
