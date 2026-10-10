//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"
)

// The read-back of one key of work_item_user_metrics_daily answers ABSENT when
// the newest version of the key is a retraction row (CHAOS-9135): the writer
// of the effect has to write it again. A retraction row read as a stored row
// would be a conflict (ErrEffectRecoveryAmbiguous) that no retry resolves. A
// newer MEASURED row after the retraction is read again as the key's row.
func TestGitHubWorkItemUserMetricsReadbackTreatsANewestRetractionAsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn := githubWorkItemMetricIntegrationConn(t, ctx)
	sink := GitHubWorkItemUserMetricsDailyClickHouseEffects{Conn: conn, Lease: githubWorkItemMetricIntegrationLease()}
	row := githubWorkItemMetricTestUserRow()
	identity, effect := githubWorkItemMetricTestIdentity(t,
		githubWorkItemUserMetricsDailyDestination, []githubWorkItemUserMetricsDailyRow{row})
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectExact, "the written row was not recognized")

	day, err := row.Day.time()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
 (day, provider, work_scope_id, user_identity, team_id, team_name, computed_at, org_id, retracted)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)`,
		day, row.Provider, row.WorkScopeID, row.UserIdentity, row.TeamID, row.TeamName, row.ComputedAt.Add(time.Hour), row.OrgID); err != nil {
		t.Fatal(err)
	}
	assertGitHubWorkItemMetricInspection(t, ctx, sink, identity, effect, EffectAbsent,
		"a key whose newest version is a retraction row was read as a stored row")

	// The effect written again, newer than the retraction, is the key's row.
	again := row
	again.ComputedAt = row.ComputedAt.Add(2 * time.Hour)
	againIdentity, againEffect := githubWorkItemMetricTestIdentity(t,
		githubWorkItemUserMetricsDailyDestination, []githubWorkItemUserMetricsDailyRow{again})
	if err := sink.WriteGitHubWorkItemEffect(ctx, againIdentity, againEffect); err != nil {
		t.Fatal(err)
	}
	assertGitHubWorkItemMetricInspection(t, ctx, sink, againIdentity, againEffect, EffectExact,
		"a measured row written after the retraction was not read as the key's row")
}
