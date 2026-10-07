//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This test deliberately builds effect batches from the typed rows that the
// derived builders persist. It then sends those batches through the Linear
// sync sink into ClickHouse migrated by newWorkItemEffectsConn; no test-local
// DDL or semantic backend stands in for the destination tables. The sink
// writes and reads back ai_attribution, fences it by tenant, and refuses each
// of the nine tables the daily job computes from stored rows.
func TestLinearDerivedClickHouseEffectsPersistReadBackAndFenceTenants(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	now := time.Date(2026, 8, 4, 12, 34, 56, 123456000, time.UTC)
	effects := linearDerivedIntegrationEffects(t, claim, now)
	lease := &linearDerivedCountingLease{}
	sink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, lease, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(effects) != len(linearWorkItemDerivedEffectDestinations) || len(effects) != 10 {
		t.Fatalf("effects=%d want=%d", len(effects), len(linearWorkItemDerivedEffectDestinations))
	}
	refused := 0
	for _, effect := range effects {
		if effect.Destination == "ai_attribution" {
			continue
		}
		refused++
		t.Run("refuses "+effect.Destination, func(t *testing.T) {
			assertSyncSinkRefusesDailyJobTable(t, ctx, conn, sink, claim, effect)
		})
	}
	if refused != 9 {
		t.Fatalf("refused effects=%d want one for each of the nine tables of the daily job", refused)
	}
	if lease.calls != 0 {
		t.Fatalf("lease assertions=%d: a refused effect must stop before the lease and the store", lease.calls)
	}

	aiEffect, ok := effectsByDestination(effects)["ai_attribution"]
	if !ok || len(aiEffect.Rows) != 1 {
		t.Fatalf("fixture ai_attribution effect present=%v rows=%d want one row", ok, len(aiEffect.Rows))
	}
	if inspection, err := sink.InspectEffect(ctx, claim, aiEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("before write: inspection=%s error=%v", inspection, err)
	}
	callsBefore := lease.calls
	if err := sink.WriteEffect(ctx, claim, aiEffect); err != nil {
		t.Fatalf("write ai_attribution: %v", err)
	}
	if lease.calls == callsBefore {
		t.Fatal("dispatcher did not assert its lease around the migrated ClickHouse write")
	}
	if inspection, err := sink.InspectEffect(ctx, claim, aiEffect); err != nil || inspection != EffectExact {
		t.Fatalf("readback ai_attribution: inspection=%s error=%v", inspection, err)
	}

	// A row at the same natural key but another tenant must not satisfy the
	// normal claim. The sink accepts the foreign effect only under the foreign
	// claim, and the adapter's org_id predicate keeps it invisible to the
	// original tenant.
	foreignClaim := claim
	foreignClaim.OrgID = "88888888-8888-4888-8888-888888888888"
	foreignEffect, ok := effectsByDestination(linearDerivedIntegrationEffects(t, foreignClaim, now))["ai_attribution"]
	if !ok {
		t.Fatal("foreign fixture omitted ai_attribution")
	}
	if err := sink.WriteEffect(ctx, claim, foreignEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign rows under the normal claim: write error=%v", err)
	}
	if err := sink.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatalf("write foreign tenant row: %v", err)
	}
	foreignInspection, err := sink.InspectEffect(ctx, claim, foreignEffect)
	if !errors.Is(err, ErrInvalidConfiguration) || foreignInspection != EffectConflict {
		t.Fatalf("foreign effect under normal claim: inspection=%s error=%v", foreignInspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, foreignClaim, foreignEffect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign tenant readback under its own claim: inspection=%s error=%v", inspection, err)
	}

	// Verify the actual readback fence with the same-key, normal-tenant effect
	// rather than relying only on the wrapper's row/identity validation.
	if inspection, err := sink.InspectEffect(ctx, claim, aiEffect); err != nil || inspection != EffectExact {
		t.Fatalf("normal tenant row was displaced by foreign row: inspection=%s error=%v", inspection, err)
	}
	var perTenant []uint64
	for _, orgID := range []string{claim.OrgID, foreignClaim.OrgID} {
		var stored uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM ai_attribution FINAL WHERE org_id = ?", orgID).Scan(&stored); err != nil {
			t.Fatalf("count ai_attribution rows: %v", err)
		}
		perTenant = append(perTenant, stored)
	}
	if perTenant[0] != 1 || perTenant[1] != 1 {
		t.Fatalf("stored ai_attribution rows per tenant=%v want one each", perTenant)
	}
}

func TestLinearDerivedClickHouseEffectsRecoverAfterLeaseLoss(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	now := time.Date(2026, 8, 5, 12, 34, 56, 123456000, time.UTC)
	effects := linearDerivedIntegrationEffects(t, claim, now)
	byDestination := effectsByDestination(effects)
	for _, testCase := range []struct {
		destination string
		failAt      int
	}{{"ai_attribution", 2}} {
		t.Run(testCase.destination, func(t *testing.T) {
			target, ok := byDestination[testCase.destination]
			if !ok {
				t.Fatalf("fixture omitted %s", testCase.destination)
			}
			// The dispatcher checks before and after its adapter. Losing the lease
			// on the post-write assertion leaves a durable row but returns an error.
			lostLease := &linearDerivedCountingLease{failAt: testCase.failAt}
			lostSink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, lostLease, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := lostSink.WriteEffect(ctx, claim, target); !errors.Is(err, providerfoundation.ErrLeaseLost) {
				t.Fatalf("lease-loss write error=%v, want ErrLeaseLost", err)
			}

			recoveredLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
			recoveredSink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, recoveredLease, nil)
			if err != nil {
				t.Fatal(err)
			}
			inspection, err := recoveredSink.InspectEffect(ctx, claim, target)
			if err != nil || inspection != EffectExact {
				t.Fatalf("recovery readback: inspection=%s error=%v", inspection, err)
			}
			if err := recoveredSink.WriteEffect(ctx, claim, target); err != nil {
				t.Fatalf("idempotent recovery replay: %v", err)
			}
			if inspection, err := recoveredSink.InspectEffect(ctx, claim, target); err != nil || inspection != EffectExact {
				t.Fatalf("post-replay readback: inspection=%s error=%v", inspection, err)
			}
		})
	}
}

func linearDerivedIntegrationEffects(t *testing.T, claim Claim, now time.Time) []EffectBatch {
	t.Helper()
	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	day := newGitHubWorkItemDerivedDay(now)
	metricDay := newGitHubWorkItemMetricDay(now)
	teamID, teamName := "linear-team", "Linear Team"
	area, rule := "feature_delivery", "linear_rule"
	ratio := 0.5
	assignee := "linear@example.com"
	startedAt := now.Add(-2 * time.Hour)
	completedAt := now

	estimate := githubEstimateCoverageMetricsDailyRow{
		Day: day, Provider: "linear", WorkScopeID: "linear:team", TeamID: &teamID,
		TeamName: &teamName, EstimatedCount: 1, UnestimatedCount: 1, BacklogSize: 2,
		Ratio: &ratio, ComputedAt: now, OrgID: claim.OrgID,
	}
	teamAttribution := githubWorkItemTeamAttributionRow{
		WorkItemID: "linear:ENG-100", Provider: "linear", Source: "native_team",
		IsPrimary: 1, Confidence: "high", Evidence: "linear team key",
		ComputedAt: now, RepoID: &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: claim.OrgID,
	}
	stateDuration := githubWorkItemStateDurationDailyRow{
		Day: day, Provider: "linear", WorkScopeID: "linear:team", TeamID: teamID,
		TeamName: teamName, Status: "in_progress", DurationHours: 2, ItemsTouched: 1,
		ComputedAt: now, AvgWIP: 0.5, OrgID: claim.OrgID,
	}
	metrics := githubWorkItemMetricTestGroupRow()
	metrics.Day, metrics.Provider, metrics.OrgID, metrics.WorkScopeID = metricDay, "linear", claim.OrgID, "linear:team"
	metrics.TeamID, metrics.TeamName, metrics.ComputedAt = teamID, teamName, now
	userMetrics := githubWorkItemMetricTestUserRow()
	userMetrics.Day, userMetrics.Provider, userMetrics.OrgID, userMetrics.WorkScopeID = metricDay, "linear", claim.OrgID, "linear:team"
	userMetrics.TeamID, userMetrics.TeamName, userMetrics.ComputedAt = teamID, teamName, now
	cycle := githubWorkItemMetricTestCycleRow()
	cycle.Day, cycle.Provider, cycle.OrgID, cycle.WorkScopeID = metricDay, "linear", claim.OrgID, "linear:team"
	cycle.WorkItemID, cycle.TeamID, cycle.TeamName, cycle.ComputedAt = "linear:ENG-100", teamID, teamName, now
	cycle.Assignee, cycle.StartedAt, cycle.CompletedAt = &assignee, &startedAt, &completedAt

	issueType := githubWorkItemEngineEffectIssueRow()
	issueType.Day, issueType.Provider, issueType.OrgID, issueType.RepoID = day, "linear", claim.OrgID, &repoID
	issueType.ComputedAt = now
	classification := githubWorkItemEngineEffectClassificationRow()
	classification.Day, classification.Provider, classification.OrgID, classification.RepoID = day, "linear", claim.OrgID, &repoID
	classification.ComputedAt, classification.InvestmentArea, classification.RuleID = now, &area, &rule
	investment := githubWorkItemEngineEffectMetricsRow()
	investment.Day, investment.OrgID, investment.RepoID = day, claim.OrgID, &repoID
	investment.ComputedAt, investment.InvestmentArea = now, &area

	rows := LinearWorkItemDerivedEffectRows{}
	rows.AIAttributions = []LinearAIAttributionRow{{
		RecordID: uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		OrgID:    uuid.MustParse(claim.OrgID), Provider: "linear", SubjectType: "issue",
		SubjectID: "linear:ENG-100", RepoID: nil, Kind: "ai_assisted",
		Source: "issue_label", Confidence: 0.95, Evidence: map[string]any{"label": "codex"},
		ObservedAt: now.Add(-time.Hour), IngestedAt: now,
	}}
	rows.EstimateCoverageMetricsDaily = []LinearEstimateCoverageMetricsDailyRow{estimate}
	rows.WorkItemTeamAttributions = []LinearWorkItemTeamAttributionRow{teamAttribution}
	rows.WorkItemStateDurationsDaily = []LinearWorkItemStateDurationDailyRow{stateDuration}
	rows.WorkItemMetricsDaily = []LinearWorkItemMetricsDailyRow{metrics}
	rows.WorkItemUserMetricsDaily = []LinearWorkItemUserMetricsDailyRow{userMetrics}
	rows.WorkItemCycleTimes = []LinearWorkItemCycleTimePersistenceRow{cycle}
	rows.IssueTypeMetricsDaily = []LinearIssueTypeMetricsDailyRow{issueType}
	rows.InvestmentClassificationsDaily = []LinearInvestmentClassificationDailyRow{classification}
	rows.InvestmentMetricsDaily = []LinearInvestmentMetricsDailyRow{investment}
	effects, err := BuildLinearWorkItemDerivedEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	return effects
}

func effectsByDestination(effects []EffectBatch) map[string]EffectBatch {
	result := make(map[string]EffectBatch, len(effects))
	for _, effect := range effects {
		result[effect.Destination] = effect
	}
	return result
}

type linearDerivedCountingLease struct {
	calls  int
	failAt int
}

func (lease *linearDerivedCountingLease) Assert(context.Context) error {
	lease.calls++
	if lease.failAt > 0 && lease.calls >= lease.failAt {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}
