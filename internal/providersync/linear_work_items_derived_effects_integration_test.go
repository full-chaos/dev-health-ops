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
// dispatcher into ClickHouse migrated by newWorkItemEffectsConn; no test-local
// DDL or semantic backend stands in for the destination tables.
func TestLinearDerivedClickHouseEffectsPersistReadBackAndFenceTenants(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	now := time.Date(2026, 8, 4, 12, 34, 56, 123456000, time.UTC)
	effects := linearDerivedIntegrationEffects(t, claim, now)
	lease := &linearDerivedCountingLease{}
	sink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, lease)
	if err != nil {
		t.Fatal(err)
	}

	if len(effects) != len(linearWorkItemDerivedEffectDestinations) {
		t.Fatalf("effects=%d want=%d", len(effects), len(linearWorkItemDerivedEffectDestinations))
	}
	for _, effect := range effects {
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, effect)
		if err != nil || inspection != EffectExact {
			t.Fatalf("readback %s: inspection=%s error=%v", effect.Destination, inspection, err)
		}
	}
	if lease.calls == 0 {
		t.Fatal("dispatcher did not assert its lease around migrated ClickHouse writes")
	}

	// A row at the same natural key but another tenant must not satisfy the
	// normal claim. The wrapper accepts the foreign effect only under the
	// foreign claim, and the real adapter's org_id predicate keeps it invisible
	// to the original tenant.
	teamID := "linear-team"
	teamName := "Linear Team"
	foreignClaim := claim
	foreignClaim.OrgID = "org-other"
	foreignRow := githubWorkItemStateDurationDailyRow{
		Day: newGitHubWorkItemDerivedDay(now), Provider: "linear", WorkScopeID: "linear:team",
		TeamID: teamID, TeamName: teamName, Status: "in_progress", DurationHours: 2,
		ItemsTouched: 1, ComputedAt: now, AvgWIP: 0.5, OrgID: foreignClaim.OrgID,
	}
	foreignRaw, err := effectRowsFromValues([]githubWorkItemStateDurationDailyRow{foreignRow})
	if err != nil {
		t.Fatal(err)
	}
	foreignEffect, err := BuildEffectBatch(
		"work_item_state_durations_daily", EffectReadbackRequired, foreignRaw,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatalf("write foreign tenant row: %v", err)
	}
	foreignInspection, err := sink.InspectEffect(ctx, claim, foreignEffect)
	if !errors.Is(err, ErrInvalidConfiguration) || foreignInspection != EffectConflict {
		t.Fatalf("foreign effect under normal claim: inspection=%s error=%v", foreignInspection, err)
	}

	// Verify the actual readback fence with a same-key, normal-tenant effect
	// rather than relying only on the wrapper's row/identity validation.
	normalState := effectsByDestination(effects)["work_item_state_durations_daily"]
	if inspection, err := sink.InspectEffect(ctx, claim, normalState); err != nil || inspection != EffectExact {
		t.Fatalf("normal tenant row was displaced by foreign row: inspection=%s error=%v", inspection, err)
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
	}{{"ai_attribution", 2}, {"work_item_metrics_daily", 4}} {
		t.Run(testCase.destination, func(t *testing.T) {
			target, ok := byDestination[testCase.destination]
			if !ok {
				t.Fatalf("fixture omitted %s", testCase.destination)
			}
			// The dispatcher checks before and after its adapter. Losing the lease
			// on the post-write assertion leaves a durable row but returns an error.
			lostLease := &linearDerivedCountingLease{failAt: testCase.failAt}
			lostSink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, lostLease)
			if err != nil {
				t.Fatal(err)
			}
			if err := lostSink.WriteEffect(ctx, claim, target); !errors.Is(err, providerfoundation.ErrLeaseLost) {
				t.Fatalf("lease-loss write error=%v, want ErrLeaseLost", err)
			}

			recoveredLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
			recoveredSink, err := NewLinearWorkItemDerivedClickHouseEffects(conn, recoveredLease)
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
