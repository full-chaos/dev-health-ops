//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// This suite applies the production ClickHouse migration chain through
// githubDerivedIntegrationConn and then exercises the Jira context loader and
// every derived adapter. It authors no local DDL. The evaluated-empty AI
// effect is the sole no-I/O destination and must remain EffectAbsent before
// and after its write call.
func TestJiraWorkItemDerivedEffectsWriteReadbackAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewJiraWorkItemDerivedClickHouseEffects(conn, lease)
	if err != nil {
		t.Fatal(err)
	}

	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = "jira-derived-integration"
	now := time.Date(2026, 8, 11, 15, 30, 0, 123000000, time.UTC)
	contextSource := jiraWorkItemClickHouseDerivationContextSource{
		delegate: githubWorkItemClickHouseDerivationContextSource{Conn: conn, Lease: lease},
	}
	facts, err := contextSource.Load(ctx, claim, githubWorkItemDerivationLoadRequest{
		AsOf: now, DonorWorkItemIDs: []string{"jira:OPS-missing"},
	})
	if err != nil {
		t.Fatalf("real migrated context load: %v", err)
	}
	if len(facts.Teams)+len(facts.Projects)+len(facts.Repos)+len(facts.Members)+
		len(facts.ManualFallbacks)+len(facts.DonorItems) != 0 {
		t.Fatalf("fresh migrated context was not empty: %+v", facts)
	}

	day := newGitHubWorkItemDerivedDay(now)
	metricDay := newGitHubWorkItemMetricDay(now)
	repoID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	teamID, teamName := "team-jira", "Jira Team"
	area, rule := "security", "sec_general"
	assignee := "jira:accountid:dev"
	ratio := 0.5

	estimate := JiraEstimateCoverageMetricsDailyRow{
		Day: day, Provider: "jira", WorkScopeID: "OPS", TeamID: &teamID,
		TeamName: &teamName, EstimatedCount: 1, UnestimatedCount: 1, BacklogSize: 2,
		Ratio: &ratio, ComputedAt: now, OrgID: claim.OrgID,
	}
	classification := JiraInvestmentClassificationDailyRow{
		RepoID: &repoID, Day: day, ArtifactType: "work_item", ArtifactID: "jira:OPS-1",
		Provider: "jira", InvestmentArea: &area, ProjectStream: "general", Confidence: 1,
		RuleID: &rule, ComputedAt: now, OrgID: claim.OrgID,
	}
	investment := JiraInvestmentMetricsDailyRow{
		RepoID: &repoID, Day: day, TeamID: teamID, InvestmentArea: &area,
		ProjectStream: "general", DeliveryUnits: 3, WorkItemsCompleted: 2, PRsMerged: 0,
		ChurnLOC: 0, CycleP50Hours: 5, ComputedAt: now, OrgID: claim.OrgID,
	}
	issueType := JiraIssueTypeMetricsDailyRow{
		RepoID: &repoID, Day: day, Provider: "jira", TeamID: teamID,
		IssueTypeNorm: "bug", CreatedCount: 1, CompletedCount: 2, ActiveCount: 3,
		CycleP50Hours: 4, CycleP90Hours: 5, LeadP50Hours: 6, ComputedAt: now,
		OrgID: claim.OrgID,
	}
	cycle := JiraWorkItemCycleTimePersistenceRow{
		WorkItemID: "jira:OPS-1", Provider: "jira", Day: metricDay,
		WorkScopeID: "OPS", TeamID: teamID, TeamName: teamName, Assignee: &assignee,
		Type: "feature", Status: "done", CreatedAt: now.Add(-72 * time.Hour),
		CompletedAt: timePtr(now.Add(-2 * time.Hour)), CycleTimeHours: floatPtr(2),
		LeadTimeHours: floatPtr(72), ComputedAt: now, OrgID: claim.OrgID,
	}
	metrics := githubWorkItemMetricTestGroupRow()
	metrics.Provider, metrics.OrgID, metrics.Day = "jira", claim.OrgID, metricDay
	users := githubWorkItemMetricTestUserRow()
	users.Provider, users.OrgID, users.Day = "jira", claim.OrgID, metricDay
	state := JiraWorkItemStateDurationDailyRow{
		Day: day, Provider: "jira", WorkScopeID: "OPS", TeamID: teamID,
		TeamName: teamName, Status: "in_progress", DurationHours: 6, ItemsTouched: 1,
		ComputedAt: now, AvgWIP: 0.25, OrgID: claim.OrgID,
	}
	team := JiraWorkItemTeamAttributionRow{
		WorkItemID: "jira:OPS-1", Provider: "jira", Source: "project_ownership",
		IsPrimary: 1, Confidence: "high", Evidence: "project_ownership=10001",
		ComputedAt: now, RepoID: &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: claim.OrgID,
	}

	effects, err := BuildJiraWorkItemDerivedEffects(JiraWorkItemDerivedEffectRows{
		EstimateCoverageMetricsDaily:   []JiraEstimateCoverageMetricsDailyRow{estimate},
		InvestmentClassificationsDaily: []JiraInvestmentClassificationDailyRow{classification},
		InvestmentMetricsDaily:         []JiraInvestmentMetricsDailyRow{investment},
		IssueTypeMetricsDaily:          []JiraIssueTypeMetricsDailyRow{issueType},
		WorkItemCycleTimes:             []JiraWorkItemCycleTimePersistenceRow{cycle},
		WorkItemMetricsDaily:           []JiraWorkItemMetricsDailyRow{metrics},
		WorkItemStateDurationsDaily:    []JiraWorkItemStateDurationDailyRow{state},
		WorkItemTeamAttributions:       []JiraWorkItemTeamAttributionRow{team},
		WorkItemUserMetricsDaily:       []JiraWorkItemUserMetricsDailyRow{users},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range effects {
		t.Run(effect.Destination, func(t *testing.T) {
			inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
			if inspectErr != nil || inspection != EffectAbsent {
				t.Fatalf("before write: inspection=%v error=%v", inspection, inspectErr)
			}
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatal(err)
			}
			inspection, inspectErr = sink.InspectEffect(ctx, claim, effect)
			want := EffectExact
			if effect.Destination == "ai_attribution" {
				want = EffectAbsent
			}
			if inspectErr != nil || inspection != want {
				t.Fatalf("after write: inspection=%v want=%v error=%v", inspection, want, inspectErr)
			}
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatalf("replay write: %v", err)
			}
		})
	}

	foreign := claim
	foreign.OrgID = "org-other"
	for _, effect := range effects {
		if effect.Destination == "ai_attribution" {
			if inspection, err := sink.InspectEffect(ctx, foreign, effect); err != nil || inspection != EffectAbsent {
				t.Fatalf("foreign evaluated-empty AI=%v err=%v", inspection, err)
			}
			continue
		}
		if err := sink.WriteEffect(ctx, foreign, effect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("foreign tenant %s write=%v", effect.Destination, err)
		}
	}

	lost, err := NewJiraWorkItemDerivedClickHouseEffects(
		conn,
		providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := lost.WriteEffect(ctx, claim, effects[1]); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease-lost real-store write=%v", err)
	}
}
