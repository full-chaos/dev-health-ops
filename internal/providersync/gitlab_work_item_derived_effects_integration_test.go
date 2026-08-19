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

// This suite deliberately uses githubDerivedIntegrationConn: it applies the
// production ClickHouse migration chain and authors no local DDL. The GitLab
// dispatcher is then exercised through the same ten schema-specific adapters
// used by the provider route, with the provider identity kept as "gitlab".
func TestGitLabWorkItemDerivedEffectsWriteReadbackAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemDerivedClickHouseEffects(conn, lease)
	if err != nil {
		t.Fatal(err)
	}

	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	now := time.Date(2026, 8, 5, 0, 30, 0, 123000000, time.UTC)
	day := newGitHubWorkItemDerivedDay(now)
	metricDay := newGitHubWorkItemMetricDay(now)
	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	orgID := uuid.MustParse(claim.OrgID)
	teamID, teamName := "team-a", "Team A"
	area, rule := "security", "sec_general"
	assignee := "dev@example.com"
	ratio := 0.5
	actor := "chatgpt-codex[bot]"
	aiAttribution := gitlabAIAttributionRow{
		RecordID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("gitlab-integration-ai")),
		OrgID:    orgID, Provider: "gitlab", SubjectType: "pull_request", SubjectID: "9",
		RepoID: &repoID, Kind: "agent_created", Source: "bot_author", Confidence: 0.9,
		Actor: &actor, Evidence: map[string]any{
			"login": actor, "user_type": "Bot", "app_slug": nil, "known_ai_bot": true,
		},
		ObservedAt: now.Add(-48 * time.Hour), IngestedAt: now,
	}

	estimate := gitlabEstimateCoverageMetricsDailyRow{
		Day: day, Provider: "gitlab", WorkScopeID: "acme/api", TeamID: &teamID,
		TeamName: &teamName, EstimatedCount: 1, UnestimatedCount: 1, BacklogSize: 2,
		Ratio: &ratio, ComputedAt: now, OrgID: claim.OrgID,
	}
	classification := gitlabInvestmentClassificationDailyRow{
		RepoID: &repoID, Day: day, ArtifactType: "work_item", ArtifactID: "acme/api#1",
		Provider: "gitlab", InvestmentArea: &area, ProjectStream: "general", Confidence: 1,
		RuleID: &rule, ComputedAt: now, OrgID: claim.OrgID,
	}
	investment := gitlabInvestmentMetricsDailyRow{
		RepoID: &repoID, Day: day, TeamID: "payments", InvestmentArea: &area,
		ProjectStream: "general", DeliveryUnits: 3, WorkItemsCompleted: 2, PRsMerged: 1,
		ChurnLOC: 4, CycleP50Hours: 5, ComputedAt: now, OrgID: claim.OrgID,
	}
	issueType := gitlabIssueTypeMetricsDailyRow{
		RepoID: &repoID, Day: day, Provider: "gitlab", TeamID: "payments",
		IssueTypeNorm: "bug", CreatedCount: 1, CompletedCount: 2, ActiveCount: 3,
		CycleP50Hours: 4, CycleP90Hours: 5, LeadP50Hours: 6, ComputedAt: now,
		OrgID: claim.OrgID,
	}
	cycle := gitlabWorkItemCycleTimePersistenceRow{
		WorkItemID: "gitlab:acme/api#1", Provider: "gitlab", Day: metricDay,
		WorkScopeID: "acme/api", TeamID: teamID, TeamName: teamName, Assignee: &assignee,
		Type: "feature", Status: "done", CreatedAt: now.Add(-72 * time.Hour),
		CompletedAt: timePtr(now.Add(-2 * time.Hour)), CycleTimeHours: floatPtr(2),
		LeadTimeHours: floatPtr(72), ComputedAt: now, OrgID: claim.OrgID,
	}
	metrics := githubWorkItemMetricTestGroupRow()
	metrics.Provider, metrics.OrgID, metrics.Day = "gitlab", claim.OrgID, metricDay
	users := githubWorkItemMetricTestUserRow()
	users.Provider, users.OrgID, users.Day = "gitlab", claim.OrgID, metricDay
	state := gitlabWorkItemStateDurationDailyRow{
		Day: day, Provider: "gitlab", WorkScopeID: "acme/api", TeamID: teamID,
		TeamName: teamName, Status: "in_progress", DurationHours: 6, ItemsTouched: 1,
		ComputedAt: now, AvgWIP: 0.25, OrgID: claim.OrgID,
	}
	team := gitlabWorkItemTeamAttributionRow{
		WorkItemID: "gitlab:acme/api#1", Provider: "gitlab", Source: "native_team",
		IsPrimary: 1, Confidence: "high", Evidence: "native", ComputedAt: now,
		RepoID: &repoID, TeamID: &teamID, TeamName: &teamName, OrgID: claim.OrgID,
	}

	effects, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{
		AIAttributions:                 []gitlabAIAttributionRow{aiAttribution},
		EstimateCoverageMetricsDaily:   []gitlabEstimateCoverageMetricsDailyRow{estimate},
		InvestmentClassificationsDaily: []gitlabInvestmentClassificationDailyRow{classification},
		InvestmentMetricsDaily:         []gitlabInvestmentMetricsDailyRow{investment},
		IssueTypeMetricsDaily:          []gitlabIssueTypeMetricsDailyRow{issueType},
		WorkItemCycleTimes:             []gitlabWorkItemCycleTimePersistenceRow{cycle},
		WorkItemMetricsDaily:           []gitlabWorkItemMetricsDailyRow{metrics},
		WorkItemStateDurationsDaily:    []gitlabWorkItemStateDurationDailyRow{state},
		WorkItemTeamAttributions:       []gitlabWorkItemTeamAttributionRow{team},
		WorkItemUserMetricsDaily:       []gitlabWorkItemUserMetricsDailyRow{users},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != len(gitlabWorkItemDerivedDestinations) {
		t.Fatalf("effects=%d want=%d", len(effects), len(gitlabWorkItemDerivedDestinations))
	}

	for _, effect := range effects {
		t.Run(effect.Destination, func(t *testing.T) {
			if inspection, inspectErr := sink.InspectEffect(ctx, claim, effect); inspectErr != nil || inspection != EffectAbsent {
				t.Fatalf("before write: inspection=%v error=%v", inspection, inspectErr)
			}
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatal(err)
			}
			if inspection, inspectErr := sink.InspectEffect(ctx, claim, effect); inspectErr != nil || inspection != EffectExact {
				t.Fatalf("after write: inspection=%v error=%v", inspection, inspectErr)
			}
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatalf("replay write: %v", err)
			}
			if inspection, inspectErr := sink.InspectEffect(ctx, claim, effect); inspectErr != nil || inspection != EffectExact {
				t.Fatalf("replay readback: inspection=%v error=%v", inspection, inspectErr)
			}

			foreign := claim
			foreign.OrgID = "org-other"
			inspection, inspectErr := sink.InspectEffect(ctx, foreign, effect)
			switch effect.Destination {
			case "estimate_coverage_metrics_daily", "work_item_state_durations_daily", "work_item_team_attributions":
				if inspectErr != nil || inspection != EffectAbsent {
					t.Fatalf("foreign tenant readback: inspection=%v error=%v", inspection, inspectErr)
				}
				if writeErr := sink.WriteEffect(ctx, foreign, effect); !errors.Is(writeErr, ErrInvalidConfiguration) {
					t.Fatalf("foreign tenant write error=%v want=%v", writeErr, ErrInvalidConfiguration)
				}
			default:
				if inspectErr == nil || inspection == EffectExact {
					t.Fatalf("foreign tenant was not rejected: inspection=%v error=%v", inspection, inspectErr)
				}
			}
		})
	}

	// Simulate a worker dying after the durable write but before the adapter's
	// post-write lease assertion. Recovery must find the exact row and avoid a
	// duplicate write. Only the estimate adapter uses the expiring guard; the
	// outer guard remains valid long enough to enter the adapter.
	recoveryGuard := &secondAssertionLosesLease{}
	recoverySink := sink
	recoverySink.EstimateCoverageMetricsDaily.Lease = recoveryGuard
	var estimateEffect EffectBatch
	for _, effect := range effects {
		if effect.Destination == "estimate_coverage_metrics_daily" {
			estimateEffect = effect
			break
		}
	}
	if estimateEffect.Destination == "" {
		t.Fatal("estimate effect missing")
	}
	if err := recoverySink.WriteEffect(ctx, claim, estimateEffect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("post-write lease loss error=%v", err)
	}
	if inspection, inspectErr := sink.InspectEffect(ctx, claim, estimateEffect); inspectErr != nil || inspection != EffectExact {
		t.Fatalf("recovery readback: inspection=%v error=%v", inspection, inspectErr)
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func floatPtr(value float64) *float64 { return &value }
