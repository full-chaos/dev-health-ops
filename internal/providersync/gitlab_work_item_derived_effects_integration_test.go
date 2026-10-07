//go:build integration

package providersync

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This suite deliberately uses githubDerivedIntegrationConn: it applies the
// production ClickHouse migration chain and authors no local DDL. The GitLab
// sync sink is exercised with the provider identity kept as "gitlab". It
// writes and reads back ai_attribution, the one effect it holds beside the raw
// tables, and refuses each of the nine tables the daily job computes from
// stored rows: the effects carry real rows, and no row reaches the store.
func TestGitLabWorkItemDerivedEffectsWriteReadbackAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemDerivedClickHouseEffects(conn, lease, nil)
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
	for _, destination := range gitlabWorkItemSyncSinkDestinations {
		if slices.Contains(githubWorkItemDerivedDestinations, destination) {
			t.Fatalf("%s is a destination of the sync sink and a table of the daily job", destination)
		}
	}

	refused := 0
	for _, effect := range effects {
		if !slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
			continue
		}
		refused++
		t.Run("refuses "+effect.Destination, func(t *testing.T) {
			assertSyncSinkRefusesDailyJobTable(t, ctx, conn, sink, claim, effect)
		})
	}
	if refused != len(githubWorkItemDerivedDestinations) || refused != 9 {
		t.Fatalf("refused effects=%d want one for each of the %d tables of the daily job", refused, len(githubWorkItemDerivedDestinations))
	}

	var aiEffect EffectBatch
	for _, effect := range effects {
		if effect.Destination == "ai_attribution" {
			aiEffect = effect
		}
	}
	if aiEffect.Destination == "" || len(aiEffect.Rows) != 1 {
		t.Fatalf("ai_attribution effect=%q rows=%d want one row", aiEffect.Destination, len(aiEffect.Rows))
	}
	foreign := claim
	foreign.OrgID = "org-other"

	// The worker dies after the durable write and before the sink's post-write
	// lease assertion. Recovery must find the exact rows and write no
	// duplicate.
	t.Run("ai_attribution recovers after lease loss", func(t *testing.T) {
		if inspection, inspectErr := sink.InspectEffect(ctx, claim, aiEffect); inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("before write: inspection=%v error=%v", inspection, inspectErr)
		}
		recoverySink := sink
		recoverySink.Lease = &secondAssertionLosesLease{}
		if err := recoverySink.WriteEffect(ctx, claim, aiEffect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("post-write lease loss error=%v", err)
		}
		if inspection, inspectErr := sink.InspectEffect(ctx, claim, aiEffect); inspectErr != nil || inspection != EffectExact {
			t.Fatalf("recovery readback: inspection=%v error=%v", inspection, inspectErr)
		}
	})

	t.Run("ai_attribution", func(t *testing.T) {
		if err := sink.WriteEffect(ctx, claim, aiEffect); err != nil {
			t.Fatal(err)
		}
		if inspection, inspectErr := sink.InspectEffect(ctx, claim, aiEffect); inspectErr != nil || inspection != EffectExact {
			t.Fatalf("after write: inspection=%v error=%v", inspection, inspectErr)
		}
		if err := sink.WriteEffect(ctx, claim, aiEffect); err != nil {
			t.Fatalf("replay write: %v", err)
		}
		if inspection, inspectErr := sink.InspectEffect(ctx, claim, aiEffect); inspectErr != nil || inspection != EffectExact {
			t.Fatalf("replay readback: inspection=%v error=%v", inspection, inspectErr)
		}
		if inspection, inspectErr := sink.InspectEffect(ctx, foreign, aiEffect); inspectErr == nil || inspection == EffectExact {
			t.Fatalf("foreign tenant was not rejected: inspection=%v error=%v", inspection, inspectErr)
		}
	})
}

// assertSyncSinkRefusesDailyJobTable holds the contract of a work-items sync
// sink for one of the nine tables the daily job writes: the write and the
// readback are refused as an invalid configuration, and the table holds no row
// of the tenant after the refused write. The effect must carry rows: a refusal
// of an empty effect would prove nothing about the store.
func assertSyncSinkRefusesDailyJobTable(
	t *testing.T,
	ctx context.Context,
	conn driver.Conn,
	sink interface {
		EffectSink
		EffectReadback
	},
	claim Claim,
	effect EffectBatch,
) {
	t.Helper()
	if !slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
		t.Fatalf("%s is not a table of the daily job", effect.Destination)
	}
	if len(effect.Rows) == 0 {
		t.Fatalf("%s: the effect has no row; the refusal would not show that the store stays empty", effect.Destination)
	}
	if err := sink.WriteEffect(ctx, claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("%s: the sync sink wrote a table of the daily job: error=%v", effect.Destination, err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
		t.Fatalf("%s: readback=%v error=%v want a refused conflict", effect.Destination, inspection, err)
	}
	var stored uint64
	// The destination is one of the nine fixed table names checked above.
	query := "SELECT count() FROM " + effect.Destination + " WHERE org_id = ?"
	if err := conn.QueryRow(ctx, query, claim.OrgID).Scan(&stored); err != nil {
		t.Fatalf("%s: count stored rows: %v", effect.Destination, err)
	}
	if stored != 0 {
		t.Fatalf("%s: %d rows of the tenant are stored after the refused write", effect.Destination, stored)
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func floatPtr(value float64) *float64 { return &value }
