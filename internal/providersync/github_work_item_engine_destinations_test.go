package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

func TestNewGitHubWorkItemEngineDeriverRejectsAMissingStatusMapping(t *testing.T) {
	engine, err := NewGitHubWorkItemEngineDeriver(nil)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
	if engine != nil {
		t.Fatalf("rejected constructor returned engine=%+v", engine)
	}
}

func TestNewGitHubWorkItemDeriverLoadsTheExplicitConfig(t *testing.T) {
	conn := stubWorkItemConn{}
	lease := githubWorkItemCompositionLease()
	statusPath := resolveStatusMappingConfig(t, "real")

	for _, testCase := range []struct {
		name       string
		conn       driver.Conn
		lease      providerfoundation.LeaseGuard
		statusPath string
	}{
		{name: "missing connection", lease: lease, statusPath: statusPath},
		{name: "missing lease", conn: conn, statusPath: statusPath},
		{name: "blank status path", conn: conn, lease: lease},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deriver, err := NewGitHubWorkItemDeriver(testCase.conn, testCase.lease, testCase.statusPath)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			if deriver != nil {
				t.Fatalf("rejected constructor returned deriver=%+v", deriver)
			}
		})
	}

	deriver, err := NewGitHubWorkItemDeriver(conn, lease, statusPath)
	if err != nil {
		t.Fatal(err)
	}
	source, ok := deriver.Source.(githubWorkItemClickHouseDerivationContextSource)
	if !ok || source.Conn == nil || source.Lease == nil {
		t.Fatalf("constructor installed unreachable source=%T %+v", deriver.Source, source)
	}
	engine, ok := deriver.engine.(*GitHubWorkItemEngineDeriver)
	if !ok || engine.statusMapping == nil {
		t.Fatalf("constructor installed partial engine=%T %+v", deriver.engine, engine)
	}
}

func TestGitHubWorkItemEngineDeriverRejectsACorruptedEngine(t *testing.T) {
	claim, rows, day, computedAt, derived := githubWorkItemEngineFixture(t)
	engine := &GitHubWorkItemEngineDeriver{}
	if _, err := engine.Derive(
		context.Background(), claim, rows, day, computedAt, derived,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
}

func TestGitHubWorkItemEngineDeriverDerivesIssueTypesPerDay(t *testing.T) {
	statusMapping := loadRealStatusMapping(t)
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping)
	if err != nil {
		t.Fatal(err)
	}
	claim, rows, day, computedAt, derived := githubWorkItemEngineFixture(t)

	encoded, err := engine.Derive(
		context.Background(), claim, rows, day, computedAt, derived,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 1 {
		t.Fatalf("destinations=%d want=1 (the investment classifier destinations are gone)", len(encoded))
	}

	issueTypes := decodeGitHubWorkItemEngineRows[githubIssueTypeMetricsDailyRow](
		t, encoded[githubIssueTypeMetricsDestination],
	)

	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	wantStamp := computedAt.Truncate(time.Second)
	wantIssueTypes := []githubIssueTypeMetricsDailyRow{
		{
			RepoID: &repoID, Day: "2026-08-04", Provider: "github",
			TeamID: "payments", IssueTypeNorm: "bug",
			CreatedCount: 1, CompletedCount: 2, ActiveCount: 2,
			CycleP50Hours: 9, CycleP90Hours: 9, LeadP50Hours: 0,
			ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
		{
			Day: "2026-08-04", Provider: "github", TeamID: "unassigned",
			IssueTypeNorm: "chore", ActiveCount: 1,
			ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
		{
			Day: "2026-08-04", Provider: "github", TeamID: "unassigned",
			IssueTypeNorm: "issue", ComputedAt: wantStamp, OrgID: claim.OrgID,
		},
	}
	if !reflect.DeepEqual(issueTypes, wantIssueTypes) {
		t.Fatalf("issue types:\n got: %#v\nwant: %#v", issueTypes, wantIssueTypes)
	}
}

func githubWorkItemEngineFixture(
	t *testing.T,
) (Claim, githubWorkItemRows, time.Time, time.Time, teamattribution.GithubWorkItemDerivationContext) {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = "org-acme"
	day := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 5, 0, 30, 17, 987654321, time.UTC)
	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	stringValue := func(value string) *string { return &value }
	floatValue := func(value float64) *float64 { return &value }
	timeValue := func(value time.Time) *time.Time { return &value }
	item := func(id string) githubWorkItemRow {
		return githubWorkItemRow{
			WorkItemID: id, Provider: "github", Title: id, Type: "issue",
			Status: "todo", ProjectID: stringValue("acme/api"),
			CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt: day, OrgID: claim.OrgID,
		}
	}
	first := item("acme/api#1")
	first.RepoID = &repoID
	first.Type = "pull_request"
	first.Labels = []string{"bug", "security"}
	first.CreatedAt = time.Date(2026, 8, 4, 1, 0, 0, 0, time.UTC)
	first.StartedAt = timeValue(time.Date(2026, 8, 4, 2, 0, 0, 0, time.UTC))
	first.CompletedAt = timeValue(time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC))
	first.StoryPoints = floatValue(3.7)

	second := item("acme/api#2")
	second.RepoID = &repoID
	second.Labels = []string{"security", "bug"}
	second.StartedAt = timeValue(time.Date(2026, 8, 4, 5, 0, 0, 0, time.UTC))
	second.CompletedAt = timeValue(time.Date(2026, 8, 4, 14, 0, 0, 0, time.UTC))
	second.StoryPoints = floatValue(0)

	third := item("acme/api#3")
	third.ProjectID = stringValue("acme/other")
	third.Labels = []string{"feature", "chore"}

	future := item("acme/api#4")
	future.ProjectID = stringValue("acme/other")
	future.CreatedAt = time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)

	prior := item("acme/api#5")
	prior.ProjectID = stringValue("acme/other")
	prior.Labels = []string{"maintenance"}
	prior.CompletedAt = timeValue(time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))

	repoIDString := repoID.String()
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Repos: []teamattribution.GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "payments", TeamName: "Payments",
			RepoID: &repoIDString, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 100, Priority: 10,
			UpdatedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		}},
	})
	return claim, githubWorkItemRows{WorkItems: []githubWorkItemRow{
		first, second, third, future, prior,
	}}, day, computedAt, derived
}

func decodeGitHubWorkItemEngineRows[T any](
	t *testing.T, rows []json.RawMessage,
) []T {
	t.Helper()
	result := make([]T, 0, len(rows))
	for _, raw := range rows {
		var row T
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}
