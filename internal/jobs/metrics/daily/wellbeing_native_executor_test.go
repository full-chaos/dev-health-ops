package daily

import (
	"context"
	stddriver "database/sql/driver"
	"errors"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// stubDriverConn is an unimplemented clickhouse driver.Conn. Every method
// panics, which is deliberate: a test that reaches one has left the path it
// meant to exercise (construction-only tests below never issue a query), and
// a silently-zero return would let it pass anyway. Mirrors
// internal/jobs/metrics/remaining's driverConnStub.
type stubDriverConn struct{}

func (stubDriverConn) Contributors() []string { panic("stub: Contributors") }
func (stubDriverConn) ServerVersion() (*chdriver.ServerVersion, error) {
	panic("stub: ServerVersion")
}
func (stubDriverConn) Select(context.Context, any, string, ...any) error { panic("stub: Select") }
func (stubDriverConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	panic("stub: Query")
}
func (stubDriverConn) QueryRow(context.Context, string, ...any) chdriver.Row {
	panic("stub: QueryRow")
}
func (stubDriverConn) PrepareBatch(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	panic("stub: PrepareBatch")
}
func (stubDriverConn) Exec(context.Context, string, ...any) error { panic("stub: Exec") }
func (stubDriverConn) AsyncInsert(context.Context, string, bool, ...any) error {
	panic("stub: AsyncInsert")
}
func (stubDriverConn) Ping(context.Context) error                  { panic("stub: Ping") }
func (stubDriverConn) Stats() chdriver.Stats                       { panic("stub: Stats") }
func (stubDriverConn) Close() error                                { panic("stub: Close") }
func (stubDriverConn) CheckNamedValue(*stddriver.NamedValue) error { panic("stub: CheckNamedValue") }

func mustParseDay(t *testing.T, value string) time.Time {
	t.Helper()
	day, err := time.Parse("2006-01-02", value)
	if err != nil {
		t.Fatal(err)
	}
	return day
}

func TestNewTeamWellbeingExecutorRejectsNilConn(t *testing.T) {
	if _, err := NewTeamWellbeingExecutor(nil); !errors.Is(err, errTeamWellbeingUnavailable) {
		t.Fatalf("err=%v, want errTeamWellbeingUnavailable", err)
	}
}

func TestNewTeamWellbeingExecutorRejectsUnparseableTimezone(t *testing.T) {
	t.Setenv("BUSINESS_TIMEZONE", "Not/A_Real_Zone")
	if _, err := NewTeamWellbeingExecutor(&stubDriverConn{}); !errors.Is(err, errTeamWellbeingUnavailable) {
		t.Fatalf("err=%v, want errTeamWellbeingUnavailable", err)
	}
}

func TestNewTeamWellbeingExecutorRejectsUnparseableBusinessHours(t *testing.T) {
	t.Setenv("BUSINESS_HOURS_START", "not-a-number")
	if _, err := NewTeamWellbeingExecutor(&stubDriverConn{}); !errors.Is(err, errTeamWellbeingUnavailable) {
		t.Fatalf("err=%v, want errTeamWellbeingUnavailable", err)
	}
}

func TestNewTeamWellbeingExecutorDefaultsToUTCNineToSeventeen(t *testing.T) {
	executor, err := NewTeamWellbeingExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if executor.businessTZ.String() != "UTC" || executor.businessHoursStart != 9 || executor.businessHoursEnd != 17 {
		t.Fatalf("defaults: tz=%s start=%d end=%d", executor.businessTZ, executor.businessHoursStart, executor.businessHoursEnd)
	}
}

func TestComputeFamilyRejectsMissingOrganizationOrDay(t *testing.T) {
	executor, err := NewTeamWellbeingExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.ComputeFamily(context.Background(), Run{}, Partition{ID: testPartitionID}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestComputeFamilyRejectsUnparseablePartitionRepoIDs(t *testing.T) {
	executor, err := NewTeamWellbeingExecutor(&stubDriverConn{})
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: testOrgID, TargetDay: mustParseDay(t, "2026-08-24")}
	partition := Partition{ID: testPartitionID, RepoIDs: []RepositoryID{"not-a-uuid"}}
	if _, err := executor.ComputeFamily(context.Background(), run, partition); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

// TestComputeWellbeingPerRepoScopesBucketsToOneRepoAtATime is the regression
// test for codex round-1 finding 2 (CHAOS-4276): a team that committed to
// TWO repos in the same partition must land as TWO team_metrics_daily rows
// (one per repo, each counting only that repo's commits), never one row
// aggregating both repos -- mirroring worker_metrics.py's `for repo_id in
// repo_ids` loop, which calls compute_team_wellbeing_metrics_daily (and so
// resets every team's bucket) once per repo.
func TestComputeWellbeingPerRepoScopesBucketsToOneRepoAtATime(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) // Monday
	repoA := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	repoB := uuid.MustParse("00000000-0000-0000-0000-0000000000b2")
	repoIDs := []uuid.UUID{repoA, repoB}

	teams := []WellbeingTeam{
		{ID: "platform", Name: "Platform", RepoPatterns: []string{"org/repo-a", "org/repo-b"}},
	}
	repoResolver := NewRepoPatternResolver(teams)
	memberResolver := NewMemberResolver(teams)
	repoNamesByID := map[string]string{
		repoA.String(): "org/repo-a",
		repoB.String(): "org/repo-b",
	}

	businessHours := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) // Monday, business hours
	commits := []numerical.Commit{
		{RepoID: repoA.String(), AuthorEmail: "dev-a@example.com", CommitterWhen: businessHours},
		{RepoID: repoA.String(), AuthorEmail: "dev-a@example.com", CommitterWhen: businessHours},
		{RepoID: repoB.String(), AuthorEmail: "dev-b@example.com", CommitterWhen: businessHours},
	}

	got := computeWellbeingPerRepo(day, repoIDs, commits, repoNamesByID, repoResolver, memberResolver, time.UTC, 9, 17)

	if len(got) != 2 {
		t.Fatalf("expected 2 repo groups (one per repo) for the one team spanning both repos, got %d: %#v", len(got), got)
	}
	for _, group := range got {
		if len(group) != 1 || group[0].TeamID != "platform" {
			t.Fatalf("unexpected group contents: %#v", group)
		}
	}
	// repo-a's group must count ONLY repo-a's 2 commits, never repo-b's.
	if got[0][0].CommitsCount != 2 {
		t.Fatalf("repo-a group: commits_count=%d, want 2 (repo-b's commit must not leak in)", got[0][0].CommitsCount)
	}
	// repo-b's group must count ONLY repo-b's 1 commit.
	if got[1][0].CommitsCount != 1 {
		t.Fatalf("repo-b group: commits_count=%d, want 1 (repo-a's commits must not leak in)", got[1][0].CommitsCount)
	}
}

func TestComputeWellbeingPerRepoSkipsRepoWithNoCommitsThatDay(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repoA := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	repoB := uuid.MustParse("00000000-0000-0000-0000-0000000000b2")
	repoIDs := []uuid.UUID{repoA, repoB}
	teams := []WellbeingTeam{{ID: "platform", Name: "Platform", RepoPatterns: []string{"org/repo-a"}}}
	repoResolver := NewRepoPatternResolver(teams)
	memberResolver := NewMemberResolver(teams)
	repoNamesByID := map[string]string{repoA.String(): "org/repo-a", repoB.String(): "org/repo-b"}
	commits := []numerical.Commit{
		{RepoID: repoA.String(), AuthorEmail: "dev@example.com", CommitterWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
	}

	got := computeWellbeingPerRepo(day, repoIDs, commits, repoNamesByID, repoResolver, memberResolver, time.UTC, 9, 17)

	if len(got) != 1 {
		t.Fatalf("expected 1 row (repo-b had no commits that day, must not write an empty row), got %d: %#v", len(got), got)
	}
}
