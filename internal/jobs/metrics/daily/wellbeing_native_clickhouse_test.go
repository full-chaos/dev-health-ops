package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

func TestLoadWellbeingTeamsUsesProductionQueryWithTenantFence(t *testing.T) {
	connection := &recordingRepositoryConnection{rows: &wellbeingTeamRowsStub{teams: []WellbeingTeam{
		{ID: "team-a", Name: "Team A", Members: []string{"a@example.com"}, RepoPatterns: []string{"org/a"}},
	}}}
	teams, err := LoadWellbeingTeams(context.Background(), connection, "org-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(teams) != 1 || teams[0].ID != "team-a" {
		t.Fatalf("teams=%#v", teams)
	}
	if len(connection.arguments) != 1 || connection.arguments[0] != "org-1" {
		t.Fatalf("query arguments=%v, want only tenant id", connection.arguments)
	}
	if connection.query != "SELECT id, name, members, repo_patterns FROM teams FINAL WHERE org_id = ?" {
		t.Fatalf("unexpected query: %s", connection.query)
	}
}

func TestLoadWellbeingTeamsRejectsMissingOrg(t *testing.T) {
	if _, err := LoadWellbeingTeams(context.Background(), &recordingRepositoryConnection{}, ""); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestRepoPatternResolverExactAndWildcard(t *testing.T) {
	teams := []WellbeingTeam{
		{ID: "team-exact", Name: "Exact Team", RepoPatterns: []string{"org/service-a"}},
		{ID: "team-wild-long", Name: "Wild Long", RepoPatterns: []string{"org/wild-service-*"}},
		{ID: "team-wild-short", Name: "Wild Short", RepoPatterns: []string{"org/wild-*"}},
	}
	resolver := NewRepoPatternResolver(teams)

	if id, name := resolver.ResolveRepo("org/service-a"); id != "team-exact" || name != "Exact Team" {
		t.Fatalf("exact match: id=%s name=%s", id, name)
	}
	// Longest prefix must win when several match.
	if id, _ := resolver.ResolveRepo("org/wild-service-x"); id != "team-wild-long" {
		t.Fatalf("expected longest prefix to win, got %s", id)
	}
	if id, _ := resolver.ResolveRepo("org/wild-other"); id != "team-wild-short" {
		t.Fatalf("expected shorter prefix fallback, got %s", id)
	}
	if id, name := resolver.ResolveRepo("unrelated/repo"); id != "" || name != "" {
		t.Fatalf("expected no match, got id=%s name=%s", id, name)
	}
	if id, _ := resolver.ResolveRepo(""); id != "" {
		t.Fatalf("expected empty repo name to resolve to nothing, got %s", id)
	}
	// Case- and whitespace-insensitive, mirroring repo_name.strip().lower().
	if id, _ := resolver.ResolveRepo("  ORG/Service-A  "); id != "team-exact" {
		t.Fatalf("expected case/whitespace-insensitive match, got %s", id)
	}
}

func TestRepoPatternResolverSkipsTeamsWithNoPatterns(t *testing.T) {
	resolver := NewRepoPatternResolver([]WellbeingTeam{{ID: "team-a", Name: "A"}})
	if id, _ := resolver.ResolveRepo("org/a"); id != "" {
		t.Fatalf("expected no match for team with no repo_patterns, got %s", id)
	}
}

func TestMemberResolverNormalizesIdentity(t *testing.T) {
	teams := []WellbeingTeam{
		{ID: "team-a", Name: "Team A", Members: []string{"Dev@Example.com", "  Display   Name  "}},
	}
	resolver := NewMemberResolver(teams)

	if id, name := resolver.ResolveMember("dev@example.com"); id != "team-a" || name != "Team A" {
		t.Fatalf("case-insensitive match failed: id=%s name=%s", id, name)
	}
	if id, _ := resolver.ResolveMember("Display Name"); id != "team-a" {
		t.Fatalf("whitespace-collapsed match failed: id=%s", id)
	}
	if id, _ := resolver.ResolveMember("nobody@example.com"); id != "" {
		t.Fatalf("expected no match, got %s", id)
	}
	if id, _ := resolver.ResolveMember(""); id != "" {
		t.Fatalf("expected empty identity to resolve to nothing, got %s", id)
	}
}

func TestMemberResolverFallsBackToTeamIDWhenNameEmpty(t *testing.T) {
	resolver := NewMemberResolver([]WellbeingTeam{{ID: "team-a", Name: "", Members: []string{"a@example.com"}}})
	if _, name := resolver.ResolveMember("a@example.com"); name != "team-a" {
		t.Fatalf("expected team name to fall back to team id, got %q", name)
	}
}

func TestLoadWellbeingCommitsBuildsTenantAndWindowScopedQuery(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	email := "dev@example.com"
	connection := &recordingRepositoryConnection{rows: &wellbeingCommitRowsStub{commits: []wellbeingCommitRow{
		{repoID: repoID, authorEmail: &email, authorName: nil, committerWhen: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)},
	}}}
	start := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	commits, err := LoadWellbeingCommits(context.Background(), connection, "org-1", start, end, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(commits) != 1 || commits[0].RepoID != repoID.String() || commits[0].AuthorEmail != email {
		t.Fatalf("commits=%#v", commits)
	}
	if len(connection.arguments) != 3 {
		t.Fatalf("expected org_id+start+end arguments with no repo scope, got %v", connection.arguments)
	}
}

func TestLoadWellbeingCommitsRejectsInvertedWindow(t *testing.T) {
	start := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	if _, err := LoadWellbeingCommits(context.Background(), &recordingRepositoryConnection{}, "org-1", start, start, nil); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}

func TestWriteTeamMetricsDailyEmptyRowsIsNoop(t *testing.T) {
	written, err := WriteTeamMetricsDaily(context.Background(), &panicBatchConn{t: t}, "org-1", time.Now(), time.Now(), nil)
	if err != nil || written != 0 {
		t.Fatalf("written=%d err=%v, want 0/nil for empty rows", written, err)
	}
}

type panicBatchConn struct{ t *testing.T }

func (conn *panicBatchConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.t.Fatal("PrepareBatch must not be called for zero rows")
	return nil, nil
}

// wellbeingTeamRowsStub and wellbeingCommitRowsStub reuse
// recordingRepositoryConnection from clickhouse_test.go, satisfying the
// driver.Rows contract for their own fixed row shape.

type wellbeingTeamRowsStub struct {
	teams    []WellbeingTeam
	position int
}

func (rows *wellbeingTeamRowsStub) Next() bool { return rows.position < len(rows.teams) }
func (rows *wellbeingTeamRowsStub) Scan(destinations ...any) error {
	if len(destinations) != 4 || rows.position >= len(rows.teams) {
		return errors.New("unexpected wellbeing team scan")
	}
	team := rows.teams[rows.position]
	*(destinations[0].(*string)) = team.ID
	*(destinations[1].(*string)) = team.Name
	*(destinations[2].(*[]string)) = team.Members
	*(destinations[3].(*[]string)) = team.RepoPatterns
	rows.position++
	return nil
}
func (*wellbeingTeamRowsStub) ScanStruct(any) error             { return errors.New("unused") }
func (*wellbeingTeamRowsStub) ColumnTypes() []driver.ColumnType { return nil }
func (*wellbeingTeamRowsStub) Totals(...any) error              { return errors.New("unused") }
func (*wellbeingTeamRowsStub) Columns() []string {
	return []string{"id", "name", "members", "repo_patterns"}
}
func (*wellbeingTeamRowsStub) Close() error  { return nil }
func (*wellbeingTeamRowsStub) Err() error    { return nil }
func (*wellbeingTeamRowsStub) HasData() bool { return true }

type wellbeingCommitRow struct {
	repoID        uuid.UUID
	authorEmail   *string
	authorName    *string
	committerWhen time.Time
}

type wellbeingCommitRowsStub struct {
	commits  []wellbeingCommitRow
	position int
}

func (rows *wellbeingCommitRowsStub) Next() bool { return rows.position < len(rows.commits) }
func (rows *wellbeingCommitRowsStub) Scan(destinations ...any) error {
	if len(destinations) != 4 || rows.position >= len(rows.commits) {
		return errors.New("unexpected wellbeing commit scan")
	}
	row := rows.commits[rows.position]
	*(destinations[0].(*uuid.UUID)) = row.repoID
	*(destinations[1].(**string)) = row.authorEmail
	*(destinations[2].(**string)) = row.authorName
	*(destinations[3].(*time.Time)) = row.committerWhen
	rows.position++
	return nil
}
func (*wellbeingCommitRowsStub) ScanStruct(any) error             { return errors.New("unused") }
func (*wellbeingCommitRowsStub) ColumnTypes() []driver.ColumnType { return nil }
func (*wellbeingCommitRowsStub) Totals(...any) error              { return errors.New("unused") }
func (*wellbeingCommitRowsStub) Columns() []string {
	return []string{"repo_id", "author_email", "author_name", "committer_when"}
}
func (*wellbeingCommitRowsStub) Close() error  { return nil }
func (*wellbeingCommitRowsStub) Err() error    { return nil }
func (*wellbeingCommitRowsStub) HasData() bool { return true }
