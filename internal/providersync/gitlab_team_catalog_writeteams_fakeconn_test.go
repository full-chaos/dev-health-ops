package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamcreated"
	"github.com/google/uuid"
)

// fakeGitLabWriteTeamsConn is a driver.Conn double covering exactly what
// writeTeams touches: PreserveExistingTeamManualMembers's read (always
// succeeds, empty), the created_at carry, and PrepareBatch (returns a fake
// batch that just records the statement and what was appended -- no real
// ClickHouse encoding/columns, proving WHICH rows reach the batch, not wire
// format). Any other query, a read of the roster column included, is an error.
type fakeGitLabWriteTeamsConn struct {
	driver.Conn
	statement  string
	created    map[string]time.Time
	createdErr error
	batch      *fakeGitLabWriteTeamsBatch
}

func (f *fakeGitLabWriteTeamsConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case query == teamcreated.Query:
		if f.createdErr != nil {
			return nil, f.createdErr
		}
		return &fakeGitLabCreatedRows{rows: f.created, index: -1}, nil
	case strings.Contains(query, "manual_members"):
		return &fakeGitLabGuardMembershipRows{rows: nil, index: -1}, nil
	default:
		return nil, fmt.Errorf("fakeGitLabWriteTeamsConn: unexpected query: %s", query)
	}
}

func (f *fakeGitLabWriteTeamsConn) PrepareBatch(_ context.Context, statement string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	f.statement = statement
	f.batch = &fakeGitLabWriteTeamsBatch{}
	return f.batch, nil
}

// fakeGitLabWriteTeamsBatch is a driver.Batch double: Append just records
// the row's first argument (the team id, always argument 0 in
// gitlabTeamCatalogTeamsInsert's column order) so the test can assert WHICH
// teams reached the batch without needing real ClickHouse column encoding.
type fakeGitLabWriteTeamsBatch struct {
	driver.Batch
	appendedIDs []string
	appended    [][]any
	sent        bool
	aborted     bool
}

func (b *fakeGitLabWriteTeamsBatch) Append(v ...any) error {
	id, ok := v[0].(string)
	if !ok {
		return fmt.Errorf("fakeGitLabWriteTeamsBatch: unexpected Append arg0 type %T", v[0])
	}
	b.appendedIDs = append(b.appendedIDs, id)
	b.appended = append(b.appended, v)
	return nil
}
func (b *fakeGitLabWriteTeamsBatch) Send() error  { b.sent = true; return nil }
func (b *fakeGitLabWriteTeamsBatch) Abort() error { b.aborted = true; return nil }
func (b *fakeGitLabWriteTeamsBatch) Column(int) driver.BatchColumn {
	panic("fakeGitLabWriteTeamsBatch: Column not implemented -- writeTeams uses Append, never Column")
}
func (b *fakeGitLabWriteTeamsBatch) Flush() error                { return nil }
func (b *fakeGitLabWriteTeamsBatch) IsSent() bool                { return b.sent }
func (b *fakeGitLabWriteTeamsBatch) Rows() int                   { return len(b.appendedIDs) }
func (b *fakeGitLabWriteTeamsBatch) Columns() []column.Interface { return nil }
func (b *fakeGitLabWriteTeamsBatch) AppendStruct(any) error {
	panic("fakeGitLabWriteTeamsBatch: AppendStruct not implemented -- writeTeams uses Append")
}

func gitlabWriteTeamsTestRow(id string) gitlabTeamCatalogTeamRow {
	return gitlabTeamCatalogTeamRow{
		ID: id, TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)).String(),
		Name: id, Members: []string{"gitlab:" + id},
		ProjectKeys: []string{}, RepoPatterns: []string{}, IsActive: 1,
		UpdatedAt: time.Now().UTC(), OrgID: "org-1", Provider: gitlabTeamCatalogProvider,
	}
}

// TestWriteTeamsWritesNoRosterColumn proves the teams writer sends no roster:
// the fake conn refuses every query but the manual_members carry and the
// created_at carry, so a read of the stored roster fails the write, and the
// INSERT statement names no `members` column and carries one value per
// column, with the observed roster of the row (Members) nowhere in it.
func TestWriteTeamsWritesNoRosterColumn(t *testing.T) {
	conn := &fakeGitLabWriteTeamsConn{}
	sink := GitLabTeamCatalogClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	rows := []gitlabTeamCatalogTeamRow{gitlabWriteTeamsTestRow("gl:org"), gitlabWriteTeamsTestRow("gl:org/team-a")}
	if err := sink.writeTeams(context.Background(), Claim{Unit: Unit{OrgID: "org-1", Provider: gitlabTeamCatalogProvider}}, rows); err != nil {
		t.Fatalf("writeTeams: %v", err)
	}
	if conn.batch == nil || !conn.batch.sent || len(conn.batch.appendedIDs) != 2 {
		t.Fatalf("batch = %+v, want both rows sent", conn.batch)
	}
	columns := strings.Split(conn.statement[strings.Index(conn.statement, "(")+1:strings.Index(conn.statement, ")")], ",")
	for _, column := range columns {
		if strings.TrimSpace(column) == "members" {
			t.Fatalf("the teams INSERT names the roster column: %s", conn.statement)
		}
	}
	for index, appended := range conn.batch.appended {
		if len(appended) != len(columns) {
			t.Fatalf("row %d carries %d values for %d columns: %s", index, len(appended), len(columns), conn.statement)
		}
		for _, value := range appended {
			if list, ok := value.([]string); ok && len(list) == 1 && strings.HasPrefix(list[0], "gitlab:") {
				t.Fatalf("row %d carries the observed roster %v into the teams INSERT", index, list)
			}
		}
	}
}

// TestGitLabTeamCatalogCollectorNonStrictWalkFailureMakesNoWrites is the
// full adapter-level proof of the Python-parity ruling: a non-strict walk
// failure must reach ZERO Sink calls -- no PrepareBatch, no Query for
// roster/manual-members preservation, nothing. Uses a real HTTP fake server
// (the walk failure itself) plus fakeGitLabWriteTeamsConn as the Sink,
// which errors on any unexpected query and would surface a non-nil
// conn.batch if PrepareBatch were ever called.
func TestGitLabTeamCatalogCollectorNonStrictWalkFailureMakesNoWrites(t *testing.T) {
	group := func(id int, fullPath, name string) map[string]any {
		return map[string]any{"id": id, "full_path": fullPath, "name": name, "description": nil}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			_ = json.NewEncoder(w).Encode(group(1, "org", "Org"))
		case "/api/v4/groups/org/subgroups":
			http.Error(w, "simulated subgroups fetch failure", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	client, err := providerfoundation.NewHTTPClient(
		"gitlab", server.URL, http.DefaultClient,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}

	conn := &fakeGitLabWriteTeamsConn{}
	collector := GitLabTeamCatalogCollector{
		Handler: GitLabTeamCatalogRouteHandler{},
		Sink: GitLabTeamCatalogClickHouseEffects{
			Conn:  conn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		},
	}
	ref := TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: false}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}

	result, err := collector.CollectTeamCatalog(context.Background(), ref, credential, client, TeamCatalogSelections{Teams: true}, time.Now())
	if err != nil {
		t.Fatalf("non-strict walk failure must not error: %v", err)
	}
	if result.TeamsWritten != 0 || result.MembersWritten != 0 || result.MembershipsWritten != 0 ||
		result.ProjectsWritten != 0 || result.OwnershipWritten != 0 || len(result.TeamKeys) != 0 {
		t.Fatalf("result=%+v, want a zero TeamCatalogResult", result)
	}
	if !result.Skipped || result.SkipReason != "subgroups_fetch_failed" {
		t.Fatalf("result=%+v, want Skipped=true SkipReason=subgroups_fetch_failed -- a caller must be able to tell this apart from a real zero-row success", result)
	}
	if conn.batch != nil {
		t.Fatalf("PrepareBatch was called (batch=%+v) -- a non-strict walk failure must make ZERO Sink calls", conn.batch)
	}
}

type fakeGitLabCreatedRows struct {
	driver.Rows
	rows  map[string]time.Time
	index int
	ids   []string
}

func (r *fakeGitLabCreatedRows) Next() bool {
	if r.index == -1 {
		for id := range r.rows {
			r.ids = append(r.ids, id)
		}
	}
	r.index++
	return r.index < len(r.ids)
}

func (r *fakeGitLabCreatedRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.ids[r.index]
	*dest[1].(*time.Time) = r.rows[r.ids[r.index]]
	return nil
}
func (r *fakeGitLabCreatedRows) Close() error { return nil }
func (r *fakeGitLabCreatedRows) Err() error   { return nil }

// The created_at column is the last value of the gitlab team insert: a team
// with a stored row keeps that creation time, a new team takes its own
// updated_at.
func TestGitLabWriteTeamsCarriesCreatedAt(t *testing.T) {
	original := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	conn := &fakeGitLabWriteTeamsConn{created: map[string]time.Time{"gl:org": original}}
	sink := GitLabTeamCatalogClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	rows := []gitlabTeamCatalogTeamRow{gitlabWriteTeamsTestRow("gl:org"), gitlabWriteTeamsTestRow("gl:org/new")}
	if err := sink.writeTeams(context.Background(), Claim{Unit: Unit{OrgID: "org-1", Provider: gitlabTeamCatalogProvider}}, rows); err != nil {
		t.Fatal(err)
	}
	last := func(i int) time.Time { v := conn.batch.appended[i]; return v[len(v)-1].(time.Time) }
	if !last(0).Equal(original) {
		t.Fatalf("existing team created_at = %v, want %v", last(0), original)
	}
	if !last(1).Equal(rows[1].UpdatedAt.Truncate(time.Microsecond)) {
		t.Fatalf("new team created_at = %v, want its updated_at %v", last(1), rows[1].UpdatedAt)
	}
}
