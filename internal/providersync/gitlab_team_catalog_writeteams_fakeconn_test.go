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
	"github.com/google/uuid"
)

// fakeGitLabWriteTeamsConn is a driver.Conn double covering exactly what
// writeTeams touches: PreserveExistingTeamManualMembers's read (always
// succeeds, empty), gitlabExistingTeamRoster's read (configurable to fail,
// to exercise the codex review finding below), and PrepareBatch (returns a
// fake batch that just records what was appended -- no real ClickHouse
// encoding/columns, proving WHICH rows reach the batch, not wire format).
type fakeGitLabWriteTeamsConn struct {
	driver.Conn
	rosterQueryErr error
	batch          *fakeGitLabWriteTeamsBatch
}

func (f *fakeGitLabWriteTeamsConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "manual_members"):
		return &fakeGitLabGuardMembershipRows{rows: nil, index: -1}, nil
	case strings.Contains(query, "SELECT id, members FROM teams"):
		if f.rosterQueryErr != nil {
			return nil, f.rosterQueryErr
		}
		return &fakeGitLabGuardMembershipRows{rows: nil, index: -1}, nil
	default:
		return nil, fmt.Errorf("fakeGitLabWriteTeamsConn: unexpected query: %s", query)
	}
}

func (f *fakeGitLabWriteTeamsConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
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
	sent        bool
	aborted     bool
}

func (b *fakeGitLabWriteTeamsBatch) Append(v ...any) error {
	id, ok := v[0].(string)
	if !ok {
		return fmt.Errorf("fakeGitLabWriteTeamsBatch: unexpected Append arg0 type %T", v[0])
	}
	b.appendedIDs = append(b.appendedIDs, id)
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

func gitlabWriteTeamsTestRow(id string, authoritative bool) gitlabTeamCatalogTeamRow {
	return gitlabTeamCatalogTeamRow{
		ID: id, TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)).String(),
		Name: id, Members: []string{}, MembersAuthoritative: authoritative,
		ProjectKeys: []string{}, RepoPatterns: []string{}, IsActive: 1,
		UpdatedAt: time.Now().UTC(), OrgID: "org-1", Provider: gitlabTeamCatalogProvider,
	}
}

// TestWriteTeamsRosterPreservationFailureOnlyExcludesNonAuthoritativeRows is
// the codex review finding's end-to-end proof (team-lead relay, 2026-08-28,
// checked container-free against a fake conn/batch): when the roster-
// preservation read fails, writeTeams must still write every
// MembersAuthoritative=true (self-sufficient) row -- only the rows that
// actually depended on the failed read are excluded.
func TestWriteTeamsRosterPreservationFailureOnlyExcludesNonAuthoritativeRows(t *testing.T) {
	conn := &fakeGitLabWriteTeamsConn{rosterQueryErr: fmt.Errorf("simulated roster read failure")}
	sink := GitLabTeamCatalogClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	rows := []gitlabTeamCatalogTeamRow{
		gitlabWriteTeamsTestRow("gl:org", true),         // healthy, self-sufficient
		gitlabWriteTeamsTestRow("gl:org/team-a", false), // needs the (failed) roster read
		gitlabWriteTeamsTestRow("gl:org/team-b", true),  // healthy, self-sufficient
	}
	if err := sink.writeTeams(context.Background(), Claim{Unit: Unit{OrgID: "org-1", Provider: gitlabTeamCatalogProvider}}, rows); err != nil {
		t.Fatalf("writeTeams returned an error -- a scoped read failure must not fail the whole batch: %v", err)
	}
	if conn.batch == nil || !conn.batch.sent {
		t.Fatal("expected the batch to be sent (the two healthy rows must still write)")
	}
	if len(conn.batch.appendedIDs) != 2 {
		t.Fatalf("appendedIDs=%v, want exactly the 2 self-sufficient rows", conn.batch.appendedIDs)
	}
	for _, id := range conn.batch.appendedIDs {
		if id == "gl:org/team-a" {
			t.Fatalf("gl:org/team-a depended on the failed roster read -- must NOT have been written: appendedIDs=%v", conn.batch.appendedIDs)
		}
	}
}

// TestWriteTeamsSkipsRosterReadEntirelyWhenAllRowsAreAuthoritative proves
// the scoping half of the fix on the happy path: when every row is
// MembersAuthoritative=true, gitlabExistingTeamRoster is never even called
// -- a pre-existing, unrelated roster-query outage cannot block this write
// at all.
func TestWriteTeamsSkipsRosterReadEntirelyWhenAllRowsAreAuthoritative(t *testing.T) {
	conn := &fakeGitLabWriteTeamsConn{rosterQueryErr: fmt.Errorf("would fail if called")}
	sink := GitLabTeamCatalogClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	rows := []gitlabTeamCatalogTeamRow{
		gitlabWriteTeamsTestRow("gl:org", true),
		gitlabWriteTeamsTestRow("gl:org/team-a", true),
	}
	if err := sink.writeTeams(context.Background(), Claim{Unit: Unit{OrgID: "org-1", Provider: gitlabTeamCatalogProvider}}, rows); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(conn.batch.appendedIDs) != 2 {
		t.Fatalf("appendedIDs=%v, want both rows written (roster read never needed)", conn.batch.appendedIDs)
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
	if conn.batch != nil {
		t.Fatalf("PrepareBatch was called (batch=%+v) -- a non-strict walk failure must make ZERO Sink calls", conn.batch)
	}
}
