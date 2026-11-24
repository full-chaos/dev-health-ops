package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider/teamprovidertest"
)

// The GitLab team catalog under a 429 whose retry wait is past the deadline,
// with the shared gate wired as in production: the k-th request is answered
// 429 (Retry-After 25 s, context 5 s), the gate refuses every later request
// with rate_limited, and the collector runs on with a live context. The fake
// store holds open rows that a run CAN close (a departed member and a removed
// grant in each listed group, and a dropped group with open rows), so the happy
// run closes them; a faulted run must close nothing for a part whose read
// failed and must mark an unread project target as not read.

const (
	tcdlGLOrg      = "org-1"
	tcdlGLRoot     = "gl:org"
	tcdlGLTeamA    = "gl:org/team-a"
	tcdlGLGone     = "gl:org/gone-team"
	tcdlGLRootPath = "/api/v4/groups/org"
	tcdlGLTeamPath = "/api/v4/groups/org%2Fteam-a"
)

// tcdlGLCall is one request that reached the doer.
type tcdlGLCall struct {
	index  int
	path   string
	lookup string // the member asked by a membership lookup (?query=)
	all    bool   // the include_subgroups project listing
	status int
}

type tcdlGLDoer struct {
	mu     sync.Mutex
	failAt int
	calls  []tcdlGLCall
}

func tcdlGLResponse(request *http.Request, status int, header http.Header, body string) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Request: request, Header: header,
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func (d *tcdlGLDoer) Do(request *http.Request) (*http.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	call := tcdlGLCall{index: len(d.calls) + 1, path: request.URL.EscapedPath(), lookup: request.URL.Query().Get("query"),
		all: request.URL.Query().Get("include_subgroups") == "true"}
	if call.index == d.failAt {
		call.status = http.StatusTooManyRequests
		d.calls = append(d.calls, call)
		return tcdlGLResponse(request, call.status, http.Header{"Retry-After": {"25"}}, `{"message":"429 Too Many Requests"}`), nil
	}
	list := http.Header{"X-Next-Page": {""}}
	body, header, status := "", list, http.StatusOK
	switch {
	case call.path == tcdlGLRootPath:
		body, header = `{"id":1,"full_path":"org","name":"Org"}`, nil
	case call.path == tcdlGLRootPath+"/subgroups":
		body = `[{"id":2,"full_path":"org/team-a","name":"Team A"}]`
	case call.path == tcdlGLRootPath+"/projects" && call.all:
		body = `[{"id":100,"path_with_namespace":"org/root-svc","name":"root-svc","archived":false},` +
			`{"id":101,"path_with_namespace":"org/team-a/svc","name":"svc","archived":false}]`
	case call.path == tcdlGLRootPath+"/projects":
		body = `[{"id":100,"path_with_namespace":"org/root-svc","name":"root-svc","archived":false}]`
	case call.path == tcdlGLTeamPath+"/projects":
		body = `[{"id":101,"path_with_namespace":"org/team-a/svc","name":"svc","archived":false}]`
	case (call.path == tcdlGLRootPath+"/members" || call.path == tcdlGLTeamPath+"/members") && call.lookup != "":
		body = `[]` // the asked member is not in the group, by GitLab's own end signal
	case call.path == tcdlGLRootPath+"/members":
		body = `[{"id":11,"username":"root-owner","name":"root-owner"}]`
	case call.path == tcdlGLTeamPath+"/members":
		body = `[{"id":12,"username":"alice","name":"alice","email":"alice@example.com"}]`
	case call.path == "/api/v4/version":
		body, header = `{"version":"17.2.0"}`, nil
	case call.path == "/api/v4/projects/100/milestones":
		body = `[{"id":7100,"title":"M","state":"active","due_date":"2026-12-01"}]`
	case call.path == "/api/v4/projects/101/milestones":
		body = `[{"id":7101,"title":"M","state":"active","due_date":"2026-12-01"}]`
	default:
		status, body, header = http.StatusNotFound, `{"message":"404 Not Found"}`, nil
	}
	call.status = status
	d.calls = append(d.calls, call)
	return tcdlGLResponse(request, status, header, body), nil
}

func (d *tcdlGLDoer) snapshot() []tcdlGLCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]tcdlGLCall(nil), d.calls...)
}

// tcdlGLRows is a driver.Rows over typed values in scan order.
type tcdlGLRows struct {
	driver.Rows
	rows  [][]any
	index int
}

func (r *tcdlGLRows) Next() bool                       { r.index++; return r.index < len(r.rows) }
func (r *tcdlGLRows) Close() error                     { return nil }
func (r *tcdlGLRows) Err() error                       { return nil }
func (r *tcdlGLRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *tcdlGLRows) Columns() []string                { return nil }
func (r *tcdlGLRows) Totals(...any) error              { return nil }
func (r *tcdlGLRows) ScanStruct(any) error             { return errors.New("tcdlGLRows: ScanStruct") }
func newTCDLRows(rows [][]any) *tcdlGLRows             { return &tcdlGLRows{rows: rows, index: -1} }
func (r *tcdlGLRows) Scan(dest ...any) error {
	row := r.rows[r.index]
	if len(dest) != len(row) {
		return fmt.Errorf("tcdlGLRows: %d dest for %d values", len(dest), len(row))
	}
	for i, value := range row {
		if err := tcdlGLAssign(dest[i], value); err != nil {
			return fmt.Errorf("tcdlGLRows: column %d: %w", i, err)
		}
	}
	return nil
}

func tcdlGLAssign(dest, value any) error {
	switch d := dest.(type) {
	case *string:
		*d = value.(string)
	case **string:
		if value == nil {
			*d = nil
		} else {
			*d = value.(*string)
		}
	case *[]string:
		*d = value.([]string)
	case *uint8:
		*d = value.(uint8)
	case *uint16:
		*d = value.(uint16)
	case *int32:
		*d = value.(int32)
	case *time.Time:
		*d = value.(time.Time)
	case **time.Time:
		if value == nil {
			*d = nil
		} else {
			*d = value.(*time.Time)
		}
	case *ProjectID:
		*d = value.(ProjectID)
	default:
		return fmt.Errorf("unsupported dest %T", dest)
	}
	return nil
}

// tcdlGLStatement is one write the store saw.
type tcdlGLStatement struct {
	statement string
	rows      [][]any
	teams     []teamprovider.Write
}

type tcdlGLConn struct {
	driver.Conn
	mu          sync.Mutex
	memberships [][]any // open team_memberships rows, membershipOpenRowsFullQuery order
	ownership   [][]any // open provider_access rows, openProviderAccessOwnership order
	writes      []tcdlGLStatement
	queries     []string
}

func tcdlGLNewConn(at time.Time) *tcdlGLConn {
	seen := at.AddDate(0, -10, 0)
	member := func(teamID, memberID string) []any {
		username := strings.TrimPrefix(memberID, "gl:")
		raw := "gitlab:" + username
		return []any{tcdlGLOrg, gitlabTeamCatalogProvider, teamID, memberID, &raw, nil, []string{raw},
			gitlabTeamCatalogSource, uint8(0), uint16(gitlabTeamCatalogBaseSpecificity), int32(gitlabTeamCatalogProviderAccessPriority), seen, seen, nil}
	}
	grant := func(teamID, nativeID, path string) []any {
		id, _ := GitLabCatalogProjectID(tcdlGLOrg, nativeID)
		key := path
		return []any{teamID, id, &key, gitlabTeamCatalogSource, uint8(0), uint16(gitlabTeamCatalogBaseSpecificity), int32(gitlabTeamCatalogProviderAccessPriority), seen, nil}
	}
	return &tcdlGLConn{
		memberships: [][]any{member(tcdlGLRoot, "gl:departed-root"), member(tcdlGLTeamA, "gl:departed-a"), member(tcdlGLGone, "gl:gone-member")},
		ownership:   [][]any{grant(tcdlGLRoot, "900", "org/old-svc"), grant(tcdlGLTeamA, "901", "org/team-a/old"), grant(tcdlGLGone, "902", "org/gone-team/svc")},
	}
}

func tcdlGLNotListed(rows [][]any, column int, listed any) [][]any {
	names, _ := listed.([]string)
	skip := map[string]bool{}
	for _, name := range names {
		skip[name] = true
	}
	seen := map[string]bool{}
	var out [][]any
	for _, row := range rows {
		team := row[column].(string)
		if !skip[team] && !seen[team] {
			seen[team] = true
			out = append(out, []any{team})
		}
	}
	return out
}

func (c *tcdlGLConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
	last := func() any {
		if len(args) == 0 {
			return nil
		}
		return args[len(args)-1]
	}
	switch {
	case query == membershipOpenRowsFullQuery:
		return newTCDLRows(c.memberships), nil
	case strings.HasPrefix(query, openMembershipTeamIDsQuery):
		if query == openMembershipTeamIDsQuery {
			return newTCDLRows(tcdlGLNotListed(c.memberships, 2, nil)), nil
		}
		return newTCDLRows(tcdlGLNotListed(c.memberships, 2, last())), nil
	case strings.HasPrefix(query, gitlabOwnershipTeamsQuery):
		if query == gitlabOwnershipTeamsQuery {
			return newTCDLRows(tcdlGLNotListed(c.ownership, 0, nil)), nil
		}
		return newTCDLRows(tcdlGLNotListed(c.ownership, 0, last())), nil
	case strings.Contains(query, "SELECT team_id, project_id, project_key, toString(source)"):
		teams, _ := last().([]string)
		want := map[string]bool{}
		for _, team := range teams {
			want[team] = true
		}
		var out [][]any
		for _, row := range c.ownership {
			if want[row[0].(string)] {
				out = append(out, row)
			}
		}
		return newTCDLRows(out), nil
	case query == importedTeamIDsQuery:
		var out [][]any
		for _, arg := range args {
			if named, ok := arg.(driver.NamedValue); ok && named.Name == "ids" {
				for _, id := range named.Value.([]string) {
					out = append(out, []any{id})
				}
			}
		}
		return newTCDLRows(out), nil
	default:
		return newTCDLRows(nil), nil
	}
}

func (c *tcdlGLConn) Exec(_ context.Context, statement string, args ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	write := tcdlGLStatement{statement: statement}
	if teamprovidertest.IsInsert(statement) {
		_, writes, err := teamprovidertest.Decode(statement, args)
		if err != nil {
			return err
		}
		write.teams = writes
	}
	c.writes = append(c.writes, write)
	return nil
}

func (c *tcdlGLConn) PrepareBatch(_ context.Context, statement string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &tcdlGLBatch{conn: c, statement: statement}, nil
}

type tcdlGLBatch struct {
	driver.Batch
	conn      *tcdlGLConn
	statement string
	rows      [][]any
	sent      bool
}

func (b *tcdlGLBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}
func (b *tcdlGLBatch) Send() error {
	b.conn.mu.Lock()
	defer b.conn.mu.Unlock()
	b.sent = true
	b.conn.writes = append(b.conn.writes, tcdlGLStatement{statement: b.statement, rows: b.rows})
	return nil
}
func (b *tcdlGLBatch) Abort() error                  { return nil }
func (b *tcdlGLBatch) Flush() error                  { return nil }
func (b *tcdlGLBatch) IsSent() bool                  { return b.sent }
func (b *tcdlGLBatch) Rows() int                     { return len(b.rows) }
func (b *tcdlGLBatch) Columns() []column.Interface   { return nil }
func (b *tcdlGLBatch) Column(int) driver.BatchColumn { panic("tcdlGLBatch: Column") }
func (b *tcdlGLBatch) AppendStruct(any) error        { panic("tcdlGLBatch: AppendStruct") }

type tcdlGLCensus struct{}

func (tcdlGLCensus) CountActiveSiblingIntegrations(context.Context, string, string, string) (int, error) {
	return 0, nil
}

// tcdlGLLog keeps the messages of the run's log lines.
type tcdlGLLog struct {
	mu       sync.Mutex
	messages []string
}

func (h *tcdlGLLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *tcdlGLLog) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, record.Message)
	return nil
}
func (h *tcdlGLLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *tcdlGLLog) WithGroup(string) slog.Handler      { return h }
func (h *tcdlGLLog) has(message string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.messages {
		if m == message {
			return true
		}
	}
	return false
}

// tcdlGLRun is what one collector run did.
type tcdlGLRun struct {
	calls     []tcdlGLCall
	result    TeamCatalogResult
	err       error
	conn      *tcdlGLConn
	log       *tcdlGLLog
	closes    []string // "membership team member" / "ownership team project"
	retires   []string // teams gone_at stated or is_active 0, catalog-evidence carry rows
	targets   map[string]string
	writes    int
	unknownWr []string
}

// tcdlGLGateless runs the sweep without the shared gate: the team-catalog
// clients are built without one (workerservice/team_catalog_clients.go:298-310).
var tcdlGLGateless bool

// tcdlGLStrict runs the sweep as the strict discovery path (Ref.Strict).
var tcdlGLStrict bool

func tcdlGLCollect(t *testing.T, selections TeamCatalogSelections, failAt int) tcdlGLRun {
	t.Helper()
	at := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	doer := &tcdlGLDoer{failAt: failAt}
	conn := tcdlGLNewConn(at)
	logs := &tcdlGLLog{}
	previous := slog.Default()
	slog.SetDefault(slog.New(logs))
	defer slog.SetDefault(previous)
	collector := GitLabTeamCatalogCollector{
		Handler:     GitLabTeamCatalogRouteHandler{},
		Sink:        GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })},
		ScopeCensus: tcdlGLCensus{},
	}
	ref := TeamCatalogReference{OrgID: tcdlGLOrg, SyncRunID: "run-1", IntegrationID: "integration-1", Strict: tcdlGLStrict}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var gate providerfoundation.BackoffGate
	if !tcdlGLGateless {
		gate = &retryDeadlineGate{}
	}
	client := retryDeadlineGitLabClient(t, doer, gate, 5*time.Millisecond)
	result, err := collector.CollectTeamCatalog(ctx, ref, credential, client, selections, at)
	run := tcdlGLRun{calls: doer.snapshot(), result: result, err: err, conn: conn, log: logs, targets: map[string]string{}}
	targetState := projectsInsertPositions["target_state"]
	for _, write := range conn.writes {
		run.writes++
		switch {
		case write.statement == gitlabTeamCatalogMembershipsInsert:
			for _, row := range write.rows {
				if to, _ := row[12].(*time.Time); to != nil {
					run.closes = append(run.closes, "membership "+row[2].(string)+" "+row[3].(string))
				}
			}
		case write.statement == gitlabTeamCatalogOwnershipInsert:
			for _, row := range write.rows {
				if to, _ := row[10].(*time.Time); to != nil {
					run.closes = append(run.closes, "ownership "+row[2].(string)+" "+row[3].(ProjectID).String())
				}
			}
		case write.statement == projectsCatalogInsert:
			for _, row := range write.rows {
				run.targets[row[0].(string)] = fmt.Sprint(row[targetState])
				if active, _ := row[5].(uint8); active == 0 {
					run.retires = append(run.retires, "project inactive "+row[0].(string))
				}
			}
		case write.teams != nil:
			for _, team := range write.teams {
				if team.Gone == teamprovider.GoneStated {
					run.retires = append(run.retires, fmt.Sprint("team gone_at ", team.Values[0]))
				}
				if active, _ := team.Values[7].(uint8); active == 0 {
					run.retires = append(run.retires, fmt.Sprint("team inactive ", team.Values[0]))
				}
			}
		case strings.Contains(write.statement, "team_drift_changes") || strings.Contains(write.statement, "team_provider_observations"):
		default:
			run.unknownWr = append(run.unknownWr, write.statement)
		}
	}
	sort.Strings(run.closes)
	return run
}

// tcdlGLRead reports whether a request for path (lookup = member asked, all =
// the include_subgroups listing) reached the provider and was answered 200.
func (run tcdlGLRun) read(path, lookup string, all bool) bool {
	for _, call := range run.calls {
		if call.path == path && call.lookup == lookup && call.all == all && call.status == http.StatusOK {
			return true
		}
	}
	return false
}

func tcdlGLGroupPath(teamID string) string {
	switch teamID {
	case tcdlGLRoot:
		return tcdlGLRootPath
	case tcdlGLTeamA:
		return tcdlGLTeamPath
	}
	return ""
}

func tcdlGLErrClass(err error) string {
	if err == nil {
		return "nil"
	}
	text := "other"
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		text = string(providerErr.Class)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		text += "+deadline"
	}
	return text
}

// tcdlGLViolations is every close, retire or complete marker of the run that a
// failed read does not allow.
func tcdlGLViolations(run tcdlGLRun, selections TeamCatalogSelections) []string {
	var bad []string
	subgroupsRead := run.read(tcdlGLRootPath+"/subgroups", "", false)
	for _, closed := range run.closes {
		fields := strings.Fields(closed)
		kind, team, subject := fields[0], fields[1], fields[2]
		if team == tcdlGLGone {
			if !subgroupsRead {
				bad = append(bad, closed+" (dropped team closed, subgroup listing not read)")
			}
			continue
		}
		switch kind {
		case "membership":
			if !run.read(tcdlGLGroupPath(team)+"/members", "", false) {
				bad = append(bad, closed+" (member list of the group not read)")
			} else if !run.read(tcdlGLGroupPath(team)+"/members", strings.TrimPrefix(subject, "gl:"), false) {
				bad = append(bad, closed+" (member lookup not answered)")
			}
		case "ownership":
			if !run.read(tcdlGLGroupPath(team)+"/projects", "", false) {
				bad = append(bad, closed+" (project list of the group not read)")
			}
		}
	}
	bad = append(bad, run.retires...)
	for id, state := range run.targets {
		native := id[strings.LastIndex(id, ":")+1:]
		read := run.read("/api/v4/projects/"+native+"/milestones", "", false)
		if (state == projectTargetStated || state == projectTargetNoneDate) && !read {
			bad = append(bad, "project "+id+" target_state "+state+" (milestones not read)")
		}
		if read && state != projectTargetStated {
			bad = append(bad, "project "+id+" target_state "+state+" (milestones read)")
		}
	}
	if run.result.Skipped && run.writes > 0 {
		bad = append(bad, fmt.Sprintf("skipped run wrote %d statements", run.writes))
	}
	if run.err == nil && !run.result.Skipped && selections.Members {
		for _, team := range []string{tcdlGLRoot, tcdlGLTeamA} {
			if !run.read(tcdlGLGroupPath(team)+"/members", "", false) && !run.log.has("gitlab_team_catalog_member_fetch_failed") {
				bad = append(bad, team+" member list not read and no member_fetch_failed marker")
			}
		}
	}
	for _, statement := range run.unknownWr {
		bad = append(bad, "unknown write: "+strings.SplitN(statement, "(", 2)[0])
	}
	return bad
}

// TestGitLabTeamCatalogRateLimitPastDeadlineUnderGateClosesNothingUnread runs
// every selection set: the happy run (control: it closes every seeded open
// row and states both targets), then each request index k answered 429 past
// the deadline with the shared gate.
func TestGitLabTeamCatalogRateLimitPastDeadlineUnderGateClosesNothingUnread(t *testing.T) {
	tcdlGLSweep(t)
}

// The same sweep without a gate, as the catalog path runs in production: every
// later request is answered.
func TestGitLabTeamCatalogRateLimitPastDeadlineGatelessClosesNothingUnread(t *testing.T) {
	tcdlGLGateless = true
	defer func() { tcdlGLGateless = false }()
	tcdlGLSweep(t)
}

// Strict (the reference-discovery path): a failed read returns the error and the
// sink is never called, so a run that errors writes nothing.
func TestGitLabTeamCatalogRateLimitPastDeadlineStrictGatelessClosesNothingUnread(t *testing.T) {
	tcdlGLGateless, tcdlGLStrict = true, true
	defer func() { tcdlGLGateless, tcdlGLStrict = false, false }()
	tcdlGLSweep(t)
}

func tcdlGLSweep(t *testing.T) {
	var sets []TeamCatalogSelections
	for mask := 1; mask < 8; mask++ {
		sets = append(sets, TeamCatalogSelections{Teams: mask&1 != 0, Projects: mask&2 != 0, Members: mask&4 != 0})
	}
	for _, selections := range sets {
		name := fmt.Sprintf("T%v_P%v_M%v", selections.Teams, selections.Projects, selections.Members)
		happy := tcdlGLCollect(t, selections, -1)
		if happy.err != nil || happy.result.Skipped {
			t.Fatalf("%s happy: err=%v result=%+v", name, happy.err, happy.result)
		}
		var want []string
		if selections.Members {
			want = append(want, "membership gl:org gl:departed-root", "membership gl:org/gone-team gl:gone-member", "membership gl:org/team-a gl:departed-a")
		}
		if selections.Projects {
			for team, native := range map[string]string{tcdlGLRoot: "900", tcdlGLTeamA: "901", tcdlGLGone: "902"} {
				id, _ := GitLabCatalogProjectID(tcdlGLOrg, native)
				want = append(want, "ownership "+team+" "+id.String())
			}
			for _, native := range []string{"100", "101"} {
				id, _ := GitLabCatalogProjectID(tcdlGLOrg, native)
				if happy.targets[id.String()] != projectTargetStated {
					t.Fatalf("%s happy: project %s target_state %q, want stated (control)", name, native, happy.targets[id.String()])
				}
			}
		}
		sort.Strings(want)
		if strings.Join(happy.closes, "|") != strings.Join(want, "|") {
			t.Fatalf("%s happy closes = %v, want %v (control: the harness must see every close)", name, happy.closes, want)
		}
		if bad := tcdlGLViolations(happy, selections); len(bad) > 0 {
			t.Fatalf("%s happy: %v", name, bad)
		}
		t.Logf("%s happy: %d requests, closes=%d", name, len(happy.calls), len(happy.closes))
		for k := 1; k <= len(happy.calls); k++ {
			run := tcdlGLCollect(t, selections, k)
			// Bound (named limit, no gate): after the 429 the run sends no more
			// than the planned requests and never repeats a request beyond what
			// the happy run does.
			if len(run.calls) > len(happy.calls) {
				t.Errorf("%s k=%d: %d requests, the happy run plans %d", name, k, len(run.calls), len(happy.calls))
			}
			happyCount, runCount := map[string]int{}, map[string]int{}
			key := func(call tcdlGLCall) string { return fmt.Sprint(call.path, "|", call.lookup, "|", call.all) }
			for _, call := range happy.calls {
				happyCount[key(call)]++
			}
			for _, call := range run.calls {
				if runCount[key(call)]++; runCount[key(call)] > happyCount[key(call)] {
					t.Errorf("%s k=%d: %s requested %d times, the happy run %d: a retry loop", name, k, key(call), runCount[key(call)], happyCount[key(call)])
				}
			}
			failed := "?"
			if k <= len(run.calls) {
				call := run.calls[k-1]
				failed = call.path
				if call.lookup != "" {
					failed += "?query=" + call.lookup
				}
				if call.all {
					failed += "?include_subgroups"
				}
			}
			targets := make([]string, 0, len(run.targets))
			for id, state := range run.targets {
				targets = append(targets, id[strings.LastIndex(id, ":")+1:]+"="+state)
			}
			sort.Strings(targets)
			t.Logf("%s k=%02d 429 %s | err=%s skipped=%v(%s) reached=%d writes=%d degraded=%d closes=%v targets=%v",
				name, k, failed, tcdlGLErrClass(run.err), run.result.Skipped, run.result.SkipReason, len(run.calls), run.writes,
				len(run.result.DegradedLegs), run.closes, targets)
			if bad := tcdlGLViolations(run, selections); len(bad) > 0 {
				t.Errorf("P1 %s k=%d (429 at %s): %v", name, k, failed, bad)
			}
		}
	}
}
