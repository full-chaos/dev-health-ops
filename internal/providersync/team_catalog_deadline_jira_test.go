package providersync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The happy walk of the Jira team catalog, request by request (doer order).
const (
	jiraDeadlineLiveP1     = "/rest/api/3/project/search?maxResults=100"
	jiraDeadlineLiveP2     = "/rest/api/3/project/search?maxResults=100&startAt=1"
	jiraDeadlineArchived   = "/rest/api/3/project/search?maxResults=100&status=archived"
	jiraDeadlineBodyAAA    = "/rest/api/3/project/AAA"
	jiraDeadlineBoardsAAA  = "/rest/agile/1.0/board?maxResults=100&projectKeyOrId=AAA&startAt=0"
	jiraDeadlineSprints1   = "/rest/agile/1.0/board/1/sprint?maxResults=100&startAt=0"
	jiraDeadlineBodyBBB    = "/rest/api/3/project/BBB"
	jiraDeadlineLookupGone = "/rest/api/3/project/search?id=30001&maxResults=50&status=live&status=archived"
)

var jiraDeadlineAnswers = map[string]string{
	jiraDeadlineLiveP1:     `{"values":[{"id":"10001","key":"AAA","name":"A"}],"isLast":false}`,
	jiraDeadlineLiveP2:     `{"values":[{"id":"10002","key":"BBB","name":"B"}],"isLast":true}`,
	jiraDeadlineArchived:   `{"values":[{"id":"20001","key":"OLD","name":"Old"}],"isLast":true}`,
	jiraDeadlineBodyAAA:    `{"projectTypeKey":"software","versions":[{"id":"1","name":"v1","released":false,"archived":false,"releaseDate":"2026-09-15"}]}`,
	jiraDeadlineBoardsAAA:  `{"values":[{"id":1}],"isLast":true}`,
	jiraDeadlineSprints1:   `{"values":[{"id":501,"name":"Sprint 1","state":"active"}],"isLast":true}`,
	jiraDeadlineBodyBBB:    `{"projectTypeKey":"business","versions":[{"id":"2","name":"v2","released":false,"archived":false,"releaseDate":"2026-10-01"}]}`,
	jiraDeadlineLookupGone: `{"values":[],"isLast":true}`,
}

// The search reads behind the ownership snapshot (the live read is read twice).
var jiraDeadlineSearchURIs = []string{jiraDeadlineLiveP1, jiraDeadlineLiveP2, jiraDeadlineArchived}

// jiraDeadlineDoer answers the happy walk; call number failAt gets a 429 with
// Retry-After 25 (far past the deadline). answered counts the 200 answers per URI.
type jiraDeadlineDoer struct {
	failAt     int
	mu         sync.Mutex
	calls      int
	order      []string
	failedURI  string
	answered   map[string]int
	unexpected []string
}

func (d *jiraDeadlineDoer) Do(request *http.Request) (*http.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	uri := request.URL.RequestURI()
	d.order = append(d.order, uri)
	header := http.Header{"Content-Type": {"application/json"}}
	if d.calls == d.failAt {
		d.failedURI = uri
		header.Set("Retry-After", "25")
		return &http.Response{StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests", Header: header,
			Body: io.NopCloser(strings.NewReader(`{"errorMessages":["rate limited"]}`)), Request: request}, nil
	}
	body, ok := jiraDeadlineAnswers[uri]
	if !ok {
		d.unexpected = append(d.unexpected, uri)
		return &http.Response{StatusCode: http.StatusNotFound, Status: "404 Not Found", Header: header,
			Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
	}
	if d.answered == nil {
		d.answered = map[string]int{}
	}
	d.answered[uri]++
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// --- fake ClickHouse conn: records every read and write ---

type jiraDeadlineRows struct {
	driver.Rows
	rows  [][]any
	index int
}

func jiraDeadlineAssign(dest []any, values []any) error {
	for i, target := range dest {
		var value any
		if i < len(values) {
			value = values[i]
		}
		if scanner, ok := target.(sql.Scanner); ok {
			if err := scanner.Scan(value); err != nil {
				return err
			}
			continue
		}
		slot := reflect.ValueOf(target).Elem()
		if value == nil {
			slot.Set(reflect.Zero(slot.Type()))
			continue
		}
		v := reflect.ValueOf(value)
		switch {
		case v.Type().AssignableTo(slot.Type()):
			slot.Set(v)
		case v.Type().ConvertibleTo(slot.Type()):
			slot.Set(v.Convert(slot.Type()))
		default:
			return fmt.Errorf("jiraDeadlineAssign: %T into %s", value, slot.Type())
		}
	}
	return nil
}

func (r *jiraDeadlineRows) Next() bool             { r.index++; return r.index < len(r.rows) }
func (r *jiraDeadlineRows) Scan(dest ...any) error { return jiraDeadlineAssign(dest, r.rows[r.index]) }
func (r *jiraDeadlineRows) Close() error           { return nil }
func (r *jiraDeadlineRows) Err() error             { return nil }

// ColumnTypes: the stored-version read of the projects contract; the store
// holds no projects row, so the read returns no row and needs no column.
func (r *jiraDeadlineRows) ColumnTypes() []driver.ColumnType { return nil }

type jiraDeadlineRow struct {
	driver.Row
	values []any
}

func (r jiraDeadlineRow) Err() error             { return nil }
func (r jiraDeadlineRow) Scan(dest ...any) error { return jiraDeadlineAssign(dest, r.values) }

type jiraDeadlineBatch struct {
	driver.Batch
	conn      *jiraDeadlineConn
	statement string
	rows      [][]any
	sent      bool
}

func (b *jiraDeadlineBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}
func (b *jiraDeadlineBatch) Send() error {
	b.sent = true
	b.conn.mu.Lock()
	b.conn.sent = append(b.conn.sent, b)
	b.conn.mu.Unlock()
	return nil
}
func (b *jiraDeadlineBatch) Abort() error { return nil }
func (b *jiraDeadlineBatch) IsSent() bool { return b.sent }
func (b *jiraDeadlineBatch) Rows() int    { return len(b.rows) }
func (b *jiraDeadlineBatch) Flush() error { return nil }

type jiraDeadlineConn struct {
	driver.Conn
	mu      sync.Mutex
	open    [][]any
	legacy  [][]any
	queries []string
	execs   []string
	sent    []*jiraDeadlineBatch
}

func (c *jiraDeadlineConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	c.mu.Unlock()
	switch {
	case query == jiraOpenCatalogOwnershipQuery:
		return &jiraDeadlineRows{rows: c.open, index: -1}, nil
	case strings.Contains(query, "FROM jira_project_ops_team_links"):
		return &jiraDeadlineRows{rows: c.legacy, index: -1}, nil
	default:
		return &jiraDeadlineRows{index: -1}, nil
	}
}

func (c *jiraDeadlineConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	c.mu.Unlock()
	// The project-as-team count read: nothing to retire.
	return jiraDeadlineRow{}
}

func (c *jiraDeadlineConn) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	c.execs = append(c.execs, query)
	c.mu.Unlock()
	return nil
}

func (c *jiraDeadlineConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &jiraDeadlineBatch{conn: c, statement: query}, nil
}

func jiraDeadlinePID(id string) ProjectID {
	pid, _ := JiraProjectID(id)
	return pid
}

// The store: open jira_legacy rows of AAA and BBB (live, linked), of OLD
// (archived) and of GONE (in no answer: the provider's lookup says it is gone).
func newJiraDeadlineConn() *jiraDeadlineConn {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	openRow := func(team, project, key string) []any {
		k := key
		return []any{teamid.Of(jiraTeamCatalogProvider, team), project, &k, jiraTeamCatalogLegacySource,
			uint8(1), uint16(jiraTeamCatalogLegacySpecificity), int32(jiraTeamCatalogLegacyPriority), since}
	}
	return &jiraDeadlineConn{
		open: [][]any{
			openRow("ops-1", "10001", "AAA"),
			openRow("ops-3", "10002", "BBB"),
			openRow("ops-9", "20001", "OLD"),
			openRow("ops-2", "30001", "GONE"),
		},
		legacy: [][]any{{"AAA", "ops-1"}, {"BBB", "ops-3"}},
	}
}

type jiraDeadlineOutcome struct {
	result   TeamCatalogResult
	err      error
	doer     *jiraDeadlineDoer
	conn     *jiraDeadlineConn
	closed   []string          // ownership rows written with valid_to
	inactive []string          // projects rows written with is_active 0
	targets  map[string]string // project id -> target_state written
	sprints  int
	ownWrite int
}

// jiraDeadlineGateless runs the sweep without the shared gate, as the catalog
// path does in production (workerservice/team_catalog_clients.go:298-310).
var jiraDeadlineGateless bool

func runJiraDeadlineCatalog(t *testing.T, failAt int, strict bool) jiraDeadlineOutcome {
	t.Helper()
	doer := &jiraDeadlineDoer{failAt: failAt}
	wait := 25 * time.Second
	if failAt < 0 {
		wait = time.Millisecond
	}
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://jira.example.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: wait, MaxWait: wait},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !jiraDeadlineGateless {
		client.Gate = &retryDeadlineGate{}
	}
	conn := newJiraDeadlineConn()
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	collector := JiraTeamCatalogCollector{
		Sink:        JiraTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
		ScopeCensus: staticScopeCensus{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, runErr := collector.CollectTeamCatalog(ctx,
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", IntegrationID: "int-1", Strict: strict},
		providerfoundation.Credential{Provider: "jira"}, client,
		TeamCatalogSelections{Teams: true, Projects: true, Members: true},
		time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
	if ctx.Err() != nil {
		t.Errorf("k=%d strict=%v: the context died during the run (%v): the run slept a wait", failAt, strict, ctx.Err())
	}
	out := jiraDeadlineOutcome{result: result, err: runErr, doer: doer, conn: conn, targets: map[string]string{}}
	for _, batch := range conn.sent {
		switch {
		case batch.statement == jiraTeamCatalogOwnershipInsert:
			for _, row := range batch.rows {
				out.ownWrite++
				if validTo, _ := row[10].(*time.Time); validTo != nil {
					out.closed = append(out.closed, fmt.Sprintf("%v@%v", row[2], row[3]))
				}
			}
		case batch.statement == projectsCatalogInsert:
			for _, row := range batch.rows {
				id := fmt.Sprint(row[projectsInsertPositions["id"]])
				if fmt.Sprint(row[projectsInsertPositions["is_active"]]) == "0" {
					out.inactive = append(out.inactive, id)
				}
				out.targets[id] = fmt.Sprint(row[projectsInsertPositions["target_state"]])
			}
		case batch.statement == gitHubSprintsInsert:
			out.sprints += len(batch.rows)
		case strings.Contains(batch.statement, "valid_to") || strings.Contains(batch.statement, "is_active"):
			t.Errorf("k=%d: unclassified write %q (%d rows)", failAt, batch.statement, len(batch.rows))
		}
	}
	sort.Strings(out.closed)
	return out
}

func jiraDeadlineClass(err error) string {
	var providerErr *providerfoundation.ProviderError
	switch {
	case err == nil:
		return "none"
	case errors.As(err, &providerErr):
		return fmt.Sprintf("%s(deadline=%v)", providerErr.Class, errors.Is(err, context.DeadlineExceeded))
	default:
		return fmt.Sprintf("plain(deadline=%v): %v", errors.Is(err, context.DeadlineExceeded), err)
	}
}

// TestJiraTeamCatalogRateLimitPastTheDeadlineClosesNothingUnread drives the
// real JiraTeamCatalogCollector (handler + sink on a fake conn) with the
// production-shaped shared gate. Each request k of the happy walk is answered
// 429 with a Retry-After past the deadline: the client returns at once with a
// live context and arms the gate, so every later request is refused. The cell
// asserts, per k: no ownership row is closed and no project is written
// inactive unless every read behind that decision answered 200; a project
// whose body was not read is written with its target not stated; a run whose
// project search did not answer in full marks the ownership snapshot
// incomplete; no retire statement is executed.
func TestJiraTeamCatalogRateLimitPastTheDeadlineClosesNothingUnread(t *testing.T) {
	jiraDeadlineSweep(t)
}

func TestJiraTeamCatalogRateLimitPastTheDeadlineGatelessClosesNothingUnread(t *testing.T) {
	jiraDeadlineGateless = true
	defer func() { jiraDeadlineGateless = false }()
	jiraDeadlineSweep(t)
}

func jiraDeadlineSweep(t *testing.T) {
	happy := runJiraDeadlineCatalog(t, -1, false)
	if happy.err != nil || len(happy.doer.unexpected) > 0 {
		t.Fatalf("happy run: err=%v unexpected=%v", happy.err, happy.doer.unexpected)
	}
	wantOrder := []string{jiraDeadlineLiveP1, jiraDeadlineLiveP2, jiraDeadlineArchived, jiraDeadlineLiveP1, jiraDeadlineLiveP2,
		jiraDeadlineBodyAAA, jiraDeadlineBoardsAAA, jiraDeadlineSprints1, jiraDeadlineBodyBBB, jiraDeadlineLookupGone}
	if !reflect.DeepEqual(happy.doer.order, wantOrder) {
		t.Fatalf("happy order=%v, want %v", happy.doer.order, wantOrder)
	}
	gone := teamid.Of(jiraTeamCatalogProvider, "ops-2") + "@30001"
	if !reflect.DeepEqual(happy.closed, []string{gone}) || happy.result.OwnershipSnapshotIncomplete ||
		!reflect.DeepEqual(happy.inactive, []string{"20001"}) || happy.sprints != 1 ||
		happy.targets["10001"] != projectTargetStated || happy.targets["10002"] != projectTargetStated {
		t.Fatalf("happy run: closed=%v incomplete=%v inactive=%v sprints=%d targets=%v execs=%d, want GONE closed, OLD inactive, 1 sprint, both targets stated",
			happy.closed, happy.result.OwnershipSnapshotIncomplete, happy.inactive, happy.sprints, happy.targets, len(happy.conn.execs))
	}
	t.Logf("happy: requests=%d closed=%v inactive=%v sprints=%d targets=%v incomplete=%v legs=%v execs=%d",
		happy.doer.calls, happy.closed, happy.inactive, happy.sprints, happy.targets, happy.result.OwnershipSnapshotIncomplete,
		happy.result.DegradedLegs, len(happy.conn.execs))

	for _, strict := range []bool{false, true} {
		for k := 1; k <= len(wantOrder); k++ {
			out := runJiraDeadlineCatalog(t, k, strict)
			d := out.doer
			if len(d.unexpected) > 0 {
				t.Errorf("k=%d strict=%v: unexpected requests %v", k, strict, d.unexpected)
			}
			// Every read the walk answered in full, by URI (the live search is
			// read twice: both reads must answer).
			full := func(uri string) bool {
				want := 1
				if uri == jiraDeadlineLiveP1 || uri == jiraDeadlineLiveP2 {
					want = 2
				}
				return d.answered[uri] >= want
			}
			searchFull := true
			for _, uri := range jiraDeadlineSearchURIs {
				searchFull = searchFull && full(uri)
			}
			// 1. Ownership closes: only GONE, and only on a full search + its lookup answered.
			for _, row := range out.closed {
				if row != gone || !searchFull || !full(jiraDeadlineLookupGone) {
					t.Errorf("P1 k=%d strict=%v (failed %s): ownership row %s CLOSED over an unread part (searchFull=%v lookup=%v)",
						k, strict, d.failedURI, row, searchFull, full(jiraDeadlineLookupGone))
				}
			}
			// 2. Projects written inactive: only OLD, only on an answered archived read.
			for _, id := range out.inactive {
				if id != "20001" || !full(jiraDeadlineArchived) {
					t.Errorf("P1 k=%d strict=%v: project %s written inactive without its archived read", k, strict, id)
				}
			}
			// 3. A project whose body was not read keeps its target unread.
			for id, key := range map[string]string{"10001": jiraDeadlineBodyAAA, "10002": jiraDeadlineBodyBBB} {
				state, written := out.targets[id]
				if written && !full(key) && (state == projectTargetStated || state == projectTargetNoneDate) {
					t.Errorf("P1 k=%d strict=%v: project %s target_state=%s with its body unread", k, strict, id, state)
				}
			}
			// 4. No retire statement and no team write.
			if len(out.conn.execs) > 0 {
				t.Errorf("P1 k=%d strict=%v: statements executed: %v", k, strict, out.conn.execs)
			}
			// 5. A partial search marks the ownership snapshot incomplete.
			if out.err == nil && !out.result.Skipped && out.ownWrite+len(out.inactive) > 0 && !searchFull &&
				!out.result.OwnershipSnapshotIncomplete {
				t.Errorf("k=%d strict=%v: search not full but OwnershipSnapshotIncomplete=false", k, strict)
			}
			// 6. Sprints only from a full sprint walk.
			if out.sprints > 0 && !(full(jiraDeadlineBodyAAA) && full(jiraDeadlineBoardsAAA) && full(jiraDeadlineSprints1)) {
				t.Errorf("P1 k=%d strict=%v: %d sprints written from a partial sprint walk", k, strict, out.sprints)
			}
			t.Logf("k=%02d strict=%-5v failed=%-75s err=%s skipped=%v requests=%d closed=%v inactive=%v sprints=%d targets=%v ownRows=%d snapshotIncomplete=%v legs=%v",
				k, strict, d.failedURI, jiraDeadlineClass(out.err), out.result.Skipped, d.calls, out.closed, out.inactive, out.sprints,
				out.targets, out.ownWrite, out.result.OwnershipSnapshotIncomplete, out.result.DegradedLegs)
		}
	}
}
