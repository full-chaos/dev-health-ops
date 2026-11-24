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
)

// CHAOS-14546 (PR #6930), pm question for the Linear team catalog: a 429 whose
// retry wait does not fit the time left now returns at once with a LIVE
// context, and with the production shared gate every later request of the run
// fails fast with rate_limited. The cells below run the REAL
// LinearTeamCatalogCollector (real handler, real ClickHouse sink) on a fake
// conn and a fake GraphQL doer, fail each request index k of the happy run
// with a 429 (Retry-After 25 s, deadline 2 s, gate armed as in production),
// and assert: nothing is closed for a part whose read failed, and the
// incomplete markers are set.

// tcdlLNRow is one positional row a fake query answers.
type tcdlLNRow []any

type tcdlLNRows struct {
	driver.Rows
	rows  []tcdlLNRow
	index int
}

func (r *tcdlLNRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *tcdlLNRows) Scan(dest ...any) error {
	row := r.rows[r.index]
	if len(dest) != len(row) {
		return fmt.Errorf("tcdlLNRows: want %d dest, got %d", len(row), len(dest))
	}
	for i, target := range dest {
		if scanner, ok := target.(sql.Scanner); ok {
			if err := scanner.Scan(row[i]); err != nil {
				return err
			}
			continue
		}
		value := reflect.ValueOf(target).Elem()
		if row[i] == nil {
			value.Set(reflect.Zero(value.Type()))
			continue
		}
		source := reflect.ValueOf(row[i])
		if !source.Type().AssignableTo(value.Type()) {
			return fmt.Errorf("tcdlLNRows: column %d: %T into %s", i, row[i], value.Type())
		}
		value.Set(source)
	}
	return nil
}

func (r *tcdlLNRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *tcdlLNRows) Columns() []string                { return nil }
func (r *tcdlLNRows) Close() error                     { return nil }
func (r *tcdlLNRows) Err() error                       { return nil }

type tcdlLNInsert struct {
	query string
	rows  [][]any
}

type tcdlLNBatch struct {
	driver.Batch
	conn  *tcdlLNConn
	query string
	rows  [][]any
}

func (b *tcdlLNBatch) Append(args ...any) error {
	b.rows = append(b.rows, append([]any(nil), args...))
	return nil
}

func (b *tcdlLNBatch) Send() error {
	b.conn.mu.Lock()
	defer b.conn.mu.Unlock()
	b.conn.inserts = append(b.conn.inserts, tcdlLNInsert{query: b.query, rows: b.rows})
	return nil
}

func (b *tcdlLNBatch) Abort() error { return nil }

// tcdlLNConn is the fake store: the open rows below are the facts the happy run
// closes; every other read is empty.
type tcdlLNConn struct {
	driver.Conn
	mu             sync.Mutex
	openOwnership  []tcdlLNRow
	openMembership []tcdlLNRow
	droppedTeams   []tcdlLNRow
	inserts        []tcdlLNInsert
	execs          []string
	queries        []string
}

func (c *tcdlLNConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	c.mu.Unlock()
	switch {
	case query == linearOpenOwnershipQuery:
		return &tcdlLNRows{rows: c.openOwnership, index: -1}, nil
	case query == membershipOpenRowsFullQuery:
		return &tcdlLNRows{rows: c.openMembership, index: -1}, nil
	case strings.HasPrefix(query, openMembershipTeamIDsQuery):
		return &tcdlLNRows{rows: c.droppedTeams, index: -1}, nil
	case query == importedTeamIDsQuery:
		// Every team a link names is an imported team.
		var rows []tcdlLNRow
		for _, arg := range args {
			if named, ok := arg.(driver.NamedValue); ok && named.Name == "ids" {
				for _, id := range named.Value.([]string) {
					rows = append(rows, tcdlLNRow{id})
				}
			}
		}
		return &tcdlLNRows{rows: rows, index: -1}, nil
	}
	return &tcdlLNRows{index: -1}, nil
}

func (c *tcdlLNConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &tcdlLNBatch{conn: c, query: query}, nil
}

func (c *tcdlLNConn) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, query)
	return nil
}

// closes lists every row this run wrote with a valid_to: the closes.
func (c *tcdlLNConn) closes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, insert := range c.inserts {
		switch insert.query {
		case linearReferenceMembershipsInsert:
			for _, row := range insert.rows {
				if validTo, ok := row[12].(*time.Time); ok && validTo != nil {
					out = append(out, "membership "+row[2].(string)+"/"+row[3].(string))
				}
			}
		case linearReferenceOwnershipInsert:
			for _, row := range insert.rows {
				if validTo, ok := row[10].(*time.Time); ok && validTo != nil {
					out = append(out, "ownership "+row[2].(string)+"/"+row[3].(ProjectID).String())
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// writes counts the rows written per destination table.
func (c *tcdlLNConn) writes() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for _, insert := range c.inserts {
		table := strings.Fields(strings.TrimPrefix(strings.TrimSpace(insert.query), "INSERT INTO "))[0]
		out[table] += len(insert.rows)
	}
	for _, exec := range c.execs {
		if fields := strings.Fields(strings.TrimSpace(exec)); len(fields) > 2 && strings.EqualFold(fields[0], "INSERT") {
			out[fields[2]]++
		}
	}
	return out
}

const tcdlLNEnd = `"pageInfo":{"hasNextPage":false,"endCursor":null}`

// tcdlLNDoer answers the Linear GraphQL requests of one catalog run by the query
// it carries, so a failed request does not shift the answers of the later
// ones. failAt (1-based) is answered 429 with a Retry-After of 25 s.
type tcdlLNDoer struct {
	mu     sync.Mutex
	failAt int
	calls  int
	labels []string
	failed string
}

func tcdlLNLabel(body string) string {
	switch {
	case strings.Contains(body, "LinearReferenceCatalogTeams"):
		return "teams"
	case strings.Contains(body, "LinearReferenceCatalogMembers"):
		return "members-QA-p2"
	case strings.Contains(body, "LinearWorkItemsCycles") && strings.Contains(body, "team-qa"):
		return "cycles-QA"
	case strings.Contains(body, "LinearWorkItemsCycles") && strings.Contains(body, "team-ops"):
		return "cycles-OPS"
	case strings.Contains(body, "LinearReferenceCatalogProjectTeams"):
		return "project-teams-p2"
	case strings.Contains(body, "LinearReferenceCatalogProjects"):
		return "projects"
	case strings.Contains(body, `teams(filter: {key: {eq: \"GONE\"}}`):
		return "lookup-team-GONE"
	case strings.Contains(body, `teams(filter: {key: {eq: \"OLD\"}}`):
		return "lookup-team-OLD"
	case strings.Contains(body, `project(id: \"keep\")`):
		return "lookup-project-keep"
	}
	return "unknown"
}

func tcdlLNAnswer(label string) string {
	switch label {
	case "teams":
		return `{"data":{"teams":{"nodes":[` +
			`{"id":"team-qa","key":"QA","name":"Quality","description":"","members":{"nodes":[` +
			`{"id":"u-alice","name":"Alice","email":"alice@example.com","active":true},` +
			`{"id":"u-bob","name":"Bob","email":null,"active":false}` +
			`],"pageInfo":{"hasNextPage":true,"endCursor":"m1"}}},` +
			`{"id":"team-ops","key":"OPS","name":"Ops","description":"","members":{"nodes":[],` + tcdlLNEnd + `}}` +
			`],` + tcdlLNEnd + `}}}`
	case "members-QA-p2":
		return `{"data":{"team":{"members":{"nodes":[{"id":"u-carol","name":"Carol","email":"carol@example.com","active":true}],` + tcdlLNEnd + `}}}}`
	case "cycles-QA", "cycles-OPS":
		return `{"data":{"cycles":{"nodes":[],` + tcdlLNEnd + `}}}`
	case "projects":
		return `{"data":{"projects":{"nodes":[{"id":"other","name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,` +
			`"teams":{"nodes":[{"id":"team-qa","key":"QA"}],"pageInfo":{"hasNextPage":true,"endCursor":"pt1"}}}],` + tcdlLNEnd + `}}}`
	case "project-teams-p2":
		return `{"data":{"project":{"teams":{"nodes":[{"id":"team-ops","key":"OPS"}],` + tcdlLNEnd + `}}}}`
	case "lookup-team-GONE", "lookup-team-OLD":
		return linearRecordedTeamsNotFound
	case "lookup-project-keep":
		return linearRecordedProjectNotFound
	}
	return `{"errors":[{"message":"unexpected"}],"data":null}`
}

func (d *tcdlLNDoer) Do(request *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	label := tcdlLNLabel(string(raw))
	d.mu.Lock()
	d.calls++
	call := d.calls
	d.labels = append(d.labels, label)
	d.mu.Unlock()
	if call == d.failAt {
		d.mu.Lock()
		d.failed = label
		d.mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests", Request: request,
			Header: http.Header{"Retry-After": {"25"}, "Content-Type": {"application/json"}},
			Body:   http.NoBody,
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Request: request,
		Header: http.Header{"Content-Type": {"application/json"}},
		Body:   io.NopCloser(strings.NewReader(tcdlLNAnswer(label))),
	}, nil
}

func tcdlLNStore(t *testing.T, org string) *tcdlLNConn {
	t.Helper()
	seededAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	keep := mustProjectID(t)(LinearProjectID("keep"))
	old := mustProjectID(t)(LinearTeamKeyProjectID(org, "OLD"))
	oldKey := "OLD"
	member := func(team, memberID string) tcdlLNRow {
		return tcdlLNRow{org, "linear", team, memberID, (*string)(nil), (*string)(nil), []string{}, "native",
			uint8(1), uint16(100), int32(10), seededAt, seededAt, (*string)(nil)}
	}
	return &tcdlLNConn{
		openOwnership: []tcdlLNRow{
			{"linear:QA", keep.String(), (*string)(nil), "native", uint8(1), uint16(100), int32(10), seededAt},
			{"linear:OLD", old.String(), &oldKey, "native", uint8(1), uint16(100), int32(10), seededAt},
		},
		openMembership: []tcdlLNRow{
			member("linear:QA", "linear:u-bob"),
			member("linear:GONE", "linear:u-dave"),
		},
		droppedTeams: []tcdlLNRow{{"linear:GONE"}},
	}
}

type tcdlLNRun struct {
	labels  []string
	failed  string
	err     error
	result  TeamCatalogResult
	closes  []string
	writes  map[string]int
	reasons []string
}

func tcdlLNCollect(t *testing.T, strict bool, failAt int, wait, deadline time.Duration) tcdlLNRun {
	t.Helper()
	claim := nativeTestClaim("linear", "work-items")
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = strict
	conn := tcdlLNStore(t, claim.OrgID)
	doer := &tcdlLNDoer{failAt: failAt}
	client := sweepClient(t, "linear", "https://api.linear.app", doer, wait)
	client.Gate = &retryDeadlineGate{}
	collector := LinearTeamCatalogCollector{
		ScopeCensus: staticScopeCensus{},
		Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
		Sink: LinearReferenceCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	result, err := collector.CollectTeamCatalog(ctx, ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}, client,
		TeamCatalogSelections{Teams: true, Members: true, Projects: true}, time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC))
	run := tcdlLNRun{labels: doer.labels, failed: doer.failed, err: err, result: result, closes: conn.closes(), writes: conn.writes()}
	for _, leg := range result.DegradedLegs {
		run.reasons = append(run.reasons, leg.Dataset+":"+leg.Reason)
	}
	return run
}

// tcdlLNCloseNeeds is, for each close of the happy run, every request whose
// answer the close stands on.
var tcdlLNCloseNeeds = map[string][]string{
	"membership linear:QA/linear:u-bob":    {"teams", "members-QA-p2"},
	"membership linear:GONE/linear:u-dave": {"teams", "members-QA-p2", "lookup-team-GONE"},
	"ownership linear:OLD/":                {"teams", "lookup-team-OLD"},
	"ownership linear:QA/keep":             {"teams", "projects", "project-teams-p2", "lookup-project-keep"},
}

func tcdlLNNeeds(close string) []string {
	for key, needs := range tcdlLNCloseNeeds {
		if strings.HasPrefix(close, key) {
			return needs
		}
	}
	return nil
}

func tcdlLNErrClass(err error) string {
	if err == nil {
		return "none"
	}
	class := "other"
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		class = string(providerErr.Class)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		class += "+deadline"
	}
	return class
}

// TestLinearTeamCatalogRateLimitPastTheDeadlineClosesNothingUnread sweeps every
// request index k of the happy run, strict and non-strict, failing request k
// with a 429 whose wait does not fit the deadline, with the shared gate armed.
func TestLinearTeamCatalogRateLimitPastTheDeadlineClosesNothingUnread(t *testing.T) {
	for _, strict := range []bool{true, false} {
		happy := tcdlLNCollect(t, strict, -1, time.Millisecond, 10*time.Second)
		if happy.err != nil {
			t.Fatalf("strict=%v happy run: %v", strict, happy.err)
		}
		// The control: the happy run closes every seeded fact, so a run below
		// that closes nothing is the guard's refusal, not a harness that cannot close.
		if len(happy.closes) != len(tcdlLNCloseNeeds) {
			t.Fatalf("strict=%v happy closes = %v, want one per seeded fact (%d)", strict, happy.closes, len(tcdlLNCloseNeeds))
		}
		for _, close := range happy.closes {
			if tcdlLNNeeds(close) == nil {
				t.Fatalf("strict=%v happy close %q has no stated needs", strict, close)
			}
		}
		t.Logf("strict=%v happy: requests=%v closes=%v writes=%v", strict, happy.labels, happy.closes, happy.writes)
		for k := 1; k <= len(happy.labels); k++ {
			run := tcdlLNCollect(t, strict, k, 25*time.Second, 2*time.Second)
			gated := max(len(run.labels)-k, 0)
			t.Logf("strict=%v k=%d failed=%s err=%s partial_writes=%v closes=%v snapshot_incomplete=%v legs=%v later_requests=%d",
				strict, k, run.failed, tcdlLNErrClass(run.err), run.writes, run.closes, run.result.OwnershipSnapshotIncomplete, run.reasons, gated)
			if run.failed == "" {
				t.Errorf("strict=%v k=%d: no request was failed", strict, k)
				continue
			}
			if len(run.labels) > k {
				t.Errorf("strict=%v k=%d: %d request(s) reached the provider after the 429 armed the gate: %v",
					strict, k, len(run.labels)-k, run.labels[k:])
			}
			for _, close := range run.closes {
				for _, need := range tcdlLNNeeds(close) {
					if need == run.failed {
						t.Errorf("P1 strict=%v k=%d: %q closed though its read %q failed", strict, k, close, run.failed)
					}
				}
				if tcdlLNNeeds(close) == nil {
					t.Errorf("P1 strict=%v k=%d: close %q is not a close of the happy run", strict, k, close)
				}
			}
			// Every request after k is refused by the gate: no close that needs a
			// later answer may happen either.
			for _, close := range run.closes {
				for _, need := range tcdlLNNeeds(close) {
					for index, label := range happy.labels {
						if label == need && index+1 > k {
							t.Errorf("P1 strict=%v k=%d: %q closed though %q (request %d) came after the gate closed", strict, k, close, need, index+1)
						}
					}
				}
			}
			if run.err == nil {
				switch run.failed {
				case "projects", "project-teams-p2":
					if !run.result.OwnershipSnapshotIncomplete {
						t.Errorf("strict=%v k=%d: projects read failed, OwnershipSnapshotIncomplete not set", strict, k)
					}
				case "lookup-team-GONE":
					found := false
					for _, reason := range run.reasons {
						found = found || reason == "team_memberships:"+TeamAbsenceNotProven
					}
					if !found {
						t.Errorf("strict=%v k=%d: GONE lookup failed, no %s leg: %v", strict, k, TeamAbsenceNotProven, run.reasons)
					}
				}
			}
		}
	}
}
