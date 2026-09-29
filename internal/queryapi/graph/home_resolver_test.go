package graph

// Unit tests for the Home resolver (schema.resolvers.go), CHAOS-6084 /
// CHAOS-7042. This field was a panic stub before this change ("not
// implemented: Home - home") -- there was no coverage to extend, only a
// guaranteed-red starting point. Same convention as
// workunitteamattributions_resolver_test.go: a capturing-and-failing
// ClickHouse fake proves the auth guard fires before any query, and a
// recording Postgres fake proves the org actually used is the
// AUTHENTICATED claim, never the caller-supplied orgId argument -- home.
// BuildResponse's own sequential first step (FetchLatestSuccessfulSyncAt,
// builder.go) lets a single recorded call settle this without needing to
// race BuildResponse's later concurrent ClickHouse fan-out.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// recordingHomeCHClient records how many times it was reached and always
// errors -- these tests exist to prove ClickHouse is never reached until
// Postgres's freshness read has already succeeded, so a client that
// would fail loudly if reached makes an accidental fall-through visible.
type recordingHomeCHClient struct{ calls int }

func (c *recordingHomeCHClient) Query(_ context.Context, _ string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.calls++
	return nil, errors.New("recordingHomeCHClient: should not have been called")
}

// failingRow is a pgx.Row whose Scan always returns the given error --
// the minimal fake needed to make QueryRow's chained .Scan(...) call
// return control to the caller without a real Postgres connection.
type failingRow struct{ err error }

func (f failingRow) Scan(_ ...any) error { return f.err }

// recordingHomePGClient records every QueryRow call's positional args
// (FetchLatestSuccessfulSyncAt passes orgID first, queries_sync.go:50)
// and fails loudly, matching recordingHomeCHClient's convention. It also
// implements Query (datahealth.PGQuerier's method) purely so the
// Resolver.Postgres field -- statically typed as that narrower interface
// -- accepts it as a struct literal; Home's own runtime type assertion
// to home.PGQueryClient is what actually reaches QueryRow.
type recordingHomePGClient struct{ args [][]any }

func (p *recordingHomePGClient) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	p.args = append(p.args, args)
	return failingRow{err: errors.New("recordingHomePGClient: should not have been called")}
}

func (p *recordingHomePGClient) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, errors.New("recordingHomePGClient: Query should not have been called")
}

func TestHome_RejectsMissingClaims(t *testing.T) {
	ch := &recordingHomeCHClient{}
	r := &Resolver{ClickHouse: ch}
	_, err := r.Query().Home(context.Background(), "org-1", nil, nil)
	asAuthorizationError(t, err)
	if ch.calls != 0 {
		t.Fatal("ClickHouse must not be reached when claims are missing")
	}
}

func TestHome_RejectsEmptyOrgIDClaim(t *testing.T) {
	ch := &recordingHomeCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: ""})
	_, err := r.Query().Home(ctx, "org-1", nil, nil)
	asAuthorizationError(t, err)
	if ch.calls != 0 {
		t.Fatal("ClickHouse must not be reached when the OrgID claim is empty")
	}
}

// TestHome_UsesAuthenticatedOrgIDNotArgument pins the resolver's own doc
// comment claim: the org actually queried is the verified envelope's
// claims.OrgID, never the client-supplied orgID GraphQL argument --
// proven by naming a DIFFERENT org in each and reading back which one
// FetchLatestSuccessfulSyncAt actually bound. The Postgres fake errors
// immediately, so BuildResponse returns before ever reaching ClickHouse
// (builder.go's pgClient block is sequential and precedes every
// ClickHouse read) -- ch.calls == 0 confirms that ordering held.
func TestHome_UsesAuthenticatedOrgIDNotArgument(t *testing.T) {
	ch := &recordingHomeCHClient{}
	pg := &recordingHomePGClient{}
	r := &Resolver{ClickHouse: ch, Postgres: pg}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-authenticated"})
	_, err := r.Query().Home(ctx, "org-argument-different", nil, nil)
	requireNotAuthorizationError(t, err)
	if len(pg.args) != 1 {
		t.Fatalf("expected exactly one Postgres QueryRow call, got %d", len(pg.args))
	}
	if ch.calls != 0 {
		t.Fatalf("ClickHouse must not be reached once Postgres already failed, got %d calls", ch.calls)
	}
	gotOrgID, ok := pg.args[0][0].(string)
	if !ok || gotOrgID != "org-authenticated" {
		t.Fatalf("QueryRow's first arg = %v, want the authenticated claim %q, never the GraphQL argument", pg.args[0], "org-authenticated")
	}
}

// TestHome_PostgresTypeAssertionFailureIsAnError proves that a Postgres
// dependency lacking QueryRow (this resolver's minimum requirement) is a
// returned error, never a nil-pointer panic -- a Resolver constructed
// without a Postgres field (nil interface) is exactly that shape.
func TestHome_PostgresTypeAssertionFailureIsAnError(t *testing.T) {
	ch := &recordingHomeCHClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-authenticated"})
	_, err := r.Query().Home(ctx, "org-authenticated", nil, nil)
	if err == nil {
		t.Fatal("expected an error when Postgres is nil, got nil")
	}
	if ch.calls != 0 {
		t.Fatalf("ClickHouse must not be reached when the Postgres dependency is missing, got %d calls", ch.calls)
	}
}

// bindingRecordingCHClient records the bindings of every query and fails it.
type bindingRecordingCHClient struct {
	mu       sync.Mutex
	bindings []clickhouse.Binding
}

func (c *bindingRecordingCHClient) Query(_ context.Context, _ string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings = append(c.bindings, b...)
	return nil, errors.New("bindingRecordingCHClient: recorded")
}

func (c *bindingRecordingCHClient) values(name string) map[any]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[any]bool{}
	for _, b := range c.bindings {
		if b.Name == name {
			out[b.Value] = true
		}
	}
	return out
}

// A requested window reaches the ClickHouse reads: 90 days ending 2026-03-31
// is queried as start_day 2026-01-01 .. end_day 2026-04-01, and a request
// without a window is queried for the default 14 days.
func TestHome_RequestedWindowIsTheWindowQueried(t *testing.T) {
	run := func(window *model.HomeWindowInput) *bindingRecordingCHClient {
		ch := &bindingRecordingCHClient{}
		pg := &recordingHomePGClient{}
		r := &Resolver{ClickHouse: ch, Postgres: pgNoRows{pg}}
		ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
		_, _ = r.Query().Home(ctx, "org-1", nil, window)
		return ch
	}
	days := 90
	end := graphqldate.Date(time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC))
	ch := run(&model.HomeWindowInput{RangeDays: &days, EndDate: &end})
	if got := ch.values("start_day"); !got["2026-01-01"] {
		t.Fatalf("start_day bindings = %v, want 2026-01-01 among them (90-day window)", got)
	}
	if got := ch.values("end_day"); !got["2026-04-01"] {
		t.Fatalf("end_day bindings = %v, want 2026-04-01 among them", got)
	}

	defaults := run(nil)
	starts := defaults.values("start_day")
	if len(starts) == 0 || starts["2026-01-01"] {
		t.Fatalf("default window start_day bindings = %v, want a 14-day window", starts)
	}
}

// pgNoRows makes the freshness read succeed with no row, so BuildResponse
// proceeds to its ClickHouse reads.
type pgNoRows struct{ *recordingHomePGClient }

func (p pgNoRows) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return failingRow{err: pgx.ErrNoRows}
}
