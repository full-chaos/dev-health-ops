package synccli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

const catalogTurnOrg = "20000000-0000-4000-8000-000000000009"

// catalogRunWithFailingCarry runs `dho sync teams --provider github` against a store whose first read fails: a run
// that reaches the collector shows as a query on conn, one that does not shows none.
func catalogRunWithFailingCarry(t *testing.T, org string, extraArgs []string, values map[string]string, d deps) (code int, stderr string, conn *teamCatalogCarryConn) {
	t.Helper()
	conn = &teamCatalogCarryConn{err: errors.New("count read failed")}
	d.openStore = func(context.Context, string) (driver.Conn, error) { return conn, nil }
	d.doer = &http.Client{Transport: refusingTransport{calls: new(int)}}
	env := map[string]string{"CLICKHOUSE_URI": dsnValue}
	for key, value := range values {
		env[key] = value
	}
	var errOut strings.Builder
	args := append([]string{"--provider", "github", "--owner", "acme", "--org", org, "--auth", "token-for-test"}, extraArgs...)
	code = runTeams(context.Background(), cli.Env{Args: args, Lookup: lookup(env), Stdout: io.Discard, Stderr: &errOut}, d)
	return code, errOut.String(), conn
}

// A run with no PostgreSQL DSN cannot take the catalog turn the worker takes; it says so and still runs.
func TestCatalogTeamsWithoutAPostgresDSNWarnsThatNoTurnIsTaken(t *testing.T) {
	_, stderr, conn := catalogRunWithFailingCarry(t, catalogTurnOrg, nil, nil, defaultDeps())

	for _, want := range []string{`"level":"WARN"`, `team catalog turn not taken`, `"reason":"no_postgres_dsn"`, `"provider":"github"`} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr lacks %s: %s", want, stderr)
		}
	}
	if !strings.Contains(stderr, "a concurrent worker or CLI run of this organization and provider is not excluded") {
		t.Errorf("the warning does not say what is not excluded: %s", stderr)
	}
	if conn.queries != 1 {
		t.Errorf("the run made %d store reads, want 1 (it still runs without the turn)", conn.queries)
	}
}

// An organization id that is not a UUID cannot key a lease: the worker only ever holds UUID organizations.
func TestCatalogTeamsWithANonUUIDOrganizationWarnsAndRunsWithoutATurn(t *testing.T) {
	_, stderr, conn := catalogRunWithFailingCarry(t, "org-not-a-uuid", nil, map[string]string{PostgresURIKey: "postgres://pg-user:pg-pass-value@127.0.0.1:1/db"}, defaultDeps())

	if !strings.Contains(stderr, `team catalog turn not taken`) || !strings.Contains(stderr, `"reason":"org_not_uuid"`) {
		t.Errorf("stderr lacks the org_not_uuid warning: %s", stderr)
	}
	if conn.queries != 1 {
		t.Errorf("the run made %d store reads, want 1", conn.queries)
	}
}

// turnProbe is the catalog turn in a test: it records when it is taken and released and how many store reads had
// happened by then, and can fail, wait for the context, or report the turn lost.
type turnProbe struct {
	conn                    *teamCatalogCarryConn
	acquired, released      int
	readsAtAcquire          int
	readsAtRelease          int
	acquiredOrg, acquiredBy string
	err                     error
	lost                    chan struct{}
}

func (probe *turnProbe) Serialize(ctx context.Context, orgID, provider string) (func(), error) {
	release, _, err := probe.SerializeWithLoss(ctx, orgID, provider)
	return release, err
}

func (probe *turnProbe) SerializeWithLoss(_ context.Context, orgID, provider string) (func(), <-chan struct{}, error) {
	if probe.err != nil {
		return nil, nil, probe.err
	}
	probe.acquired++
	probe.acquiredOrg, probe.acquiredBy = orgID, provider
	if probe.conn != nil {
		probe.readsAtAcquire = probe.conn.queries
	}
	return func() {
		probe.released++
		if probe.conn != nil {
			probe.readsAtRelease = probe.conn.queries
		}
	}, probe.lost, nil
}

// withProbe runs the verb with a Postgres pool that never connects (pgxpool dials lazily) and the probe as its turn.
func withProbe(t *testing.T, probe *turnProbe, opener func(context.Context, string) (*pgxpool.Pool, error)) (int, string) {
	t.Helper()
	d := defaultDeps()
	if opener == nil {
		opener = func(ctx context.Context, dsn string) (*pgxpool.Pool, error) { return openPostgresPool(ctx, dsn) }
	}
	d.openPostgres = opener
	d.catalogTurn = func(*pgxpool.Pool) providersync.TeamCatalogSerializer { return probe }
	code, stderr, conn := catalogRunWithFailingCarry(t, catalogTurnOrg, []string{"--db", "postgres://pg-user:pg-pass-value@127.0.0.1:1/db"}, nil, d)
	if probe.conn == nil {
		probe.conn = conn
	}
	return code, stderr
}

func TestCatalogTeamsTakesTheTurnBeforeItReadsAndReleasesItAfter(t *testing.T) {
	probe := &turnProbe{}
	d := defaultDeps()
	d.openPostgres = func(ctx context.Context, dsn string) (*pgxpool.Pool, error) { return openPostgresPool(ctx, dsn) }
	d.catalogTurn = func(*pgxpool.Pool) providersync.TeamCatalogSerializer { return probe }
	conn := &teamCatalogCarryConn{err: errors.New("count read failed")}
	probe.conn = conn
	d.openStore = func(context.Context, string) (driver.Conn, error) { return conn, nil }
	d.doer = &http.Client{Transport: refusingTransport{calls: new(int)}}
	var stderr strings.Builder
	runTeams(context.Background(), cli.Env{
		Args:   []string{"--provider", "github", "--owner", "acme", "--org", catalogTurnOrg, "--auth", "t", "--db", "postgres://pg-user:pg-pass-value@127.0.0.1:1/db"},
		Lookup: lookup(map[string]string{"CLICKHOUSE_URI": dsnValue}), Stdout: io.Discard, Stderr: &stderr,
	}, d)

	if probe.acquired != 1 || probe.released != 1 {
		t.Fatalf("turn acquired %d times, released %d, want 1 and 1", probe.acquired, probe.released)
	}
	if probe.acquiredOrg != catalogTurnOrg || probe.acquiredBy != "github" {
		t.Errorf("turn taken for %q/%q, want the run's organization and provider", probe.acquiredOrg, probe.acquiredBy)
	}
	if probe.readsAtAcquire != 0 || probe.readsAtRelease != 1 {
		t.Errorf("store reads at acquire/release = %d/%d, want 0/1 (the turn wraps the whole collection)", probe.readsAtAcquire, probe.readsAtRelease)
	}
	for _, want := range []string{"waiting for the team catalog turn", "team catalog turn taken"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr lacks the %q line: %s", want, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), "team catalog turn not taken") {
		t.Errorf("a run that took the turn warned that it did not: %s", stderr.String())
	}
}

// A run that was asked to take the turn never runs without it.
func TestCatalogTeamsFailsClosedWhenTheTurnCannotBeTaken(t *testing.T) {
	failed := errors.New("postgres down")
	for name, tc := range map[string]struct {
		probe  *turnProbe
		opener func(context.Context, string) (*pgxpool.Pool, error)
	}{
		"the pool cannot be opened": {probe: &turnProbe{}, opener: func(context.Context, string) (*pgxpool.Pool, error) { return nil, failed }},
		"the lease cannot be taken": {probe: &turnProbe{err: failed}},
	} {
		t.Run(name, func(t *testing.T) {
			code, stderr := withProbe(t, tc.probe, tc.opener)
			if code != cli.ExitFailure || !strings.Contains(stderr, `"code":"turn_unavailable"`) {
				t.Errorf("exit %d, stderr %s, want exit 1 with turn_unavailable", code, stderr)
			}
			if tc.probe.conn != nil && tc.probe.conn.queries != 0 {
				t.Errorf("the run made %d store reads without the turn", tc.probe.conn.queries)
			}
		})
	}
}

// A lease the run loses while it lives ends the run before it writes (the seam cancels it): exit 1, nothing written.
type blockingStoreConn struct {
	driver.Conn
	batches int
}

func (conn *blockingStoreConn) Query(ctx context.Context, _ string, _ ...any) (driver.Rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (conn *blockingStoreConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.batches++
	return nil, errors.New("no batch")
}

func (conn *blockingStoreConn) Close() error { return nil }

func TestCatalogTeamsRunWhoseTurnIsTakenOverIsEndedAndWritesNothing(t *testing.T) {
	lost := make(chan struct{})
	close(lost)
	probe := &turnProbe{lost: lost}
	conn := &blockingStoreConn{}
	d := defaultDeps()
	d.openPostgres = func(ctx context.Context, dsn string) (*pgxpool.Pool, error) { return openPostgresPool(ctx, dsn) }
	d.catalogTurn = func(*pgxpool.Pool) providersync.TeamCatalogSerializer { return probe }
	d.openStore = func(context.Context, string) (driver.Conn, error) { return conn, nil }
	d.doer = &http.Client{Transport: refusingTransport{calls: new(int)}}
	// Bounded: a turn that is not honoured would leave the store read waiting for ever; that is a failure, not a hang.
	bounded, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	var stderr strings.Builder
	code := runTeams(bounded, cli.Env{
		Args:   []string{"--provider", "github", "--owner", "acme", "--org", catalogTurnOrg, "--auth", "t", "--db", "postgres://pg-user:pg-pass-value@127.0.0.1:1/db"},
		Lookup: lookup(map[string]string{"CLICKHOUSE_URI": dsnValue}), Stdout: io.Discard, Stderr: &stderr,
	}, d)

	if code != cli.ExitFailure || !strings.Contains(stderr.String(), providersync.ErrTeamCatalogTurnLost.Error()) {
		t.Errorf("exit %d, stderr %s, want exit 1 naming %q", code, stderr.String(), providersync.ErrTeamCatalogTurnLost)
	}
	if conn.batches != 0 {
		t.Errorf("the run wrote %d batches after its turn was lost", conn.batches)
	}
}

// The CLI takes the very turn the worker takes: one shared type, not a copy.
func TestCatalogTeamsDefaultTurnIsTheWorkersSharedTurn(t *testing.T) {
	turn := defaultDeps().catalogTurn(nil)
	if _, ok := turn.(jobruntime.TeamCatalogTurn); !ok {
		t.Errorf("the CLI's default turn is %T, want jobruntime.TeamCatalogTurn (the worker's)", turn)
	}
}
