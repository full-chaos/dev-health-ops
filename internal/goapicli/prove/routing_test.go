package prove

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// F4: readRoutingState had ZERO tests, and both of its guards survived
// removal under the full suite.
//
//   - line 403: `if err := VerifyCandidateBuild(...); err != nil { _ = err }`
//     compiles and runs. The comment above the function claimed that
//     mutation was "unwritable"; it was not, and the comment now says so.
//   - line 393: deleting the `continue` that drops rows keyed to a
//     SUPERSEDED document digest. A dead row is then adopted as the
//     operation's current mode -- the CHAOS-5416 class the function's own
//     comment cites.
//
// A fake Querier is enough to hold both.
type fakeRoutingRows struct {
	rows [][]any
	i    int
	err  error
}

func (f *fakeRoutingRows) Next() bool                                   { f.i++; return f.i <= len(f.rows) }
func (f *fakeRoutingRows) Err() error                                   { return f.err }
func (f *fakeRoutingRows) Close()                                       {}
func (f *fakeRoutingRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (f *fakeRoutingRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (f *fakeRoutingRows) Values() ([]any, error)                       { return nil, nil }
func (f *fakeRoutingRows) RawValues() [][]byte                          { return nil }
func (f *fakeRoutingRows) Conn() *pgx.Conn                              { return nil }
func (f *fakeRoutingRows) Scan(dest ...any) error {
	row := f.rows[f.i-1]
	for i := range dest {
		*(dest[i].(*string)) = row[i].(string)
	}
	return nil
}

type fakeQuerier struct{ rows *fakeRoutingRows }

func (f fakeQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return f.rows, nil
}

func TestReadRoutingStateRefusesAndFilters(t *testing.T) {
	registry := goapiproof.RegistryView{
		BuildIdentity:  "running-build",
		DocumentDigest: map[string]string{"featureFlags": "live-digest", "hotspots": "live-digest"},
	}

	t.Run("a row naming another build refuses the whole read", func(t *testing.T) {
		source := fakeQuerier{rows: &fakeRoutingRows{rows: [][]any{
			{"featureFlags", "live-digest", "canary", "some-other-build"},
		}}}
		rows, err := readRoutingState(context.Background(), source, registry, "")
		if err == nil {
			t.Fatal("a stale routing row must refuse: the rows and the verdict come back together or neither does")
		}
		if rows != nil {
			t.Fatalf("rows were returned alongside the refusal: %v", rows)
		}
		if !strings.Contains(err.Error(), "featureFlags") {
			t.Fatalf("the refusal must name the row: %v", err)
		}
	})

	t.Run("a row at a superseded document digest is dropped", func(t *testing.T) {
		source := fakeQuerier{rows: &fakeRoutingRows{rows: [][]any{
			{"featureFlags", "live-digest", "canary", "running-build"},
			// Dead: the running process registers a different document for
			// this operation, so this row's mode must not be adopted.
			{"hotspots", "superseded-digest", "primary", "running-build"},
		}}}
		rows, err := readRoutingState(context.Background(), source, registry, "")
		if err != nil {
			t.Fatalf("readRoutingState: %v", err)
		}
		if _, adopted := rows["hotspots"]; adopted {
			t.Fatal("a row keyed to a superseded document digest was adopted as the operation's current mode -- that is the CHAOS-5416 class")
		}
		if rows["featureFlags"].Mode != "canary" {
			t.Fatalf("the live row must survive: %+v", rows)
		}
	})

	t.Run("agreeing rows come back", func(t *testing.T) {
		source := fakeQuerier{rows: &fakeRoutingRows{rows: [][]any{
			{"featureFlags", "live-digest", "canary", "running-build"},
		}}}
		rows, err := readRoutingState(context.Background(), source, registry, "running-build")
		if err != nil {
			t.Fatalf("readRoutingState: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected one row, got %v", rows)
		}
	})
}

// isNilSource is a real guard with no test: --dry-run passes a nil
// *pgxpool.Pool, which inside an interface is NOT == nil, and
// reflect.Value.IsNil panics on a non-pointer kind. Reverting it to a bare
// `pool == nil` makes the dry-run path panic instead of returning an empty
// routing map.
func TestReadRoutingStateHandlesATypedNilPool(t *testing.T) {
	// Exactly what run() passes under --dry-run: a nil *pgxpool.Pool
	// carried in the interface.
	var pool *pgxpool.Pool
	rows, err := readRoutingState(context.Background(), pool, goapiproof.RegistryView{}, "")
	if err != nil {
		t.Fatalf("a dry run must not fail: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("a dry run has no rows to read, got %v", rows)
	}

	// And an untyped nil, which is the other way a caller can express it.
	rows, err = readRoutingState(context.Background(), nil, goapiproof.RegistryView{}, "")
	if err != nil || len(rows) != 0 {
		t.Fatalf("an absent source must yield an empty map: %v %v", rows, err)
	}

	// A real source is NOT treated as absent -- otherwise the guard could
	// pass by declaring everything nil.
	source := fakeQuerier{rows: &fakeRoutingRows{}}
	if isNilSource(source) {
		t.Fatal("a live source was treated as absent")
	}
}

// fakeErrQuerier simulates a pool that opened successfully (a Ping would
// pass) but whose FIRST real operation -- exactly readRoutingState's own
// Query -- fails with a driver error that still carries the DSN. This is
// the class a Ping at pool-open time does not close by itself: a
// connection that drops, or a driver that only discovers the DSN is bad
// on first real use, after Ping already succeeded.
type fakeErrQuerier struct{ err error }

func (f fakeErrQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, f.err
}

// TestRunRedactsALaterQueryErrorEvenAfterASuccessfulOpen pins the
// boundary run() applies: the DSN this run resolved is redacted from
// WHATEVER error readRoutingState's own Query returns, not only from
// openPostgresPool's own. Composed the same way run() itself composes
// it -- secrets.NewBoundary(the resolved DSN).Redact(the returned error)
// -- against a fake Querier standing in for a pool whose Query call is
// the first to discover the DSN is unreachable.
func TestRunRedactsALaterQueryErrorEvenAfterASuccessfulOpen(t *testing.T) {
	dsn := "postgres://user:" + postgresURIMarker + "@host/db"
	registry := goapiproof.RegistryView{
		BuildIdentity:  "running-build",
		DocumentDigest: map[string]string{"featureFlags": "live-digest"},
	}
	source := fakeErrQuerier{err: errors.New("read go_api_routing_state: failed to connect to `" + dsn + "`: server closed the connection")}

	_, err := readRoutingState(context.Background(), source, registry, "")
	if err == nil {
		t.Fatal("want a non-nil error from a failing Query")
	}

	redacted := secrets.NewBoundary(dsn).Redact(err)

	if strings.Contains(redacted.Error(), postgresURIMarker) {
		t.Fatalf("the boundary left the marker in a later-call error: %v", redacted)
	}
	if strings.Contains(redacted.Error(), dsn) {
		t.Fatalf("the boundary left the DSN in a later-call error: %v", redacted)
	}
}

// failingDBPool satisfies dbPool: Query (routingRowSource, the only
// method run() reaches before returning on a failing readRoutingState)
// returns the injected DSN-carrying error; the rest are never called on
// this path and panic if they ever are, so a future change routing
// further than expected fails loudly instead of silently passing.
type failingDBPool struct{ queryErr error }

func (p failingDBPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, p.queryErr
}
func (failingDBPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("not reached: run() returns on readRoutingState's error first")
}
func (failingDBPool) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("not reached: run() returns on readRoutingState's error first")
}
func (failingDBPool) Close() {}

// TestRun_RedactsALaterQueryErrorEvenAfterASuccessfulOpen drives run()
// itself (not just readRoutingState in isolation) against a pool that
// opens successfully -- withFakePool substitutes openPostgresPool
// entirely, standing in for a Ping that already passed -- but whose
// first real operation, readRoutingState's own Query, fails with a
// driver error carrying the DSN. Proves the production wiring: run()'s
// own defer redacts it, not a hand-composed call in the test.
func TestRun_RedactsALaterQueryErrorEvenAfterASuccessfulOpen(t *testing.T) {
	dsn := "postgres://user:" + postgresURIMarker + "@host/db"

	featureFlagsDoc := "query FeatureFlags { featureFlags { key } }"
	featureFlagsDigest := goapidigest.Document(featureFlagsDoc)

	type registryOp struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	}
	var ops []registryOp
	for _, name := range goapiproof.KnownOperations() {
		if name == "featureFlags" {
			ops = append(ops, registryOp{Operation: name, DocumentDigest: featureFlagsDigest})
			continue
		}
		// Registered (AssertCoverage requires it) but never matched by a
		// document below; this run never reaches an operation that would
		// refuse on that, since it fails at readRoutingState first.
		ops = append(ops, registryOp{Operation: name, DocumentDigest: "sha256:unused-" + name})
	}
	registryBody, err := json.Marshal(struct {
		SchemaDigest string       `json:"schema_digest"`
		Operations   []registryOp `json:"operations"`
	}{SchemaDigest: "sha256:redact-e2e", Operations: ops})
	if err != nil {
		t.Fatalf("marshal registry body: %v", err)
	}
	registry := httptest.NewServer(writeStaticJSONHandler(string(registryBody)))
	t.Cleanup(registry.Close)

	buildinfo := httptest.NewServer(writeStaticJSONHandler(`{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`))
	t.Cleanup(buildinfo.Close)

	type documentEntry struct {
		Operation string `json:"operation"`
		Document  string `json:"document"`
	}
	docsJSON, err := json.Marshal([]documentEntry{{Operation: "featureFlags", Document: featureFlagsDoc}})
	if err != nil {
		t.Fatalf("marshal documents: %v", err)
	}
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docsJSON, 0o600); err != nil {
		t.Fatalf("write documents file: %v", err)
	}

	withFakePool(t, failingDBPool{
		queryErr: errors.New("read go_api_routing_state: failed to connect to `" + dsn + "`: server closed the connection"),
	})

	edgeToken := syntheticJWT(t, map[string]string{"sub": "edge"})
	proofToken := syntheticJWT(t, map[string]string{"sub": "proof"})

	err = runCLI(t, []string{
		"-registry-url=" + registry.URL + "/registry",
		"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
		"-documents=" + docsPath,
		"-postgres-uri=" + dsn,
		"-org=70d529e0",
		"-artifact-dir=" + t.TempDir(),
		"-recorded-by=harness",
		"-review-evidence=e2e harness run",
		"-edge-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, edgeToken)),
		"-proof-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, proofToken)),
		"-timeout=5s",
	})

	if err == nil {
		t.Fatal("want the run to fail on the injected query error")
	}
	if strings.Contains(err.Error(), postgresURIMarker) {
		t.Fatalf("run()'s own error leaked the DSN's marker password: %v", err)
	}
	if strings.Contains(err.Error(), dsn) {
		t.Fatalf("run()'s own error leaked the DSN: %v", err)
	}
}
