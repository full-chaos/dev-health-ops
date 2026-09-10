package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
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
