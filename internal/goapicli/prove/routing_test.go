package prove

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

// A document operation is measured as served: canary against the running build, one entry per registered operation.
func TestServedRoutingMeasuresEveryRegisteredOperationAsCanaryAgainstTheRunningBuild(t *testing.T) {
	registry := goapiproof.RegistryView{
		BuildIdentity:  "running-build",
		DocumentDigest: map[string]string{"featureFlags": "d1", "hotspots": "d2"},
	}
	routing := servedRouting(registry)
	if len(routing) != 2 {
		t.Fatalf("routing = %v, want one entry per registered operation", routing)
	}
	for operation := range registry.DocumentDigest {
		if row := routing[operation]; row.Mode != "canary" || row.CandidateBuild != "running-build" {
			t.Fatalf("%s = %+v, want canary against the running build", operation, row)
		}
	}
	if got := servedRouting(goapiproof.RegistryView{}); len(got) != 0 {
		t.Fatalf("a registry that registers nothing yields entries: %v", got)
	}
	if err := goapiproof.VerifyCandidateBuild("running-build", "another-build", routing); err == nil {
		t.Fatal("-candidate-build is a cross-check: a build that is not the running one must refuse")
	}
	if err := goapiproof.VerifyCandidateBuild("running-build", "running-build", routing); err != nil {
		t.Fatalf("a matching cross-check must pass: %v", err)
	}
}
