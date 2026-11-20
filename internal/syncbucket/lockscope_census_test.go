package syncbucket

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-14250 acp lock-1 rule 1: while the bucket lock is held, a writer runs only the database statements of its own
// write and of the pick: no outbound call. This census reads each locked section's source and fails on any direct call
// that leaves the transaction: a River insert or retry, a dispatch wake or slot release (their own statements on a
// second connection), a call on a pool (a second connection), the non-tx budget estimator, net, net/http, os/exec and
// time.Sleep. A marker that is not found fails the census (it never skips).

// lockScope is one locked section: the function's body from the first `start` to the first `end` after it (both
// empty: the whole body; function empty: every function of the file).
type lockScope struct {
	file     string
	function string
	start    string
	end      string
}

var lockScopes = []lockScope{
	{"internal/providersync/repository_postgres.go", "Complete", "repository.lockSlotBucket(", "tx.Commit(ctx)"},
	{"internal/providersync/repository_postgres.go", "CompleteLinearWorkItemFamily", "repository.lockSlotBucket(", "tx.Commit(ctx)"},
	{"internal/providersync/repository_postgres.go", "failTx", "repository.lockSlotBucket(", "tx.Commit(ctx)"},
	{"internal/syncreconciler/lease_repair.go", "Step", "acquireLeaseRepairBucketLocks(", "tx.Commit(ctx)"},
	{"internal/syncdispatchruntime/bucket_sweep.go", "pickBucketSlots", "", ""},
	{"internal/syncreconciler/dispatch_wake.go", "reserveReleasedSlots", "", ""},
	{"internal/syncdispatchruntime/native_dispatch_sync_run_service.go", "Dispatch", "authorizeRun(", "service.commitFenced(ctx, tx, fence); err != nil"},
	{"internal/syncbucket/syncbucket.go", "", "", ""},
}

// Calls banned by name on any receiver.
var bannedMethodNames = map[string]string{
	"EnqueueBucketSweep":     "bucket sweep enqueue (River insert)",
	"EnsureBucketSweep":      "bucket sweep enqueue (River insert)",
	"JobRetry":               "River JobRetry",
	"Insert":                 "River client Insert (non-Tx)",
	"InsertMany":             "River client InsertMany (non-Tx)",
	"DispatchBudgetEstimate": "non-tx budget estimator",
}

// syncrunrollup functions that run their own statements on a second connection.
var bannedRollupFuncs = map[string]bool{
	"WakeDispatch": true, "WakeReserved": true, "WakeDispatchAt": true,
	"ReleaseSlot": true, "BumpBucketGeneration": true, "AckBucketGeneration": true,
}

var bannedImportPaths = map[string]bool{"net/http": true, "net": true, "os/exec": true}

type censusHit struct {
	position string
	call     string
	reason   string
}

func (hit censusHit) String() string { return hit.position + ": " + hit.call + " (" + hit.reason + ")" }

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("repo root %s has no go.mod: %v", dir, err)
	}
	return dir
}

func isPoolSelector(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && (selector.Sel.Name == "pool" || selector.Sel.Name == "Pool")
}

// censusSource parses src (named name) and returns the banned calls inside scope's locked section.
func censusSource(name string, src []byte, scope lockScope) ([]censusHit, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, 0, fmt.Errorf("parse %s: %w", name, err)
	}
	imports := map[string]string{}
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		local := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			local = spec.Name.Name
		}
		imports[local] = path
	}
	type section struct{ from, to token.Pos }
	var sections []section
	found := false
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Body == nil || (scope.function != "" && function.Name.Name != scope.function) {
			continue
		}
		found = true
		from, to := function.Body.Pos(), function.Body.End()
		if scope.start != "" || scope.end != "" {
			body := string(src[fset.Position(from).Offset:fset.Position(to).Offset])
			startAt := strings.Index(body, scope.start)
			if startAt < 0 {
				return nil, 0, fmt.Errorf("%s %s: start marker %q not found", name, scope.function, scope.start)
			}
			endAt := strings.Index(body[startAt:], scope.end)
			if endAt < 0 {
				return nil, 0, fmt.Errorf("%s %s: end marker %q not found after the start marker", name, scope.function, scope.end)
			}
			to = from + token.Pos(startAt+endAt+len(scope.end))
			from += token.Pos(startAt)
		}
		sections = append(sections, section{from, to})
	}
	if !found {
		return nil, 0, fmt.Errorf("%s: function %q not found", name, scope.function)
	}
	var hits []censusHit
	calls := 0
	for _, part := range sections {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || call.Pos() < part.from || call.Pos() >= part.to {
				return true
			}
			calls++
			position := fset.Position(call.Pos())
			at := fmt.Sprintf("%s:%d", name, position.Line)
			text := string(src[position.Offset:fset.Position(call.Fun.End()).Offset]) + "("
			report := func(reason string) { hits = append(hits, censusHit{at, text, reason}) }
			switch fun := call.Fun.(type) {
			case *ast.SelectorExpr:
				if reason, banned := bannedMethodNames[fun.Sel.Name]; banned {
					report(reason)
				}
				if pkg, ok := fun.X.(*ast.Ident); ok {
					path := imports[pkg.Name]
					switch {
					case path == "github.com/full-chaos/dev-health-ops/internal/syncrunrollup" && bannedRollupFuncs[fun.Sel.Name]:
						report("dispatch wake / slot release on a second connection")
					case bannedImportPaths[path]:
						report("outbound call (" + path + ")")
					case path == "time" && fun.Sel.Name == "Sleep":
						report("time.Sleep under the lock")
					}
				}
				if isPoolSelector(fun.X) {
					report("pool receiver call (a second connection)")
				}
			case *ast.Ident:
				if bannedRollupFuncs[fun.Name] {
					report("dispatch wake / slot release on a second connection")
				}
			}
			for _, arg := range call.Args {
				if isPoolSelector(arg) {
					report("pool passed as an argument (a second connection)")
				}
			}
			return true
		})
	}
	return hits, calls, nil
}

func TestNoOutboundCallInsideTheBucketLock(t *testing.T) {
	t.Run("planted", testCensusFlagsAPlantedRelease)
	root := repoRoot(t)
	var all []censusHit
	for _, scope := range lockScopes {
		src, err := os.ReadFile(filepath.Join(root, scope.file))
		if err != nil {
			t.Fatalf("read %s: %v", scope.file, err)
		}
		hits, calls, err := censusSource(scope.file, src, scope)
		if err != nil {
			t.Fatalf("census: %v", err)
		}
		if calls == 0 {
			t.Fatalf("census: %s %s: the locked section has no call at all (a marker matched the wrong place)", scope.file, scope.function)
		}
		t.Logf("%s %s: %d calls in the locked section, %d banned", scope.file, scope.function, calls, len(hits))
		all = append(all, hits...)
	}
	for _, hit := range all {
		t.Errorf("outbound call inside the bucket lock: %s", hit)
	}
}

const plantedLockedSource = `package planted

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
)

type repo struct{ Pool any }

func (repository *repo) Complete(ctx context.Context, tx pgx.Tx) error {
	syncrunrollup.ReleaseSlot(ctx, nil, nil, syncrunrollup.SlotFree{}, now, "before")
	if err := repository.lockSlotBucket(ctx, tx); err != nil {
		return err
	}
	syncrunrollup.ReleaseSlot(ctx, nil, nil, syncrunrollup.SlotFree{}, now, "planted")
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	syncrunrollup.ReleaseSlot(ctx, nil, nil, syncrunrollup.SlotFree{}, now, "after")
	return nil
}
`

// The census flags a ReleaseSlot planted inside a marked range, and only that one; a missing marker fails.
func testCensusFlagsAPlantedRelease(t *testing.T) {
	scope := lockScope{"planted.go", "Complete", "repository.lockSlotBucket(", "tx.Commit(ctx)"}
	hits, _, err := censusSource("planted.go", []byte(plantedLockedSource), scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].position != "planted.go:17" || hits[0].call != "syncrunrollup.ReleaseSlot(" {
		t.Fatalf("planted ReleaseSlot inside the lock: hits %v, want exactly planted.go:17 syncrunrollup.ReleaseSlot(", hits)
	}
	t.Logf("planted hit found: %s", hits[0])
	for _, missing := range []lockScope{
		{"planted.go", "Complete", "repository.lockMissing(", "tx.Commit(ctx)"},
		{"planted.go", "Complete", "repository.lockSlotBucket(", "tx.CommitMissing(ctx)"},
		{"planted.go", "Missing", "", ""},
	} {
		if _, _, err := censusSource("planted.go", []byte(plantedLockedSource), missing); err == nil {
			t.Fatalf("census of %+v: a missing marker passed, want a failure", missing)
		} else {
			t.Logf("missing marker fails: %v", err)
		}
	}
}
