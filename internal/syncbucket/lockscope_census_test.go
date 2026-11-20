package syncbucket

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
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
			hits = append(hits, checkCall(fset, name, src, imports, call, false)...)
			return true
		})
	}
	return hits, calls, nil
}

// checkCall is the census's verdict on one call: every reason it is banned under the lock. outboundOnly keeps the
// outbound classes (net, net/http, os/exec, time.Sleep and the credential network steps) and drops the
// second-connection classes, for the deep walk below.
func checkCall(fset *token.FileSet, name string, src []byte, imports map[string]string, call *ast.CallExpr, outboundOnly bool) []censusHit {
	var hits []censusHit
	position := fset.Position(call.Pos())
	at := fmt.Sprintf("%s:%d", name, position.Line)
	text := string(src[position.Offset:fset.Position(call.Fun.End()).Offset]) + "("
	report := func(reason string) { hits = append(hits, censusHit{at, text, reason}) }
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		if reason, banned := bannedMethodNames[fun.Sel.Name]; banned && !outboundOnly {
			report(reason)
		}
		if reason, banned := outboundMethodNames[fun.Sel.Name]; banned && outboundOnly {
			report(reason)
		}
		if pkg, ok := fun.X.(*ast.Ident); ok {
			path := imports[pkg.Name]
			switch {
			case path == "github.com/full-chaos/dev-health-ops/internal/syncrunrollup" && bannedRollupFuncs[fun.Sel.Name] && !outboundOnly:
				report("dispatch wake / slot release on a second connection")
			case bannedImportPaths[path]:
				report("outbound call (" + path + ")")
			case path == "time" && fun.Sel.Name == "Sleep":
				report("time.Sleep under the lock")
			}
		}
		if isPoolSelector(fun.X) && !outboundOnly {
			report("pool receiver call (a second connection)")
		}
	case *ast.Ident:
		if bannedRollupFuncs[fun.Name] && !outboundOnly {
			report("dispatch wake / slot release on a second connection")
		}
	}
	if !outboundOnly {
		for _, arg := range call.Args {
			if isPoolSelector(arg) {
				report("pool passed as an argument (a second connection)")
			}
		}
	}
	return hits
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

// --- Call-deep census (acp lock-1 ruling 14250, item 4) ---
//
// The direct census above reads only the locked section itself, so it missed the one outbound call a locked section
// did reach: the dispatch pass's budget estimate (observeRun / enforceRun -> estimateChunk -> DispatchBudgetEstimateOn
// -> syncbudget.Loader.EstimateUnits -> load -> hydratePagerDuty -> hydratePagerDutyOnce), which hydrated a PagerDuty
// credential over the network under the bucket lock. Two walks close that:
//
//   - one call deep, every locked section: each function or method of the module a locked section calls (by name: a
//     plain call in the same package, pkg.F of an imported module package, x.M of any method M declared in the
//     same package or an imported module package; noise accepted) is read with every rule of the direct census.
//   - the estimate path, transitively: from the dispatch pass's locked section through syncdispatchruntime and
//     syncbudget, every reachable function is read for the outbound classes only (net, net/http, os/exec, time.Sleep,
//     and a credential hydration, token exchange or HTTP send by method name).
//
// Both walks are guard-aware for one statement form only: inside a function, `if <x>.locked != nil { ...; return }`
// (or `if locked != nil`) ends the walk of the statements after it in its block. That is the no-network guard of a
// locked estimate (syncbudget.pagerDutyStep.locked): what follows it runs only when the estimate is not under a lock.
// The census also requires the dispatch pass to set that lock context before its locked section starts.

const censusModulePath = "github.com/full-chaos/dev-health-ops"

// outboundMethodNames are the credential network steps the estimate path can reach through an interface.
var outboundMethodNames = map[string]string{
	"Hydrate":      "credential hydration (network)",
	"HydrateUntil": "credential hydration (network)",
	"Apply":        "token exchange (network)",
	"Do":           "HTTP send (network)",
}

type parsedFile struct {
	name    string
	src     []byte
	file    *ast.File
	dir     string
	imports map[string]string
}

// moduleIndex parses the module's packages on demand: from the repository, or from planted sources.
type moduleIndex struct {
	root    string
	fset    *token.FileSet
	pkgs    map[string][]*parsedFile
	planted map[string]map[string]string
}

func newModuleIndex(root string, planted map[string]map[string]string) *moduleIndex {
	return &moduleIndex{root: root, fset: token.NewFileSet(), pkgs: map[string][]*parsedFile{}, planted: planted}
}

func (index *moduleIndex) parse(dir, name string, src []byte) (*parsedFile, error) {
	file, err := parser.ParseFile(index.fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", name, err)
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
	return &parsedFile{name: name, src: src, file: file, dir: dir, imports: imports}, nil
}

// pkg is the non-test files of the package in dir (repository-relative).
func (index *moduleIndex) pkg(dir string) ([]*parsedFile, error) {
	if files, ok := index.pkgs[dir]; ok {
		return files, nil
	}
	var files []*parsedFile
	if index.planted != nil {
		names := make([]string, 0, len(index.planted[dir]))
		for name := range index.planted[dir] {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			parsed, err := index.parse(dir, name, []byte(index.planted[dir][name]))
			if err != nil {
				return nil, err
			}
			files = append(files, parsed)
		}
	} else {
		entries, err := os.ReadDir(filepath.Join(index.root, dir))
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			name := filepath.ToSlash(filepath.Join(dir, entry.Name()))
			src, err := os.ReadFile(filepath.Join(index.root, name))
			if err != nil {
				return nil, err
			}
			parsed, err := index.parse(dir, name, src)
			if err != nil {
				return nil, err
			}
			files = append(files, parsed)
		}
	}
	index.pkgs[dir] = files
	return files, nil
}

type funcRef struct {
	file *parsedFile
	decl *ast.FuncDecl
}

func moduleDir(path string) (string, bool) {
	if !strings.HasPrefix(path, censusModulePath+"/") {
		return "", false
	}
	return strings.TrimPrefix(path, censusModulePath+"/"), true
}

// declsNamed are the functions (method false) or methods (method true) named name in dir.
func (index *moduleIndex) declsNamed(dir, name string, method bool) ([]funcRef, error) {
	files, err := index.pkg(dir)
	if err != nil {
		return nil, err
	}
	var refs []funcRef
	for _, file := range files {
		for _, decl := range file.file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if ok && function.Body != nil && function.Name.Name == name && (function.Recv != nil) == method {
				refs = append(refs, funcRef{file, function})
			}
		}
	}
	return refs, nil
}

// targets are the module functions a call can reach, by name, in the packages allowed admits.
func (index *moduleIndex) targets(caller *parsedFile, call *ast.CallExpr, allowed func(string) bool) ([]funcRef, error) {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return index.declsNamed(caller.dir, fun.Name, false)
	case *ast.SelectorExpr:
		if pkg, ok := fun.X.(*ast.Ident); ok {
			if path, imported := caller.imports[pkg.Name]; imported {
				dir, inModule := moduleDir(path)
				if !inModule || !allowed(dir) {
					return nil, nil
				}
				return index.declsNamed(dir, fun.Sel.Name, false)
			}
		}
		refs, err := index.declsNamed(caller.dir, fun.Sel.Name, true)
		if err != nil {
			return nil, err
		}
		dirs := make([]string, 0, len(caller.imports))
		for _, path := range caller.imports {
			if dir, inModule := moduleDir(path); inModule && allowed(dir) && dir != caller.dir {
				dirs = append(dirs, dir)
			}
		}
		sort.Strings(dirs)
		for _, dir := range dirs {
			more, err := index.declsNamed(dir, fun.Sel.Name, true)
			if err != nil {
				return nil, err
			}
			refs = append(refs, more...)
		}
		return refs, nil
	}
	return nil, nil
}

type posRange struct{ from, to token.Pos }

// isLockGuard is `if <x>.locked != nil { ...; return ... }` or `if locked != nil { ...; return ... }`.
func isLockGuard(statement ast.Stmt) bool {
	guard, ok := statement.(*ast.IfStmt)
	if !ok || guard.Else != nil || len(guard.Body.List) == 0 {
		return false
	}
	if _, returns := guard.Body.List[len(guard.Body.List)-1].(*ast.ReturnStmt); !returns {
		return false
	}
	condition, ok := guard.Cond.(*ast.BinaryExpr)
	if !ok || condition.Op != token.NEQ {
		return false
	}
	if nilIdent, ok := condition.Y.(*ast.Ident); !ok || nilIdent.Name != "nil" {
		return false
	}
	switch subject := condition.X.(type) {
	case *ast.SelectorExpr:
		return subject.Sel.Name == "locked"
	case *ast.Ident:
		return subject.Name == "locked"
	}
	return false
}

// guardedRanges are the statements of body that follow a lock guard in their block: unreachable under the lock.
func guardedRanges(body *ast.BlockStmt) []posRange {
	var ranges []posRange
	mark := func(list []ast.Stmt) {
		for at, statement := range list {
			if isLockGuard(statement) && at+1 < len(list) {
				ranges = append(ranges, posRange{list[at+1].Pos(), list[len(list)-1].End()})
				return
			}
		}
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch block := node.(type) {
		case *ast.BlockStmt:
			mark(block.List)
		case *ast.CaseClause:
			mark(block.Body)
		case *ast.CommClause:
			mark(block.Body)
		}
		return true
	})
	return ranges
}

func inRanges(pos token.Pos, ranges []posRange) bool {
	for _, r := range ranges {
		if pos >= r.from && pos < r.to {
			return true
		}
	}
	return false
}

// deepCensus walks from the calls in [from, to) of caller's function into the functions they reach, up to maxDepth
// calls deep, and returns the banned calls in those functions (the section's own calls are the direct census's), the
// number of functions read and the number of calls read.
func (index *moduleIndex) deepCensus(caller *parsedFile, decl *ast.FuncDecl, from, to token.Pos, maxDepth int, outboundOnly bool, allowed func(string) bool) ([]censusHit, int, int, error) {
	type pending struct {
		ref   funcRef
		depth int
	}
	visited := map[*ast.FuncDecl]bool{decl: true}
	var queue []pending
	enqueue := func(file *parsedFile, body *ast.BlockStmt, lo, hi token.Pos, depth int) error {
		shadow := guardedRanges(body)
		var walkErr error
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || walkErr != nil || call.Pos() < lo || call.Pos() >= hi || inRanges(call.Pos(), shadow) {
				return true
			}
			refs, err := index.targets(file, call, allowed)
			if err != nil {
				walkErr = err
				return false
			}
			for _, ref := range refs {
				if !visited[ref.decl] {
					visited[ref.decl] = true
					queue = append(queue, pending{ref, depth})
				}
			}
			return true
		})
		return walkErr
	}
	if err := enqueue(caller, decl.Body, from, to, 1); err != nil {
		return nil, 0, 0, err
	}
	var hits []censusHit
	functions, calls := 0, 0
	for len(queue) > 0 {
		next := queue[0]
		queue = queue[1:]
		functions++
		body := next.ref.decl.Body
		shadow := guardedRanges(body)
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || inRanges(call.Pos(), shadow) {
				return true
			}
			calls++
			hits = append(hits, checkCall(index.fset, next.ref.file.name, next.ref.file.src, next.ref.file.imports, call, outboundOnly)...)
			return true
		})
		if next.depth < maxDepth {
			if err := enqueue(next.ref.file, body, body.Pos(), body.End(), next.depth+1); err != nil {
				return nil, 0, 0, err
			}
		}
	}
	return hits, functions, calls, nil
}

// section finds scope's locked section in the index: its file, function and [from, to).
func (index *moduleIndex) section(scope lockScope) ([]funcSection, error) {
	dir := filepath.ToSlash(filepath.Dir(scope.file))
	files, err := index.pkg(dir)
	if err != nil {
		return nil, err
	}
	var sections []funcSection
	for _, file := range files {
		if file.name != scope.file {
			continue
		}
		for _, decl := range file.file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil || (scope.function != "" && function.Name.Name != scope.function) {
				continue
			}
			from, to := function.Body.Pos(), function.Body.End()
			if scope.start != "" || scope.end != "" {
				base := index.fset.Position(from).Offset
				body := string(file.src[base:index.fset.Position(to).Offset])
				startAt := strings.Index(body, scope.start)
				endAt := strings.Index(body[max(startAt, 0):], scope.end)
				if startAt < 0 || endAt < 0 {
					return nil, fmt.Errorf("%s %s: a marker is not found", scope.file, scope.function)
				}
				to = from + token.Pos(startAt+endAt+len(scope.end))
				from += token.Pos(startAt)
			}
			sections = append(sections, funcSection{file, function, from, to})
		}
	}
	if len(sections) == 0 {
		return nil, fmt.Errorf("%s: function %q not found", scope.file, scope.function)
	}
	return sections, nil
}

type funcSection struct {
	file     *parsedFile
	decl     *ast.FuncDecl
	from, to token.Pos
}

// dispatchPassScope is the dispatch pass's locked section, walked transitively through the estimate path's packages.
func dispatchPassScope(t *testing.T) lockScope {
	t.Helper()
	for _, scope := range lockScopes {
		if scope.file == "internal/syncdispatchruntime/native_dispatch_sync_run_service.go" && scope.function == "Dispatch" {
			return scope
		}
	}
	t.Fatal("lockScopes has no dispatch pass section")
	return lockScope{}
}

var estimatePathPackages = map[string]bool{"internal/syncdispatchruntime": true, "internal/syncbudget": true}

const estimatePathMaxDepth = 32

func anyModulePackage(string) bool { return true }

// lockedContextIsSet: the dispatch pass runs its locked section on the lock context (the guard's precondition).
func lockedContextIsSet(index *moduleIndex, estimatePathScope lockScope) error {
	sections, err := index.section(lockScope{estimatePathScope.file, estimatePathScope.function, "", ""})
	if err != nil {
		return err
	}
	file, body := sections[0].file, sections[0].decl.Body
	text := string(file.src[index.fset.Position(body.Pos()).Offset:index.fset.Position(body.End()).Offset])
	setAt, lockAt := strings.Index(text, "ctx = preHydratePass("), strings.Index(text, estimatePathScope.start)
	if setAt < 0 || lockAt < 0 || setAt > lockAt {
		return fmt.Errorf("%s %s: `ctx = preHydratePass(` must come before %q", estimatePathScope.file, estimatePathScope.function, estimatePathScope.start)
	}
	refs, err := index.declsNamed(file.dir, "preHydratePass", false)
	if err != nil {
		return err
	}
	if len(refs) != 1 {
		return fmt.Errorf("preHydratePass: %d declarations, want 1", len(refs))
	}
	ref := refs[0]
	if !strings.Contains(string(ref.file.src[index.fset.Position(ref.decl.Body.Pos()).Offset:index.fset.Position(ref.decl.Body.End()).Offset]), "syncbudget.WithLockedHydrations(") {
		return fmt.Errorf("preHydratePass does not return syncbudget.WithLockedHydrations(...)")
	}
	return nil
}

func TestNoOutboundCallOneCallDeepOrOnTheEstimatePath(t *testing.T) {
	t.Run("planted", testDeepCensusFlagsAnUnguardedHydration)
	root := repoRoot(t)
	index := newModuleIndex(root, nil)
	var all []censusHit
	for _, scope := range lockScopes {
		sections, err := index.section(scope)
		if err != nil {
			t.Fatalf("census: %v", err)
		}
		for _, part := range sections {
			hits, functions, calls, err := index.deepCensus(part.file, part.decl, part.from, part.to, 1, false, anyModulePackage)
			if err != nil {
				t.Fatalf("census: %v", err)
			}
			t.Logf("one call deep: %s %s: %d functions, %d calls, %d banned", scope.file, part.decl.Name.Name, functions, calls, len(hits))
			all = append(all, hits...)
		}
	}
	estimatePathScope := dispatchPassScope(t)
	if err := lockedContextIsSet(index, estimatePathScope); err != nil {
		t.Fatalf("census: %v", err)
	}
	sections, err := index.section(estimatePathScope)
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	part := sections[0]
	hits, functions, calls, err := index.deepCensus(part.file, part.decl, part.from, part.to, estimatePathMaxDepth, true,
		func(dir string) bool { return estimatePathPackages[dir] })
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	if functions < 20 {
		t.Fatalf("estimate path: %d functions read, want the pass's whole locked call tree (a resolution broke)", functions)
	}
	t.Logf("estimate path (transitive, outbound classes): %s %s: %d functions, %d calls, %d banned", estimatePathScope.file, estimatePathScope.function, functions, calls, len(hits))
	all = append(all, hits...)
	for _, hit := range all {
		t.Errorf("outbound call under the bucket lock: %s", hit)
	}
	if len(all) == 0 {
		t.Logf("0 outbound under lock")
	}
}

// The planted estimate path: a locked section calls estimate (another package), which calls hydrate. Unguarded, the
// walk flags the http call and the Apply two calls deep; behind the lock guard, it flags nothing.
const plantedDispatchSource = `package pass

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/plantedbudget"
	"github.com/jackc/pgx/v5"
)

func Dispatch(ctx context.Context, tx pgx.Tx) error {
	ctx = preHydratePass(ctx)
	if err := authorizeRun(ctx, tx); err != nil {
		return err
	}
	if err := observe(ctx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func observe(ctx context.Context) error {
	var loader plantedbudget.Loader
	return loader.Estimate(ctx)
}
`

const plantedBudgetSource = `package plantedbudget

import (
	"context"
	"net/http"
)

type Loader struct{ auth interface{ Apply(*http.Request) error } }

type step struct{ locked *int }

func (loader Loader) Estimate(ctx context.Context) error { return loader.hydrate(ctx, step{}) }

func (loader Loader) hydrate(ctx context.Context, s step) error {
	GUARD
	probe, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.invalid/", nil)
	if err != nil {
		return err
	}
	return loader.auth.Apply(probe)
}
`

func testDeepCensusFlagsAnUnguardedHydration(t *testing.T) {
	scope := lockScope{"internal/plantedpass/pass.go", "Dispatch", "authorizeRun(", "tx.Commit(ctx)"}
	for _, guarded := range []bool{false, true} {
		guard := ""
		if guarded {
			guard = "if s.locked != nil {\n\t\treturn nil\n\t}"
		}
		index := newModuleIndex("", map[string]map[string]string{
			"internal/plantedpass":   {"internal/plantedpass/pass.go": plantedDispatchSource},
			"internal/plantedbudget": {"internal/plantedbudget/budget.go": strings.Replace(plantedBudgetSource, "GUARD", guard, 1)},
		})
		sections, err := index.section(scope)
		if err != nil {
			t.Fatal(err)
		}
		part := sections[0]
		// One call deep reaches observe only: the hydration is three calls deep.
		shallow, _, _, err := index.deepCensus(part.file, part.decl, part.from, part.to, 1, false, anyModulePackage)
		if err != nil {
			t.Fatal(err)
		}
		if len(shallow) != 0 {
			t.Fatalf("planted, one call deep: %v, want none", shallow)
		}
		hits, _, _, err := index.deepCensus(part.file, part.decl, part.from, part.to, estimatePathMaxDepth, true,
			func(dir string) bool { return dir == "internal/plantedpass" || dir == "internal/plantedbudget" })
		if err != nil {
			t.Fatal(err)
		}
		if guarded {
			if len(hits) != 0 {
				t.Fatalf("planted hydration behind the lock guard: %v, want none", hits)
			}
			t.Logf("guarded planted hydration: no hit")
			continue
		}
		var calls []string
		for _, hit := range hits {
			calls = append(calls, hit.call)
		}
		if strings.Join(calls, " ") != "http.NewRequestWithContext( loader.auth.Apply(" {
			t.Fatalf("planted unguarded hydration: hits %v, want http.NewRequestWithContext( and loader.auth.Apply(", hits)
		}
		t.Logf("unguarded planted hydration found: %v", hits)
	}
}
