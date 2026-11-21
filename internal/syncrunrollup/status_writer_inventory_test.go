package syncrunrollup

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The queued provider-unit job of a closed unit completes without work only because no writer moves a failed
// sync unit back to a claimable status, and no writer moves a terminal sync run back to a non-terminal one.
// This census finds every non-test Go statement that writes the status column of public.sync_run_units or
// public.sync_runs and compares the set with statusWriterInventory: a writer that is not listed, a listed writer
// that is gone, a changed statement count and a changed SET target all fail.
//
// How a writer is found: go/parser reads every non-test .go file under internal/ and cmd/. Each string
// expression (a literal, a + chain of literals, and package-level string constants of the same package) is lexed
// into whole SQL tokens: comments are dropped, a quoted literal is one token, a quoted identifier keeps its case.
// A writer is `UPDATE [public.]<table> ... SET ... [alias.]status = <target>` or `INSERT INTO [public.]<table>
// (... status ...)`. A `$N` target is bound to the N-th parameter of the call that runs the statement (the
// arguments after the SQL argument, a []any literal expanded); a package-level constant parameter is pinned by
// its name, anything else reads "runtime".
//
// NOT pinned here: SQL built at runtime (Sprintf, a table name in a variable, a constant from another package);
// SQL outside non-test Go files (migrations, scripts, hand SQL); `SET (status, ...) = (...)`; a database trigger or
// function; a new caller of a listed statement (same key); and, by design, the WHERE guard of a listed writer:
// a writer that loses or widens its old-status guard keeps its key. That needs a behavior cell per writer
// (cell column below). A Go string that only spells such a statement (a log text) reads as a writer.

// statusWriterClass says why a writer cannot re-arm a failed unit or a terminal run.
type statusWriterClass string

const (
	// newRow inserts a row with a new id (or ON CONFLICT (id) DO NOTHING): it never touches an existing row.
	newRow statusWriterClass = "new row"
	// seedRow is a test-support insert (imported only by tests).
	seedRow statusWriterClass = "test seed"
	// guardedUnit updates units only from planned / retrying / dispatching / running (never failed).
	guardedUnit statusWriterClass = "unit writer guarded on a non-failed old status"
	// terminalRun writes a terminal run status only (success / partial_failed / failed).
	terminalRun statusWriterClass = "run writer with a terminal target"
	// queuedOnlyRun writes `dispatching` with no SQL guard on the old run status; it runs only when this pass
	// claimed a unit, and a terminal run has no claimable unit.
	queuedOnlyRun statusWriterClass = "run writer safe through the unit rule"
)

// noBehaviorCellYet marks a writer whose "a failed unit / a terminal run does not change" cell is a follow-up.
const noBehaviorCellYet = "no behavior cell yet (follow-up)"

type statusWriterRow struct {
	class   statusWriterClass
	targets []string // one SET target per statement, sorted; "-" for an INSERT
	cell    string   // "<package dir>|<TestName>" of a cell that applies this writer to a failed unit or terminal run
}

// statusWriterInventory is keyed "<file>|<func or pkgdecl:CONST>|<table>|<UPDATE or INSERT>".
var statusWriterInventory = map[string]statusWriterRow{
	"internal/fixturescli/fixturescli.go|Seed|sync_run_units|INSERT": {newRow, []string{"-"}, noBehaviorCellYet},
	"internal/fixturescli/fixturescli.go|Seed|sync_runs|INSERT":      {newRow, []string{"-"}, noBehaviorCellYet},

	"internal/providersync/repository_postgres.go|pkgdecl:claimUnitSQL|sync_run_units|UPDATE": {guardedUnit, []string{"'running'"},
		"internal/syncdispatchruntime|TestQueuedJobOfACancelledUnitCompletesOnItsFirstAttemptWithoutADomainWrite"},
	"internal/providersync/repository_postgres.go|pkgdecl:completeUnitSQL|sync_run_units|UPDATE":              {guardedUnit, []string{"'success'"}, noBehaviorCellYet},
	"internal/providersync/repository_postgres.go|pkgdecl:deferForBudgetContentionSQL|sync_run_units|UPDATE":  {guardedUnit, []string{"'dispatching'"}, noBehaviorCellYet},
	"internal/providersync/repository_postgres.go|pkgdecl:deferForChunkContinuationSQL|sync_run_units|UPDATE": {guardedUnit, []string{"'dispatching'"}, noBehaviorCellYet},
	"internal/providersync/repository_postgres.go|pkgdecl:deferForRateLimitSQL|sync_run_units|UPDATE":         {guardedUnit, []string{"'dispatching'"}, noBehaviorCellYet},
	"internal/providersync/repository_postgres.go|pkgdecl:failUnitSQL|sync_run_units|UPDATE":                  {guardedUnit, []string{"'failed'"}, noBehaviorCellYet},
	"internal/providersync/repository_postgres.go|pkgdecl:releaseForRetrySQL|sync_run_units|UPDATE":           {guardedUnit, []string{"'dispatching'"}, noBehaviorCellYet},

	"internal/scheduler/sync/materializer.go|persistDomainGraph|sync_runs|INSERT": {newRow, []string{"-"},
		"internal/syncdispatchruntime|TestFeatureReEnableMintsANewRunAndTheOldFailedUnitStaysFailed"},
	"internal/scheduler/sync/materializer.go|persistPlannedUnits|sync_run_units|INSERT": {newRow, []string{"-"},
		"internal/syncdispatchruntime|TestFeatureReEnableMintsANewRunAndTheOldFailedUnitStaysFailed"},

	"internal/syncdispatchruntime/budget_cas.go|applyCooldownDeferral|sync_run_units|UPDATE":                                  {guardedUnit, []string{"syncRunUnitStatusRetrying"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/budget_cas.go|deferUnitForBudget|sync_run_units|UPDATE":                                     {guardedUnit, []string{"syncRunUnitStatusRetrying"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/budget_chokepoint.go|terminalizeUnit|sync_run_units|UPDATE":                                 {guardedUnit, []string{"syncRunUnitStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/claim_units.go|claimUnits|sync_run_units|UPDATE":                                            {guardedUnit, []string{"syncRunUnitStatusDispatching", "syncRunUnitStatusDispatching"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/dispatch_denial.go|failPlannedUnits|sync_run_units|UPDATE":                                  {guardedUnit, []string{"syncRunUnitStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/dispatch_denial.go|failStaleDispatchingUnits|sync_run_units|UPDATE":                         {guardedUnit, []string{"syncRunUnitStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/dispatch_denial.go|terminalizeInvalidClaimUnits|sync_run_units|UPDATE":                      {guardedUnit, []string{"syncRunUnitStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/dispatch_denial.go|terminalizeUnroutableUnits|sync_run_units|UPDATE":                        {guardedUnit, []string{"syncRunUnitStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/feature_disabled_termination.go|terminalizeRun|sync_run_units|UPDATE":                       {guardedUnit, []string{"syncRunUnitStatusFailed", "syncRunUnitStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/feature_disabled_termination.go|terminalizeRun|sync_runs|UPDATE":                            {terminalRun, []string{"syncRunStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/native_dispatch_sync_run_service.go|NativeDispatchSyncRunService.Dispatch|sync_runs|UPDATE": {queuedOnlyRun, []string{"syncRunStatusDispatching"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/native_dispatch_sync_run_service.go|NativeDispatchSyncRunService.denyRun|sync_runs|UPDATE":  {terminalRun, []string{"syncRunStatusFailed"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/native_finalize_sync_run.go|writeFinalizeRun|sync_runs|UPDATE":                              {terminalRun, []string{"runtime"}, noBehaviorCellYet},
	"internal/syncdispatchruntime/native_reference_discovery.go|failNonterminalUnits|sync_run_units|UPDATE":                   {guardedUnit, []string{"syncRunUnitStatusFailed"}, noBehaviorCellYet},

	"internal/syncreconciler/lease_repair.go|pkgdecl:markExpiredLeaseFailedSQL|sync_run_units|UPDATE":                       {guardedUnit, []string{"'failed'"}, noBehaviorCellYet},
	"internal/syncreconciler/lease_repair.go|pkgdecl:markExpiredLeaseRetryingSQL|sync_run_units|UPDATE":                     {guardedUnit, []string{"'retrying'"}, noBehaviorCellYet},
	"internal/syncreconciler/route_unavailable_termination.go|pkgdecl:terminalizeRouteUnavailableSQL|sync_run_units|UPDATE": {guardedUnit, []string{"'failed'"}, noBehaviorCellYet},
	"internal/syncreconciler/unreclaimable_sweep.go|pkgdecl:terminalizeTerminalDeliverySQL|sync_run_units|UPDATE":           {guardedUnit, []string{"'failed'"}, noBehaviorCellYet},
	"internal/syncreconciler/unreclaimable_sweep.go|pkgdecl:terminalizeUnreclaimableSQL|sync_run_units|UPDATE":              {guardedUnit, []string{"'failed'"}, noBehaviorCellYet},

	"internal/testsupport/pgseed/sync.go|EnsureSyncRun|sync_runs|INSERT":          {seedRow, []string{"-"}, noBehaviorCellYet},
	"internal/testsupport/pgseed/sync.go|InsertSyncRunUnit|sync_run_units|INSERT": {seedRow, []string{"-"}, noBehaviorCellYet},
}

func TestSyncStatusWritersMatchTheInventory(t *testing.T) {
	root := filepath.Join("..", "..")
	found := scanStatusWriters(t, root, "internal", "cmd")
	var keys []string
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		row, ok := statusWriterInventory[key]
		if !ok {
			t.Errorf("unlisted status writer %s (SET targets %v): it can move a failed sync unit or a terminal sync run "+
				"back to a claimable status unless its WHERE guard says otherwise. Add a row with its class and a behavior "+
				"cell that applies it to a failed unit (or a terminal run) and finds the row unchanged.", key, found[key])
			continue
		}
		if strings.Join(found[key], ",") != strings.Join(row.targets, ",") {
			t.Errorf("status writer %s: SET targets %v, inventory %v (a changed target or statement count needs a reviewed row)",
				key, found[key], row.targets)
		}
	}
	for key := range statusWriterInventory {
		if _, ok := found[key]; !ok {
			t.Errorf("stale inventory row %s: no such status writer on this tree; remove the row only with its writer", key)
		}
	}
}

// TestSyncStatusWriterInventoryRowsAreConsistent checks each row's class against its table and verb, and that a
// bound behavior cell exists as a Test func in the named package.
func TestSyncStatusWriterInventoryRowsAreConsistent(t *testing.T) {
	root := filepath.Join("..", "..")
	testFuncs := map[string]map[string]bool{}
	for key, row := range statusWriterInventory {
		parts := strings.Split(key, "|")
		if len(parts) != 4 {
			t.Errorf("row %s: want <file>|<func>|<table>|<verb>", key)
			continue
		}
		table, verb := parts[2], parts[3]
		switch row.class {
		case newRow, seedRow:
			if verb != "INSERT" {
				t.Errorf("row %s: class %q is for an INSERT", key, row.class)
			}
		case guardedUnit:
			if table != "sync_run_units" || verb != "UPDATE" {
				t.Errorf("row %s: class %q is for a sync_run_units UPDATE", key, row.class)
			}
		case terminalRun, queuedOnlyRun:
			if table != "sync_runs" || verb != "UPDATE" {
				t.Errorf("row %s: class %q is for a sync_runs UPDATE", key, row.class)
			}
		default:
			t.Errorf("row %s: unknown class %q", key, row.class)
		}
		if row.class == seedRow && !strings.HasPrefix(parts[0], "internal/testsupport/") {
			t.Errorf("row %s: a test seed lives under internal/testsupport/", key)
		}
		if row.cell == noBehaviorCellYet {
			continue
		}
		dir, name, ok := strings.Cut(row.cell, "|")
		if !ok {
			t.Errorf("row %s: cell %q, want <package dir>|<TestName> or %q", key, row.cell, noBehaviorCellYet)
			continue
		}
		if testFuncs[dir] == nil {
			testFuncs[dir] = testFuncNames(t, filepath.Join(root, filepath.FromSlash(dir)))
		}
		if !testFuncs[dir][name] {
			t.Errorf("row %s: behavior cell %s is not a Test func in %s", key, name, dir)
		}
	}
}

// TestSyncStatusWriterScanFindsPlantedWriters is the scan's own control: planted writers in every supported shape
// are found with their SET targets, and statements that do not write the status column are not.
func TestSyncStatusWriterScanFindsPlantedWriters(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "planted")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := "package planted\n\n" +
		"const failedStatus = \"failed\"\n" +
		"const plannedStatus = \"planned\"\n\n" +
		"const literalSQL = `\nUPDATE public.sync_run_units AS unit\nSET attempts = 0, status = 'planned' -- comment: status = 'x'\nWHERE unit.status = 'failed'`\n\n" +
		"const headSQL = `UPDATE sync_runs `\n" +
		"const splitSQL = headSQL + `SET \"status\" = 'dispatching' WHERE id = $1`\n\n" +
		"type execer interface{ Exec(sql string, args ...any) error }\n\n" +
		"func constForm(db execer, id string) {\n\tdb.Exec(`UPDATE public.sync_run_units SET status = $2 WHERE id = $1 AND status = $3`, id, plannedStatus, failedStatus)\n}\n\n" +
		"func localForm(db execer, id, next string) {\n\tstmt := `WITH x AS (UPDATE public.sync_runs AS run SET run.status = $2 WHERE run.id = $1 RETURNING 1) SELECT 1`\n\tdb.Exec(stmt, id, next)\n}\n\n" +
		"func run(db execer, build func(string) string) {}\n\n" +
		"func builderForm(db execer) {\n\trun(db, func(ids string) string {\n\t\treturn `UPDATE public.sync_run_units SET status = $2 WHERE status = $3` + ids\n\t}, []any{\"run\", plannedStatus, failedStatus})\n}\n\n" +
		"func insertForm(db execer) {\n\tdb.Exec(`INSERT INTO public.sync_run_units (id, status) VALUES ($1, 'planned') ON CONFLICT (id) DO NOTHING`)\n}\n\n" +
		"func notWriters(db execer) {\n" +
		"\tdb.Exec(`SELECT status FROM public.sync_runs WHERE id = $1 FOR UPDATE`)\n" +
		"\tdb.Exec(`UPDATE public.sync_run_units SET result = '{\"status\": 1}' WHERE status = 'failed'`)\n" +
		"\tdb.Exec(`UPDATE public.sync_runs SET error = $2 WHERE id = $1 -- SET status = 'planned'`)\n" +
		"\tdb.Exec(`UPDATE public.sync_runs_archive SET status = 'planned'`)\n" +
		"\tdb.Exec(`UPDATE public.sync_runs SET completed_units = (SELECT count(*) FROM public.sync_run_units WHERE status = 'success')`)\n" +
		"}\n"
	if err := os.WriteFile(filepath.Join(dir, "planted.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "planted_test.go"), []byte("package planted\n\nconst inTest = `UPDATE sync_runs SET status = 'planned'`\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := scanStatusWriters(t, root, "internal")
	want := map[string][]string{
		"internal/planted/planted.go|pkgdecl:literalSQL|sync_run_units|UPDATE": {"'planned'"},
		"internal/planted/planted.go|pkgdecl:splitSQL|sync_runs|UPDATE":        {"'dispatching'"},
		"internal/planted/planted.go|constForm|sync_run_units|UPDATE":          {"plannedStatus"},
		"internal/planted/planted.go|localForm|sync_runs|UPDATE":               {"runtime"},
		"internal/planted/planted.go|builderForm|sync_run_units|UPDATE":        {"plannedStatus"},
		"internal/planted/planted.go|insertForm|sync_run_units|INSERT":         {"-"},
	}
	for key, targets := range want {
		if strings.Join(got[key], ",") != strings.Join(targets, ",") {
			t.Errorf("planted %s: got %v, want %v", key, got[key], targets)
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			t.Errorf("planted tree: %s read as a status writer", key)
		}
	}
}

// scanStatusWriters returns "<file>|<owner>|<table>|<verb>" -> sorted SET targets, one per statement.
func scanStatusWriters(t *testing.T, root string, tops ...string) map[string][]string {
	t.Helper()
	byDir := map[string][]string{}
	for _, top := range tops {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				byDir[filepath.Dir(path)] = append(byDir[filepath.Dir(path)], path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	found := map[string][]string{}
	for _, paths := range byDir {
		scanStatusWriterPackage(t, root, paths, found)
	}
	for key := range found {
		sort.Strings(found[key])
	}
	return found
}

type swFile struct {
	rel  string
	file *ast.File
}

func scanStatusWriterPackage(t *testing.T, root string, paths []string, found map[string][]string) {
	t.Helper()
	fset := token.NewFileSet()
	var files []swFile
	consts := map[string]ast.Expr{}
	constNames := map[string]bool{}
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, swFile{rel: filepath.ToSlash(rel), file: file})
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					constNames[name.Name] = true
					if i < len(value.Values) {
						consts[name.Name] = value.Values[i]
					}
				}
			}
		}
	}
	scan := &swPackage{consts: consts, constNames: constNames}
	for _, f := range files {
		scan.collectCalls(f.file)
	}
	for _, f := range files {
		for _, decl := range f.file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					scan.scanFunc(f.rel, swOwner(d), d.Body, found)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					value, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, name := range value.Names {
						if i >= len(value.Values) {
							continue
						}
						sql, ok := scan.fold(value.Values[i], true)
						if !ok {
							continue
						}
						for _, write := range swStatusWrites(sql) {
							key := f.rel + "|pkgdecl:" + name.Name + "|" + write.table + "|" + write.verb
							found[key] = append(found[key], scan.bindTarget(write.target, scan.callsUsing(name.Name, nil)))
						}
					}
				}
			}
		}
	}
}

type swCall struct {
	owner *ast.BlockStmt
	call  *ast.CallExpr
}

type swPackage struct {
	consts     map[string]ast.Expr
	constNames map[string]bool
	calls      []swCall
}

func (p *swPackage) collectCalls(file *ast.File) {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				p.calls = append(p.calls, swCall{owner: fn.Body, call: call})
			}
			return true
		})
	}
}

// fold reads a string expression: a literal, a + chain, a parenthesized one, and (when resolve) a same-package
// string constant. Other operands read as a blank. ok is false when no literal text was read at all.
func (p *swPackage) fold(expr ast.Expr, resolve bool) (string, bool) {
	return p.foldDepth(expr, resolve, 0)
}

func (p *swPackage) foldDepth(expr ast.Expr, resolve bool, depth int) (string, bool) {
	if depth > 16 {
		return "", false
	}
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		text, err := strconv.Unquote(e.Value)
		return text, err == nil
	case *ast.ParenExpr:
		return p.foldDepth(e.X, resolve, depth+1)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, lok := p.foldDepth(e.X, resolve, depth+1)
		right, rok := p.foldDepth(e.Y, resolve, depth+1)
		if !lok && !rok {
			return "", false
		}
		return left + " " + right, true
	case *ast.Ident:
		if value, ok := p.consts[e.Name]; ok && resolve {
			return p.foldDepth(value, resolve, depth+1)
		}
	}
	return "", false
}

// scanFunc reads each string expression in a function body that holds literal text; a same-package constant is
// folded only inside an expression that also has literal text, so a bare use of a listed constant is not a
// second writer.
func (p *swPackage) scanFunc(rel, owner string, body *ast.BlockStmt, found map[string][]string) {
	var stack []ast.Node
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		expr, isExpr := n.(ast.Expr)
		if isExpr && swHasLiteral(expr) {
			if sql, ok := p.fold(expr, true); ok {
				for _, write := range swStatusWrites(sql) {
					key := rel + "|" + owner + "|" + write.table + "|" + write.verb
					found[key] = append(found[key], p.bindTarget(write.target, p.callsFor(expr, stack, body)))
				}
				return false
			}
		}
		stack = append(stack, n)
		return true
	})
}

func swHasLiteral(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.BasicLit:
		return e.Kind == token.STRING
	case *ast.ParenExpr:
		return swHasLiteral(e.X)
	case *ast.BinaryExpr:
		return e.Op == token.ADD && (swHasLiteral(e.X) || swHasLiteral(e.Y))
	}
	return false
}

// swParams is the parameter list ($1, $2, ...) of one call that runs a statement.
type swParams []ast.Expr

// callsFor finds the calls that run a statement read in a function: the call it is an argument of, the call a
// func literal returning it is an argument of, or the calls of the same function that use the local name it is
// assigned to.
func (p *swPackage) callsFor(expr ast.Expr, stack []ast.Node, body *ast.BlockStmt) []swParams {
	for i := len(stack) - 1; i >= 0; i-- {
		switch parent := stack[i].(type) {
		case *ast.CallExpr:
			if params, ok := swParamsAfter(parent, expr); ok {
				return []swParams{params}
			}
		case *ast.AssignStmt:
			for j, rhs := range parent.Rhs {
				if rhs == expr && j < len(parent.Lhs) {
					if name, ok := parent.Lhs[j].(*ast.Ident); ok {
						return p.callsUsing(name.Name, body)
					}
				}
			}
			return nil
		case *ast.ValueSpec:
			for j, value := range parent.Values {
				if value == expr && j < len(parent.Names) {
					return p.callsUsing(parent.Names[j].Name, body)
				}
			}
			return nil
		case *ast.FuncLit:
			for k := i - 1; k >= 0; k-- {
				if call, ok := stack[k].(*ast.CallExpr); ok {
					if params, ok := swParamsAfter(call, parent); ok {
						return []swParams{params}
					}
				}
			}
			return nil
		case *ast.BlockStmt, *ast.ExprStmt, *ast.ReturnStmt, *ast.IfStmt:
			continue
		}
	}
	return nil
}

// callsUsing lists the calls (in body, or in the whole package when body is nil) that pass the name as an
// argument, alone or in a + chain.
func (p *swPackage) callsUsing(name string, body *ast.BlockStmt) []swParams {
	var out []swParams
	for _, c := range p.calls {
		if body != nil && c.owner != body {
			continue
		}
		for i, arg := range c.call.Args {
			if swNames(arg, name) {
				out = append(out, swExpand(c.call.Args[i+1:]))
				break
			}
		}
	}
	return out
}

func swNames(expr ast.Expr, name string) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name == name
	case *ast.ParenExpr:
		return swNames(e.X, name)
	case *ast.BinaryExpr:
		return e.Op == token.ADD && (swNames(e.X, name) || swNames(e.Y, name))
	}
	return false
}

func swParamsAfter(call *ast.CallExpr, arg ast.Node) (swParams, bool) {
	for i, a := range call.Args {
		if a.Pos() <= arg.Pos() && arg.End() <= a.End() {
			return swExpand(call.Args[i+1:]), true
		}
	}
	return nil, false
}

// swExpand lists call arguments as statement parameters; an untyped or []any composite literal adds its elements.
func swExpand(args []ast.Expr) swParams {
	var out swParams
	for _, arg := range args {
		if lit, ok := arg.(*ast.CompositeLit); ok {
			if array, ok := lit.Type.(*ast.ArrayType); ok {
				if elem, ok := array.Elt.(*ast.Ident); ok && elem.Name == "any" {
					out = append(out, lit.Elts...)
					continue
				}
			}
		}
		out = append(out, arg)
	}
	return out
}

// bindTarget pins a SET target: a literal as written, a $N as the same-package constant passed in that place by
// every call that runs it, else "runtime".
func (p *swPackage) bindTarget(target string, calls []swParams) string {
	if target == "-" || !strings.HasPrefix(target, "$") {
		return target
	}
	n, err := strconv.Atoi(target[1:])
	if err != nil || n < 1 || len(calls) == 0 {
		return "runtime"
	}
	bound := ""
	for _, params := range calls {
		if n > len(params) {
			return "runtime"
		}
		name, ok := params[n-1].(*ast.Ident)
		if !ok || !p.constNames[name.Name] {
			return "runtime"
		}
		if bound != "" && bound != name.Name {
			return "runtime"
		}
		bound = name.Name
	}
	return bound
}

func swOwner(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if generic, ok := recv.(*ast.IndexExpr); ok {
		recv = generic.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

type swToken struct {
	text    string
	literal bool
}

// swTokens lexes SQL into whole tokens: lowercase words ($N included), quoted identifiers as written, one token
// per string literal (its text kept), one token per other rune. Comments are dropped.
func swTokens(sql string) []swToken {
	var out []swToken
	isWord := func(c byte) bool {
		return c == '_' || c == '$' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
	}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			depth := 0
			for i < len(sql) {
				if strings.HasPrefix(sql[i:], "/*") {
					depth++
					i += 2
					continue
				}
				if strings.HasPrefix(sql[i:], "*/") {
					depth--
					i += 2
					if depth == 0 {
						break
					}
					continue
				}
				i++
			}
		case c == '\'':
			var text strings.Builder
			j := i + 1
			for j < len(sql) {
				if sql[j] == '\'' {
					if j+1 < len(sql) && sql[j+1] == '\'' {
						text.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				text.WriteByte(sql[j])
				j++
			}
			out = append(out, swToken{text: text.String(), literal: true})
			i = j + 1
		case c == '"':
			end := strings.IndexByte(sql[i+1:], '"')
			if end < 0 {
				return out
			}
			out = append(out, swToken{text: sql[i+1 : i+1+end]})
			i += end + 2
		case isWord(c):
			j := i
			for j < len(sql) && isWord(sql[j]) {
				j++
			}
			out = append(out, swToken{text: strings.ToLower(sql[i:j])})
			i = j
		default:
			out = append(out, swToken{text: string(c)})
			i++
		}
	}
	return out
}

type swWrite struct{ table, verb, target string }

func swWord(tokens []swToken, i int, word string) bool {
	return i < len(tokens) && !tokens[i].literal && tokens[i].text == word
}

// swTable reads [public .] sync_runs | sync_run_units at i; it returns the table and the index after it.
func swTable(tokens []swToken, i int) (string, int) {
	if swWord(tokens, i, "public") && swWord(tokens, i+1, ".") {
		i += 2
	}
	if swWord(tokens, i, "sync_runs") || swWord(tokens, i, "sync_run_units") {
		return tokens[i].text, i + 1
	}
	return "", i
}

// swStatusWrites lists the statements of one SQL text that write the status column of the two tables.
func swStatusWrites(sql string) []swWrite {
	tokens := swTokens(sql)
	var out []swWrite
	for i := range tokens {
		switch {
		case swWord(tokens, i, "update"):
			table, j := swTable(tokens, i+1)
			if table == "" {
				continue
			}
			for j < len(tokens) && !swWord(tokens, j, "set") {
				j++
			}
			depth := 0
			for k := j + 1; k < len(tokens); k++ {
				if swWord(tokens, k, "(") {
					depth++
					continue
				}
				if swWord(tokens, k, ")") {
					if depth == 0 {
						break
					}
					depth--
					continue
				}
				if depth > 0 {
					continue
				}
				if swWord(tokens, k, "where") || swWord(tokens, k, "from") || swWord(tokens, k, "returning") || swWord(tokens, k, ";") {
					break
				}
				if !swWord(tokens, k, "status") || !swWord(tokens, k+1, "=") || k+2 >= len(tokens) {
					continue
				}
				head := k - 1
				if swWord(tokens, head, ".") {
					head -= 2
				}
				if !swWord(tokens, head, "set") && !swWord(tokens, head, ",") {
					continue
				}
				target := tokens[k+2]
				if target.literal {
					out = append(out, swWrite{table, "UPDATE", "'" + target.text + "'"})
				} else {
					out = append(out, swWrite{table, "UPDATE", target.text})
				}
				break
			}
		case swWord(tokens, i, "insert") && swWord(tokens, i+1, "into"):
			table, j := swTable(tokens, i+2)
			if table == "" || !swWord(tokens, j, "(") {
				continue
			}
			for k := j + 1; k < len(tokens) && !swWord(tokens, k, ")"); k++ {
				if swWord(tokens, k, "status") {
					out = append(out, swWrite{table, "INSERT", "-"})
					break
				}
			}
		}
	}
	return out
}

// testFuncNames lists the Test funcs of a package's _test.go files, whatever their build tags.
func testFuncNames(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	names := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Test") {
				names[fn.Name.Name] = true
			}
		}
	}
	return names
}
