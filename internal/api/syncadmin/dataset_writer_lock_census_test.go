package syncadmin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// datasetWriterLockOwners names, for every writer of integration_datasets
// rows in datasetStateWriters, the function that takes the selection lock
// (providersync.LockIntegrationSelection, one hierarchy for every writer) and
// the call it makes to reach the writer. via is empty when the owner holds the
// row statement itself. The lock call must come BEFORE that statement or call
// in the owner's body.
var datasetWriterLockOwners = map[string]struct{ owner, via string }{
	"internal/api/syncadmin.applySelectionChange":       {"internal/api/syncadmin.updateSyncConfigTx", "saveSelection"},
	"internal/api/syncadmin.createPlannerManagedConfig": {"internal/api/syncadmin.createPlannerManagedConfig", ""},
	"internal/api/syncadmin.repairPagerDutyDatasets":    {"internal/api/syncadmin.createPlannerManagedConfig", "repairPagerDutyOperationalIntegration"},
	"internal/api/integrationsadmin.applyDataset":       {"internal/api/integrationsadmin.updateDatasets", "applyDataset"},
}

// datasetWriterLockExempt are the writers that do not take the lock, each
// with the proof.
var datasetWriterLockExempt = map[string]string{
	"internal/scheduler/sync.applyDomainPlanMutations": "runs inside the coordinator transaction, which already holds FOR UPDATE on the occurrence's configuration row; a save holds the selection lock and then updates that row, so a blocking selection lock here would close a cycle. Its statements are an insert-only security row (ON CONFLICT DO NOTHING) and the convergent PagerDuty repair",
	"internal/fixturescli.Seed":                        "the fixtures verb seeds a synthetic integration that no route can reach",
}

var selectionLockCalls = map[string]bool{
	"LockIntegrationSelection": true, "LockIntegrationSelections": true, "lockConfigSelections": true,
}

func callName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

// findFunction parses the non-test files of the package directory of
// "<dir>.<name>" and returns the declaration of the function (or method).
func findFunction(t *testing.T, repoRoot, key string) *ast.FuncDecl {
	t.Helper()
	cut := strings.LastIndex(key, ".")
	dir, name := key[:cut], key[cut+1:]
	files, err := filepath.Glob(filepath.Join(repoRoot, filepath.FromSlash(dir), "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no Go files under %s: nothing is measured", dir)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, declaration := range parsed.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == name && function.Body != nil {
				return function
			}
		}
	}
	t.Fatalf("function %s not found: nothing is measured", key)
	return nil
}

// positions returns the first position of a selection lock call and of the
// first row write (the statement literal when via is empty, else the call to
// via) in the function body; zero means not found.
func positions(function *ast.FuncDecl, via string) (lock, write token.Pos) {
	ast.Inspect(function.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			name := callName(typed)
			if selectionLockCalls[name] && (lock == 0 || typed.Pos() < lock) {
				lock = typed.Pos()
			}
			if via != "" && name == via && (write == 0 || typed.Pos() < write) {
				write = typed.Pos()
			}
		case *ast.BasicLit:
			if via == "" && typed.Kind == token.STRING {
				if text, err := strconv.Unquote(typed.Value); err == nil && datasetStateWrite.MatchString(text) && (write == 0 || typed.Pos() < write) {
					write = typed.Pos()
				}
			}
		}
		return true
	})
	return lock, write
}

// TestEveryDatasetWriterTakesTheSelectionLockFirst: each writer in the census
// is either exempt with a proof, or reached through an owner that takes the
// selection lock before the write. A function or call that is not found
// fails: it measured nothing.
func TestEveryDatasetWriterTakesTheSelectionLockFirst(t *testing.T) {
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("no caller file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	var covered []string
	for writer, owned := range datasetWriterLockOwners {
		covered = append(covered, writer)
		lock, write := positions(findFunction(t, repoRoot, owned.owner), owned.via)
		switch {
		case write == 0:
			t.Errorf("%s: the owner %s has no write (via %q): nothing is measured", writer, owned.owner, owned.via)
		case lock == 0:
			t.Errorf("%s: the owner %s never takes the selection lock", writer, owned.owner)
		case lock > write:
			t.Errorf("%s: the owner %s takes the selection lock after the write", writer, owned.owner)
		}
	}
	for writer, proof := range datasetWriterLockExempt {
		covered = append(covered, writer)
		if strings.TrimSpace(proof) == "" {
			t.Errorf("exempt writer %s has no proof", writer)
		}
	}
	var named []string
	for writer := range datasetStateWriters {
		named = append(named, writer)
	}
	sort.Strings(covered)
	sort.Strings(named)
	if strings.Join(covered, "\n") != strings.Join(named, "\n") {
		t.Fatalf("the lock census and the writer census differ.\n lock  %v\n named %v", covered, named)
	}
	function := findFunction(t, repoRoot, "internal/api/syncadmin.lockConfigSelections")
	if lock, _ := positions(function, ""); lock == 0 {
		t.Error("lockConfigSelections does not take the shared selection lock")
	}
}
