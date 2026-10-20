package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10376: guards ported from tests/test_e2e_organization_id_contract_conformance.py,
// tests/test_executed_proof_ledger_write_path_audit.py and tests/test_rate_limit_policy_doc.py. Every guard is a
// function of a repo root; one test runs it on the real tree, a table copies the inputs to a temp tree, plants
// one defect and asserts the guard names it.

// copyRel copies the named repo-relative files of root into a new temp tree.
func copyRel(t *testing.T, root string, rels ...string) string {
	t.Helper()
	dst := t.TempDir()
	for _, rel := range rels {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dst, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, rel), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func editRel(t *testing.T, root, rel, old, repl string) {
	t.Helper()
	p := filepath.Join(root, rel)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("planted-defect anchor %q not found in %s", old, rel)
	}
	if err := os.WriteFile(p, []byte(strings.ReplaceAll(string(b), old, repl)), 0o644); err != nil {
		t.Fatal(err)
	}
}

// --- e2e seed organization id satisfies the job contract (CHAOS-3903) ---

func e2eOrgIDViolations(root string) []string {
	src, err := os.ReadFile(filepath.Join(root, "internal", "jobcontract", "types.go"))
	if err != nil {
		return []string{err.Error()}
	}
	m := uuidPatternDecl.FindSubmatch(src)
	if m == nil {
		return []string{"uuidPattern not found in internal/jobcontract/types.go: retarget this guard, do not delete it"}
	}
	pattern, err := regexp.Compile(string(m[1]))
	if err != nil {
		return []string{err.Error()}
	}
	var bad []string
	if pattern.MatchString("11111111-2222-3333-4444-555555555555") {
		bad = append(bad, "the contract pattern accepts the id that caused CHAOS-3903")
	}
	script, err := os.ReadFile(filepath.Join(root, "ci", "run_live_backend_e2e.sh"))
	if err != nil {
		return append(bad, err.Error())
	}
	ids := e2eOrgIDLiteral.FindAllSubmatch(script, -1)
	if len(ids) == 0 {
		return append(bad, "no E2E_ORG_ID literal found in ci/run_live_backend_e2e.sh: the guard checks nothing")
	}
	for _, id := range ids {
		if !pattern.MatchString(string(id[1])) {
			bad = append(bad, fmt.Sprintf("e2e seed organization id %s is not accepted by jobcontract.uuidPattern", id[1]))
		}
	}
	return bad
}

func TestE2ESeedOrganizationIDSatisfiesJobContract(t *testing.T) {
	if v := e2eOrgIDViolations(repoRoot(t)); len(v) > 0 {
		t.Fatal(strings.Join(v, "\n"))
	}
}

func TestE2ESeedOrganizationIDGuardFiresOnPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{"internal/jobcontract/types.go", "ci/run_live_backend_e2e.sh"}
	for _, c := range []struct{ name, rel, old, repl, want string }{
		{"seed id with a bad variant nibble", "ci/run_live_backend_e2e.sh", `E2E_ORG_ID="11111111-2222-4333-8444-555555555555"`, `E2E_ORG_ID="11111111-2222-3333-4444-555555555555"`, "is not accepted by jobcontract.uuidPattern"},
		{"seed literal moved away", "ci/run_live_backend_e2e.sh", `E2E_ORG_ID="11111111-2222-4333-8444-555555555555"`, `E2E_ORG_ID="${SEED_ORG}"`, "11111111-2222-4333-8444-555555555555"},
		{"pattern accepts anything", "internal/jobcontract/types.go", "regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)", "regexp.MustCompile(`.*`)", "accepts the id that caused CHAOS-3903"},
		{"pattern renamed", "internal/jobcontract/types.go", "uuidPattern       =", "uuidPatternX      =", "uuidPattern not found"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := copyRel(t, root, files...)
			if c.name == "seed literal moved away" {
				// A single literal replaced leaves the python-heredoc literal; replace all of them.
				p := filepath.Join(tree, c.rel)
				b, _ := os.ReadFile(p)
				s := e2eOrgIDReplace.ReplaceAllString(string(b), `E2E_ORG_ID="${SEED_ORG}"`)
				if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
					t.Fatal(err)
				}
				c.want = "no E2E_ORG_ID literal found"
			} else {
				editRel(t, tree, c.rel, c.old, c.repl)
			}
			if v := strings.Join(e2eOrgIDViolations(tree), "\n"); !strings.Contains(v, c.want) {
				t.Fatalf("guard did not fire naming %q; got:\n%s", c.want, v)
			}
		})
	}
}

// --- executed-proof ledger: the set of writers is closed (CHAOS-4114) ---

var (
	ledgerAttemptedWriters = map[string]string{"internal/scheduler/sync/materializer.go": "RecordExecutedProofAttempted"}
	ledgerProvenWriters    = map[string]string{"internal/providersync/repository_postgres.go": "RecordExecutedProofTerminal"}
	ledgerFailureFiles     = []string{
		"internal/syncreconciler/unreclaimable_sweep.go",
		"internal/syncreconciler/terminal_delivery_repair.go",
		"internal/joboutbox/terminal_delivery_repair.go",
	}
)

func ledgerGoSources(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".venv", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	return out, err
}

func ledgerSetDiff(found map[string]bool, want map[string]string) string {
	var f, w []string
	for k := range found {
		f = append(f, k)
	}
	for k := range want {
		w = append(w, k)
	}
	sort.Strings(f)
	sort.Strings(w)
	if strings.Join(f, ",") == strings.Join(w, ",") {
		return ""
	}
	return fmt.Sprintf("found %v, expected %v", f, w)
}

func ledgerViolations(root string) []string {
	files, err := ledgerGoSources(root)
	if err != nil {
		return []string{err.Error()}
	}
	insert := ledgerInsertPattern
	update := ledgerUpdatePattern
	success := ledgerSuccessPattern
	inserting, terminalizing := map[string]bool{}, map[string]bool{}
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return []string{err.Error()}
		}
		src := string(b)
		if insert.MatchString(src) {
			inserting[rel] = true
		}
		// Go keeps its SQL in backtick literals; scanning whole files lets two statements satisfy one match.
		for _, lit := range func() []string {
			parts := strings.Split(src, "`")
			var odd []string
			for i := 1; i < len(parts); i += 2 {
				odd = append(odd, parts[i])
			}
			return odd
		}() {
			if update.MatchString(lit) && success.MatchString(lit) {
				terminalizing[rel] = true
			}
		}
	}
	var bad []string
	if d := ledgerSetDiff(inserting, ledgerAttemptedWriters); d != "" {
		bad = append(bad, "the set of paths that INSERT sync_run_units rows changed: "+d+"; a new insert path must record the attempt in the same transaction")
	}
	if d := ledgerSetDiff(terminalizing, ledgerProvenWriters); d != "" {
		bad = append(bad, "the set of paths that terminalize a sync unit as SUCCESS changed: "+d+"; a new terminalizer must stamp the proof in the same transaction")
	}
	for _, m := range []map[string]string{ledgerAttemptedWriters, ledgerProvenWriters} {
		for rel, call := range m {
			b, err := os.ReadFile(filepath.Join(root, rel))
			if err != nil || !strings.Contains(string(b), call) {
				bad = append(bad, fmt.Sprintf("%s no longer calls %s", rel, call))
			}
		}
	}
	for _, rel := range ledgerFailureFiles {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			bad = append(bad, err.Error())
			continue
		}
		if strings.Contains(string(b), "ExecutedProof") || strings.Contains(string(b), "executed_proof") {
			bad = append(bad, fmt.Sprintf("%s now touches the executed-proof ledger: failure terminalization cannot change attempted or proven", rel))
		}
	}
	return bad
}

func TestExecutedProofLedgerWriterSetIsClosed(t *testing.T) {
	if v := ledgerViolations(repoRoot(t)); len(v) > 0 {
		t.Fatal(strings.Join(v, "\n"))
	}
}

func TestExecutedProofLedgerGuardFiresOnPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	all := append([]string{"internal/scheduler/sync/materializer.go", "internal/providersync/repository_postgres.go"}, ledgerFailureFiles...)
	for _, c := range []struct{ name, rel, old, repl, want string }{
		{"writer stops recording the attempt", "internal/scheduler/sync/materializer.go", "RecordExecutedProofAttempted", "RecordSomethingElse", "no longer calls RecordExecutedProofAttempted"},
		{"writer stops stamping the proof", "internal/providersync/repository_postgres.go", "RecordExecutedProofTerminal", "RecordSomethingElse", "no longer calls RecordExecutedProofTerminal"},
		{"a failure path touches the ledger", "internal/syncreconciler/unreclaimable_sweep.go", "package ", "// ExecutedProof\npackage ", "now touches the executed-proof ledger"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tree := copyRel(t, root, all...)
			editRel(t, tree, c.rel, c.old, c.repl)
			if v := strings.Join(ledgerViolations(tree), "\n"); !strings.Contains(v, c.want) {
				t.Fatalf("guard did not fire naming %q; got:\n%s", c.want, v)
			}
		})
	}
	t.Run("a new insert path appears", func(t *testing.T) {
		tree := copyRel(t, root, all...)
		extra := filepath.Join(tree, "internal", "newwriter", "w.go")
		if err := os.MkdirAll(filepath.Dir(extra), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(extra, []byte("package newwriter\nconst q = `INSERT INTO public.sync_run_units (id) VALUES ($1)`\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if v := strings.Join(ledgerViolations(tree), "\n"); !strings.Contains(v, "INSERT sync_run_units rows changed") {
			t.Fatalf("guard did not fire on a new insert path; got:\n%s", v)
		}
	})
}

// --- provider limits page exists and is in the mkdocs nav (CHAOS-2757) ---

const providerLimitsPage = "docs/reference/limits-and-compatibility/provider-limits.md"

func providerLimitsViolations(root string) []string {
	var bad []string
	if st, err := os.Stat(filepath.Join(root, filepath.FromSlash(providerLimitsPage))); err != nil || st.IsDir() {
		bad = append(bad, "missing canonical provider limits page "+providerLimitsPage)
	}
	nav, err := os.ReadFile(filepath.Join(root, "mkdocs.yml"))
	if err != nil || !strings.Contains(string(nav), strings.TrimPrefix(providerLimitsPage, "docs/")) {
		bad = append(bad, "provider-limits.md is not wired into the mkdocs.yml nav: the page would be unreachable")
	}
	return bad
}

func TestProviderLimitsPageExistsAndIsInNav(t *testing.T) {
	if v := providerLimitsViolations(repoRoot(t)); len(v) > 0 {
		t.Fatal(strings.Join(v, "\n"))
	}
}

func TestProviderLimitsGuardFiresOnPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	t.Run("page removed", func(t *testing.T) {
		tree := copyRel(t, root, "mkdocs.yml")
		if v := strings.Join(providerLimitsViolations(tree), "\n"); !strings.Contains(v, "missing canonical provider limits page") {
			t.Fatalf("got:\n%s", v)
		}
	})
	t.Run("page dropped from nav", func(t *testing.T) {
		tree := copyRel(t, root, "mkdocs.yml", providerLimitsPage)
		editRel(t, tree, "mkdocs.yml", "reference/limits-and-compatibility/provider-limits.md", "reference/other.md")
		if v := strings.Join(providerLimitsViolations(tree), "\n"); !strings.Contains(v, "not wired into the mkdocs.yml nav") {
			t.Fatalf("got:\n%s", v)
		}
	})
}

// pySpace is Python's str \s as a Go regexp class body: exactly the code points where str.isspace() is true (Go's
// own \s is ASCII only). Python's \w would be \p{L}\p{N}_ and \d would be \p{Nd}; no ported regex here uses them.
const pySpace = `\x{9}-\x{d}\x{1c}-\x{20}\x{85}\x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}`

func pyIsSpace(r rune) bool {
	switch {
	case r >= 0x9 && r <= 0xd, r >= 0x1c && r <= 0x20, r == 0x85, r == 0xa0, r == 0x1680, r >= 0x2000 && r <= 0x200a,
		r == 0x2028, r == 0x2029, r == 0x202f, r == 0x205f, r == 0x3000:
		return true
	}
	return false
}

// pyStrip is str.strip(); pyFields is str.split(). Go's strings.TrimSpace and strings.Fields miss \x1c-\x1f.
func pyStrip(s string) string    { return strings.TrimFunc(s, pyIsSpace) }
func pyFields(s string) []string { return strings.FieldsFunc(s, pyIsSpace) }

var (
	uuidPatternDecl      = regexp.MustCompile(`uuidPattern[` + pySpace + `]*=[` + pySpace + `]*regexp\.MustCompile\(` + "`([^`]+)`" + `\)`)
	e2eOrgIDLiteral      = regexp.MustCompile(`E2E_ORG_ID[",)]?[` + pySpace + `]*[,=][` + pySpace + `]*"([0-9a-fA-F-]{36})"`)
	e2eOrgIDReplace      = regexp.MustCompile(`E2E_ORG_ID[",)]?[` + pySpace + `]*[,=][` + pySpace + `]*"[0-9a-fA-F-]{36}"`)
	ledgerInsertPattern  = regexp.MustCompile(`INSERT[` + pySpace + `]+INTO[` + pySpace + `]+public\.sync_run_units`)
	ledgerUpdatePattern  = regexp.MustCompile(`UPDATE[` + pySpace + `]+public\.sync_run_units`)
	ledgerSuccessPattern = regexp.MustCompile(`SET[` + pySpace + `]+status[` + pySpace + `]*=[` + pySpace + `]*'success'`)
)
