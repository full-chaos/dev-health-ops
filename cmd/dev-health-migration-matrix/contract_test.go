package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/migrationmatrix"
)

// TestTheCommittedMatrixSatisfiesItsOwnContract runs the real -check against
// the real repository, so `go test ./...` fails the moment the committed page
// disagrees with the committed sources or a row claims a status its evidence
// column cannot support.
//
// This is separate from internal/migrationmatrix's unit tests on purpose:
// those prove the RULES work on synthetic rows, this proves the SHIPPED
// DOCUMENT obeys them. A repo can have a perfect validator and a lying page.
func TestTheCommittedMatrixSatisfiesItsOwnContract(t *testing.T) {
	root := repoRoot(t)
	if err := runCheck(root); err != nil {
		t.Fatalf("the committed migration matrix fails its own contract: %v\n\n"+
			"Re-render with:\n"+
			"  go run ./cmd/dev-health-migration-matrix -render -root . -routing <snapshot.json>\n"+
			"(-routing takes the `psql -At` output of -print-routing-sql, unedited.)", err)
	}
}

// TestCheckFailsWhenTheDocIsEditedByHand is the RED half. A generated block is
// only as good as the check that notices someone improved it in place -- which
// is precisely how the ic_finalize citation came to name a file that had been
// deleted weeks earlier.
func TestCheckFailsWhenTheDocIsEditedByHand(t *testing.T) {
	root := t.TempDir()
	realRoot := repoRoot(t)

	for _, relative := range []string{
		docRelative,
		statusRelative,
		renderRelative,
		nativeRelative,
		digestPinRelative,
		providerMatrixRelative,
		dailyFamiliesRelative,
		remainingFamiliesRel,
		jobDailyPyRelative,
		catalogRelative,
	} {
		copyInto(t, filepath.Join(realRoot, relative), filepath.Join(root, relative))
	}

	// Sanity: the copy still passes, so a failure below is caused by the
	// edit and not by the copying.
	if err := runCheck(root); err != nil {
		t.Fatalf("the copied tree should pass before it is tampered with: %v", err)
	}

	docPath := filepath.Join(root, docRelative)
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read copied doc: %v", err)
	}
	// The most tempting hand edit there is: soften the honest cell. Tampering
	// with the PARITY column rather than the deployed one on purpose -- the
	// deployed cell's text legitimately changes when the fleet is rebuilt
	// (it read "**unknown**" until the Compose COMMIT build-arg landed,
	// and a real sha after), so a test anchored on it would fail
	// for a reason that has nothing to do with tampering. "UNVERIFIED" is
	// present for as long as any family is unverified, which is the state
	// this whole section exists to keep visible.
	tampered := replaceFirst(string(raw), "| UNVERIFIED |", "| VERIFIED |")
	if tampered == string(raw) {
		t.Fatal("expected the rendered doc to contain an UNVERIFIED parity cell to tamper with")
	}
	if err := os.WriteFile(docPath, []byte(tampered), 0o644); err != nil {
		t.Fatalf("write tampered doc: %v", err)
	}

	if err := runCheck(root); err == nil {
		t.Fatal("hand-editing a generated cell must fail the check; it passed")
	}
}

func replaceFirst(haystack, old, replacement string) string {
	index := indexOf(haystack, old)
	if index < 0 {
		return haystack
	}
	return haystack[:index] + replacement + haystack[index+len(old):]
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func copyInto(t *testing.T, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(from) //nolint:gosec // test-local path
	if err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(to), err)
	}
	if err := os.WriteFile(to, raw, 0o644); err != nil {
		t.Fatalf("write %s: %v", to, err)
	}
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root above the test's working directory")
		}
		dir = parent
	}
}

// The offline -routing path exists so an operator without a DSN can still
// render the page. Its instruction was once unfollowable: it
// named an unexported function, so "produce rows with the reader's SQL" meant
// retyping the SQL. These two tests pin the two halves of the fix -- that the
// statement is reachable, and that a snapshot missing the routing KEY is
// refused instead of silently collapsing two documents into one row.

func TestTheOperatorCanObtainTheExactStatementTheReaderRuns(t *testing.T) {
	statement, err := migrationmatrix.RoutingStateSQL()
	if err != nil {
		t.Fatalf("build the routing statement: %v", err)
	}
	// Not a substring check: the whole point is that the offline rows and
	// the live rows come from the SAME text, so anything less than
	// equality with both mode arms of the shared predicate would let the
	// two derivations drift apart again.
	for _, mode := range []string{goapiproof.TargetModePrimary, goapiproof.TargetModeCanary} {
		clause, err := goapiproof.EnablementProofClause("pr", mode)
		if err != nil {
			t.Fatalf("build the %s predicate: %v", mode, err)
		}
		if !strings.Contains(statement, clause) {
			t.Fatalf("the printed statement does not contain the %s enablement clause;\nstatement:\n%s\n\nclause:\n%s", mode, statement, clause)
		}
	}
	// Deliberately NOT asserted here: that the statement SELECTS the
	// document digest. A `strings.Contains(statement, "rs.document_digest")`
	// looks like that assertion and is not one -- the join predicate
	// mentions the same identifier, so deleting the output column leaves
	// the substring in place and the test green. Measured: that exact
	// mutation survived. The column is pinned where it can only be pinned
	// honestly -- by reading it back out of PostgreSQL, in
	// TestReadRoutingStateKeepsTwoDocumentsOfOneOperationApart.
}

func TestARoutingSnapshotWithoutTheDocumentDigestIsRefused(t *testing.T) {
	dir := t.TempDir()

	// Two DIFFERENT documents of one operation at one schema digest -- the
	// exact shape Trap #120 is about. Accepting them without their digests
	// makes them indistinguishable, and ValidateRender's R8 rule then
	// reports a duplicate row on a page whose reason for existing is that
	// a silent death should not look like health.
	withoutDigest := filepath.Join(dir, "no-document-digest.json")
	if err := os.WriteFile(withoutDigest, []byte(`{
	  "proof_run_total": 2,
	  "rows": [
	    {"selected_operation":"featureFlags","mode":"primary","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":"p1"},
	    {"selected_operation":"featureFlags","mode":"python","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":""}
	  ]
	}`), 0o600); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	_, _, err := readRoutingFile(withoutDigest, "sha256:pin")
	if err == nil {
		t.Fatal("a routing snapshot with no document_digest was accepted; two documents of one operation are then one row")
	}
	if !strings.Contains(err.Error(), "document_digest") || !strings.Contains(err.Error(), "print-routing-sql") {
		t.Fatalf("the refusal must name the missing field AND how to regenerate the snapshot, got: %v", err)
	}

	// The control: the SAME two rows, with their digests, are two rows.
	withDigest := filepath.Join(dir, "with-document-digest.json")
	if err := os.WriteFile(withDigest, []byte(`{
	  "proof_run_total": 2,
	  "rows": [
	    {"selected_operation":"featureFlags","document_digest":"doc-new","mode":"primary","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":"p1"},
	    {"selected_operation":"featureFlags","document_digest":"doc-old","mode":"python","schema_digest":"sha256:pin","current_candidate_build":"b1","proof_run_id":""}
	  ]
	}`), 0o600); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	rows, total, err := readRoutingFile(withDigest, "sha256:pin")
	if err != nil {
		t.Fatalf("a complete snapshot was refused: %v", err)
	}
	if total != 2 || len(rows) != 2 {
		t.Fatalf("expected 2 rows and a total of 2, got %d rows and %d", len(rows), total)
	}
	if rows[0].DocumentDigest == rows[1].DocumentDigest {
		t.Fatalf("both rows carry document digest %q; the two documents collapsed", rows[0].DocumentDigest)
	}
	if rows[0].Proven == rows[1].Proven {
		t.Fatalf("both rows derived proof %q, so the digests are being read but not used", rows[0].Proven)
	}
}

// copyContractTree copies every committed file -check reads into a fresh
// root, so a test can alter one of them and run the real -check on it.
func copyContractTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	realRoot := repoRoot(t)
	for _, relative := range []string{
		docRelative, statusRelative, renderRelative, nativeRelative, digestPinRelative,
		providerMatrixRelative, dailyFamiliesRelative, remainingFamiliesRel, jobDailyPyRelative,
		catalogRelative,
	} {
		copyInto(t, filepath.Join(realRoot, relative), filepath.Join(root, relative))
	}
	return root
}

// runCheckCapturingViolations runs the real -check and returns its error and
// what it printed, so a test can say WHICH rule failed rather than only that
// something did.
func runCheckCapturingViolations(t *testing.T, root string) (error, string) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	saved := os.Stderr
	os.Stderr = writer
	checkErr := runCheck(root)
	os.Stderr = saved
	_ = writer.Close()
	var captured bytes.Buffer
	_, _ = io.Copy(&captured, reader)
	return checkErr, captured.String()
}

// writeSnapshotAndPage commits `rows` as the render snapshot AND re-renders
// the ops block from them, exactly as -render would, so the page and its
// snapshot agree (R13 passes) and any failure is about the rows themselves.
func writeSnapshotAndPage(t *testing.T, root string, rows []migrationmatrix.OperationRow) {
	t.Helper()
	snapshot, err := migrationmatrix.LoadRender(filepath.Join(root, renderRelative))
	if err != nil {
		t.Fatalf("load the copied snapshot: %v", err)
	}
	snapshot.Operations = rows
	if err := writeJSON(filepath.Join(root, renderRelative), snapshot); err != nil {
		t.Fatalf("write the snapshot: %v", err)
	}
	catalog, err := migrationmatrix.LoadCatalog(filepath.Join(root, catalogRelative))
	if err != nil {
		t.Fatalf("load the copied catalog: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, docRelative))
	if err != nil {
		t.Fatalf("read the copied doc: %v", err)
	}
	doc, err := migrationmatrix.ReplaceBlock(string(raw), migrationmatrix.OpsBlockBegin, migrationmatrix.OpsBlockEnd,
		migrationmatrix.RenderOpsBlock(snapshot, catalog))
	if err != nil {
		t.Fatalf("re-render the ops block: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, docRelative), []byte(doc), 0o644); err != nil {
		t.Fatalf("write the doc: %v", err)
	}
}

// Through the real -check: the drifted row rendered "live,
// primary, proven" and -check printed "migration matrix OK" with rc=0,
// while `dev-hops go-api routing status` named the same row DOCUMENT_DRIFT.
// The page's own reason for existing is that a silent death must not look
// like health. Executed on the shipped binary before the fix: rc=0.
func TestCheckFailsOnALiveRowTheCatalogCannotDispatch(t *testing.T) {
	root := copyContractTree(t)
	pin, err := migrationmatrix.SchemaDigestPin(filepath.Join(root, digestPinRelative))
	if err != nil {
		t.Fatalf("read pin: %v", err)
	}
	catalog, err := migrationmatrix.LoadCatalog(filepath.Join(root, catalogRelative))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	served := migrationmatrix.OperationRow{
		Operation: "featureFlags", Mode: "canary", SchemaDigest: pin,
		DocumentDigest: catalog.Documents("featureFlags")[0],
		CandidateBuild: strings.Repeat("a", 40), Live: true,
		Proven: "742b4019-0000-4000-8000-000000000002",
	}
	drifted := served
	drifted.Mode = "primary"
	drifted.DocumentDigest = strings.Repeat("0", 64)
	drifted.Proven = "70ab8989-0000-4000-8000-000000000001"

	// Control first: the catalog's own row alone passes the real -check,
	// so the failure below is caused by the drifted row and nothing else.
	writeSnapshotAndPage(t, root, []migrationmatrix.OperationRow{served})
	if err, printed := runCheckCapturingViolations(t, root); err != nil {
		t.Fatalf("the control (the catalog's row alone) must pass -check: %v\n%s", err, printed)
	}

	writeSnapshotAndPage(t, root, []migrationmatrix.OperationRow{served, drifted})
	err, printed := runCheckCapturingViolations(t, root)
	if err == nil {
		t.Fatalf("-check PASSED with a live primary row whose document the catalog does not name: the page says the operation is served at primary with proof while the edge can only dispatch it at canary.\nprinted:\n%s", printed)
	}
	if !strings.Contains(printed, "R14-document-drift") {
		t.Fatalf("-check failed, but not on R14-document-drift -- the right answer for the wrong reason proves nothing:\n%s", printed)
	}
	if strings.Contains(printed, "R8-duplicate-row") {
		t.Fatalf("two DIFFERENT documents were reported as a duplicate row; R8 must key on the triple:\n%s", printed)
	}
	page, readErr := os.ReadFile(filepath.Join(root, docRelative))
	if readErr != nil {
		t.Fatalf("read the page: %v", readErr)
	}
	if !strings.Contains(string(page), "**DOCUMENT_DRIFT**") {
		t.Fatalf("the rendered page does not name the drifted row DOCUMENT_DRIFT")
	}
}

// Mutant g40: -print-routing-sql could print anything and no test would
// notice, because no test ran the flag's code. printRoutingSQL is that code;
// what it prints must be exactly the statement ReadRoutingState executes,
// and TestTheRoutingSnapshotStatementIsWhatTheOfflineReaderReads (in
// internal/migrationmatrix, against real PostgreSQL) executes that
// statement and feeds its value through the same parser the -routing
// reader uses.
func TestPrintRoutingSQLPrintsTheStatementTheReaderRuns(t *testing.T) {
	var printed bytes.Buffer
	if err := printRoutingSQL(&printed); err != nil {
		t.Fatalf("printRoutingSQL: %v", err)
	}
	statement, err := migrationmatrix.RoutingStateSQL()
	if err != nil {
		t.Fatalf("RoutingStateSQL: %v", err)
	}
	if printed.String() != strings.TrimSpace(statement)+"\n" {
		t.Fatalf("-print-routing-sql does not print the statement ReadRoutingState runs.\nprinted:\n%s\nstatement:\n%s", printed.String(), statement)
	}
}

// The offline file is the unedited `psql -At` output of the printed
// statement: one line of JSON. A file missing proof_run_total is a
// hand-built one, and its zero would falsify every proven cell at once, so
// it is refused rather than read as 0.
func TestARoutingSnapshotWithoutAProofTotalIsRefused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no-total.json")
	if err := os.WriteFile(path, []byte(`{"rows":[{"selected_operation":"featureFlags","document_digest":"d","mode":"canary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null}]}`+"\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := readRoutingFile(path, "sha256:pin"); err == nil || !strings.Contains(err.Error(), "proof_run_total") {
		t.Fatalf("a snapshot with no proof_run_total must be refused by name, got %v", err)
	}
	// Control: the same file with the total is read, and a JSON null proof
	// id is NoProof.
	if err := os.WriteFile(path, []byte(`{"proof_run_total":0,"rows":[{"selected_operation":"featureFlags","document_digest":"d","mode":"canary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null}]}`+"\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	rows, total, err := readRoutingFile(path, "sha256:pin")
	if err != nil || total != 0 || len(rows) != 1 || rows[0].Proven != migrationmatrix.NoProof || !rows[0].Live {
		t.Fatalf("the control snapshot was misread: rows=%+v total=%d err=%v", rows, total, err)
	}
}

// Every pair of flags that name one resource, or one flag another mode never
// reads: each cell is set, and the result is either an explicit refusal, an
// explicit notice, or the flag is read. Executed before the fix, on the
// shipped binary: `-render -dsn X -routing F` rendered from F without a word
// about X, and `-check -dsn X -routing /nonexistent -fleet /nonexistent`
// printed "migration matrix OK".
func TestNoFlagIsSilentlyIgnored(t *testing.T) {
	type flags = map[string]bool
	for _, c := range []struct {
		name                 string
		explicit             flags
		print, render, check bool
		fleet                string
		envDSN               bool
		refuse               bool
		notice               bool
	}{
		{"-print-routing-sql alone", flags{"print-routing-sql": true}, true, false, false, "docker", false, false, false},
		{"-print-routing-sql with $POSTGRES_URI set (an env var, not a flag -- nothing is claimed)", flags{"print-routing-sql": true}, true, false, false, "docker", true, false, false},
		{"-print-routing-sql -dsn", flags{"print-routing-sql": true, "dsn": true}, true, false, false, "docker", false, true, false},
		{"-print-routing-sql -routing", flags{"print-routing-sql": true, "routing": true}, true, false, false, "docker", false, true, false},
		{"-print-routing-sql -root", flags{"print-routing-sql": true, "root": true}, true, false, false, "docker", false, true, false},
		{"-print-routing-sql -render", flags{"print-routing-sql": true, "render": true}, true, true, false, "docker", false, true, false},
		{"-print-routing-sql -check", flags{"print-routing-sql": true, "check": true}, true, false, true, "docker", false, true, false},
		{"-check alone", flags{"check": true}, false, false, true, "docker", false, false, false},
		{"-check -root", flags{"check": true, "root": true}, false, false, true, "docker", false, false, false},
		{"-check with $POSTGRES_URI set", flags{"check": true}, false, false, true, "docker", true, false, false},
		{"-check -dsn", flags{"check": true, "dsn": true}, false, false, true, "docker", false, true, false},
		{"-check -routing", flags{"check": true, "routing": true}, false, false, true, "docker", false, true, false},
		{"-check -fleet", flags{"check": true, "fleet": true}, false, false, true, "none", false, true, false},
		{"-check -containers", flags{"check": true, "containers": true}, false, false, true, "docker", false, true, false},
		{"-render -check=false (set, false: not -check)", flags{"render": true, "check": true}, false, true, false, "docker", false, false, false},
		{"-render -dsn", flags{"render": true, "dsn": true}, false, true, false, "docker", false, false, false},
		{"-render -routing", flags{"render": true, "routing": true}, false, true, false, "docker", false, false, false},
		{"-render -dsn -routing", flags{"render": true, "dsn": true, "routing": true}, false, true, false, "docker", false, true, false},
		{"-render -routing with $POSTGRES_URI set", flags{"render": true, "routing": true}, false, true, false, "docker", true, false, true},
		{"-render with $POSTGRES_URI set (it IS the -dsn default, and is read)", flags{"render": true}, false, true, false, "docker", true, false, false},
		{"-render -fleet docker -containers", flags{"render": true, "fleet": true, "containers": true}, false, true, false, "docker", false, false, false},
		{"-render -fleet none -containers", flags{"render": true, "fleet": true, "containers": true}, false, true, false, "none", false, true, false},
		{"-render -fleet <file> -containers", flags{"render": true, "fleet": true, "containers": true}, false, true, false, "/tmp/fleet.json", false, true, false},
	} {
		notice, err := checkFlagCombination(c.explicit, c.print, c.render, c.check, c.fleet, c.envDSN)
		if (err != nil) != c.refuse {
			t.Fatalf("%s: refused=%v, want %v (err=%v)", c.name, err != nil, c.refuse, err)
		}
		if (notice != "") != c.notice {
			t.Fatalf("%s: notice=%q, want a notice=%v", c.name, notice, c.notice)
		}
		t.Logf("cell %-86s observed refused=%-5v notice=%v", c.name, err != nil, notice != "")
	}
}

// Mutant g35: R14 on -render was unpinned -- dropping it left
// `-render` exiting 0 on a drift snapshot, caught only later by -check. The
// real runRender, in a scratch git repository, on a -routing file whose live
// row serves a document the catalog does not name.
func TestRenderFailsOnALiveRowTheCatalogCannotDispatch(t *testing.T) {
	root := copyContractTree(t)
	for _, args := range [][]string{
		{"init", "-q"},
		{"-c", "user.email=t@example.com", "-c", "user.name=t", "-c", "commit.gpgsign=false", "add", "-A"},
		{"-c", "user.email=t@example.com", "-c", "user.name=t", "-c", "commit.gpgsign=false", "commit", "-qm", "scratch"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	pin, err := migrationmatrix.SchemaDigestPin(filepath.Join(root, digestPinRelative))
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	routing := filepath.Join(t.TempDir(), "routing.json")
	payload := `{"proof_run_total":0,"rows":[{"selected_operation":"featureFlags","document_digest":"` + strings.Repeat("0", 64) +
		`","mode":"primary","schema_digest":"` + pin + `","current_candidate_build":"` + strings.Repeat("a", 40) + `","proof_run_id":null}]}`
	if err := os.WriteFile(routing, []byte(payload), 0o600); err != nil {
		t.Fatalf("write routing: %v", err)
	}
	reader, writer, _ := os.Pipe()
	saved := os.Stderr
	os.Stderr = writer
	renderErr := runRender(root, "", routing, "none", nil)
	os.Stderr = saved
	_ = writer.Close()
	var printed bytes.Buffer
	_, _ = io.Copy(&printed, reader)
	if renderErr == nil || !strings.Contains(printed.String(), "R14-document-drift") {
		t.Fatalf("-render must fail on R14 for a drifted live row; err=%v stderr=%s", renderErr, printed.String())
	}
	t.Logf("cell -render on a drifted live row -> err=%q, R14 printed: true", renderErr)
}

// Mutant g36: -check's warning about rows it cannot judge was
// unpinned. On the committed tree (whose snapshot predates document digests)
// the real -check must pass AND say how many rows it could not judge.
func TestCheckWarnsAboutRowsItCannotJudge(t *testing.T) {
	err, printed := runCheckCapturingViolations(t, copyContractTree(t))
	if err != nil {
		t.Fatalf("the committed tree must pass -check: %v\n%s", err, printed)
	}
	if !strings.Contains(printed, "carry no document digest") {
		t.Fatalf("-check must warn about rows it cannot judge for DOCUMENT_DRIFT; stderr:\n%s", printed)
	}
	t.Logf("cell committed tree -> -check passes and warns: %v", strings.Contains(printed, "carry no document digest"))
}
