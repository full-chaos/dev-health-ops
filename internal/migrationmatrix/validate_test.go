package migrationmatrix

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The point of every test in this file is one sentence: a row that lies must
// fail. Each case states the lie in its name.

func families() *NativeFamilies {
	return &NativeFamilies{
		Daily:     map[string]string{"work_item_attribution": "native", "cicd": "native"},
		Finalize:  map[string]string{"ic_finalize": "finalize"},
		Remaining: map[string]string{"work_item_attribution": "native"},
		Workgraph: map[string]string{"investment.materialize": "native"},
	}
}

// honestRow is the state every family starts in and the state the whole
// program is actually in today: it executes in Go, nothing has compared its
// output, the fleet cannot say what commit it is running, and no proof run
// exists. It must PASS -- a gate that rejects the truth gets routed around.
func honestRow() FamilyStatus {
	return FamilyStatus{
		Parity:   Parity{Status: ParityUnverified},
		Deployed: Deployed{Revision: UnknownRevision, ReadAt: time.Unix(1757, 0).UTC(), Source: "docker inspect x"},
		Proof:    Proof{Ref: NoProof},
	}
}

func fullLedger(overrides map[string]FamilyStatus) *StatusLedger {
	ledger := &StatusLedger{SchemaVersion: 1, Families: map[string]FamilyStatus{}}
	for _, key := range families().Keys() {
		ledger.Families[key.ID()] = honestRow()
	}
	for id, entry := range overrides {
		ledger.Families[id] = entry
	}
	return ledger
}

func rules(violations []Violation) []string {
	out := make([]string, 0, len(violations))
	for _, v := range violations {
		out = append(out, v.Rule)
	}
	return out
}

func assertRule(t *testing.T, violations []Violation, want string) {
	t.Helper()
	for _, v := range violations {
		if v.Rule == want {
			return
		}
	}
	t.Fatalf("want a %s violation, got %v", want, rules(violations))
}

func assertClean(t *testing.T, violations []Violation) {
	t.Helper()
	if len(violations) != 0 {
		t.Fatalf("want no violations, got:\n%s", FormatViolations(violations))
	}
}

func TestTheHonestLedgerPasses(t *testing.T) {
	assertClean(t, ValidateLedger(fullLedger(nil), families()))
}

func TestAFamilyNameThatIsNotUniqueAcrossScopesGetsItsOwnRow(t *testing.T) {
	// work_item_attribution is a daily family (the full attribution compute)
	// AND a remaining family (a narrow staleness backstop). Two executors,
	// two correctness claims. A ledger keyed by bare name would carry one
	// row and silently report whichever was written last.
	keys := families().Keys()
	var ids []string
	for _, key := range keys {
		if key.Name == "work_item_attribution" {
			ids = append(ids, key.ID())
		}
	}
	if len(ids) != 2 {
		t.Fatalf("want two distinct ids for work_item_attribution, got %v", ids)
	}
	if ids[0] == ids[1] {
		t.Fatalf("the two scopes collapsed to one id: %v", ids)
	}

	// And dropping one of them is caught rather than absorbed.
	ledger := fullLedger(nil)
	delete(ledger.Families, "remaining/work_item_attribution")
	assertRule(t, ValidateLedger(ledger, families()), "R1-missing-status")
}

func TestVerifiedParityWithoutEvidenceFails(t *testing.T) {
	cases := []struct {
		name   string
		parity Parity
		want   string
	}{
		{
			name:   "no ticket",
			parity: Parity{Status: ParityVerified, Sha: strings.Repeat("a", 40), Evidence: "run 1"},
			want:   "R2-parity-ticket",
		},
		{
			name:   "abbreviated sha",
			parity: Parity{Status: ParityVerified, Ticket: "CHAOS-1", Sha: "a1b2c3d", Evidence: "run 1"},
			want:   "R2-parity-sha",
		},
		{
			name:   "no evidence path",
			parity: Parity{Status: ParityVerified, Ticket: "CHAOS-1", Sha: strings.Repeat("a", 40)},
			want:   "R2-parity-evidence",
		},
		{
			name:   "evidence is only whitespace",
			parity: Parity{Status: ParityVerified, Ticket: "CHAOS-1", Sha: strings.Repeat("a", 40), Evidence: "   "},
			want:   "R2-parity-evidence",
		},
		{
			name:   "a status nobody defined",
			parity: Parity{Status: "done"},
			want:   "R2-parity-status",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := honestRow()
			row.Parity = tc.parity
			ledger := fullLedger(map[string]FamilyStatus{"daily/cicd": row})
			assertRule(t, ValidateLedger(ledger, families()), tc.want)
		})
	}
}

func TestFullyEvidencedVerifiedParityPasses(t *testing.T) {
	row := honestRow()
	row.Parity = Parity{
		Status:   ParityVerified,
		Ticket:   "CHAOS-4290",
		Sha:      strings.Repeat("a", 40),
		Evidence: "ci/evidence/ic_finalize_ab_2026-09-09.json",
	}
	assertClean(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()))
}

func TestDivergedParityMustAlsoAppearInOpenRegressions(t *testing.T) {
	// A family whose output is known wrong, with the ticket recorded only in
	// the parity cell, reads as "handled" in the one column a status report
	// scans. It has to be in the open-regressions column too.
	row := honestRow()
	row.Parity = Parity{Status: ParityDiverged, Ticket: "CHAOS-5459", Evidence: "coverage 41% -> 34%"}
	assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R5-diverged-not-open")

	row.OpenRegressions = []string{"CHAOS-5459"}
	assertClean(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()))
}

func TestDeployedRevisionMustBeAShaOrTheLiteralUnknown(t *testing.T) {
	for _, revision := range []string{"main", "latest", "HEAD", "dc4194f58", "", "unknown "} {
		t.Run(revision, func(t *testing.T) {
			row := honestRow()
			row.Deployed.Revision = revision
			assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R3-deployed-revision")
		})
	}
}

func TestARevisionWithNoReadTimeIsNotAFact(t *testing.T) {
	row := honestRow()
	row.Deployed = Deployed{Revision: strings.Repeat("b", 40), Source: "docker inspect x"}
	assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R3-deployed-read-at")
}

func TestAProofCannotBeClaimedAgainstABuildNobodyCanName(t *testing.T) {
	// This is the "done" claim in its purest form: a recorded proof run
	// against a fleet whose own image label says "unknown". The proof is
	// evidence for exactly one build; if the build has no name, the proof
	// has no subject.
	row := honestRow()
	row.Proof = Proof{Ref: "run-34126520155"}
	assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R4-proof-without-build")

	// Same proof, against a fleet that CAN name its build: fine.
	row.Deployed.Revision = strings.Repeat("c", 40)
	assertClean(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()))
}

func TestAnEmptyProofCellIsNotTheSameAsNone(t *testing.T) {
	// "" is an unanswered question; "none" is an answer. The distinction is
	// the entire difference between a tracker that omits a column and one
	// that says the column is empty.
	row := honestRow()
	row.Proof = Proof{Ref: ""}
	assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R4-proof-empty")
}

func TestAShortProofRefCannotIdentifyARun(t *testing.T) {
	row := honestRow()
	row.Deployed.Revision = strings.Repeat("c", 40)
	row.Proof = Proof{Ref: "ok"}
	assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R4-proof-ref")
}

func TestOpenRegressionsMustBeTicketIdsNotProse(t *testing.T) {
	row := honestRow()
	row.OpenRegressions = []string{"coverage is down"}
	assertRule(t, ValidateLedger(fullLedger(map[string]FamilyStatus{"daily/cicd": row}), families()), "R6-regression-ticket")
}

func TestAFamilyTheWorkerDoesNotHaveIsCaught(t *testing.T) {
	ledger := fullLedger(map[string]FamilyStatus{"daily/renamed_last_week": honestRow()})
	assertRule(t, ValidateLedger(ledger, families()), "R1-unknown-family")
}

func liveRow(operation, mode string) OperationRow {
	return OperationRow{
		Operation:      operation,
		Mode:           mode,
		SchemaDigest:   "sha256:" + strings.Repeat("d", 64),
		DocumentDigest: documentOf(operation),
		CandidateBuild: strings.Repeat("e", 40),
		Live:           true,
		Proven:         NoProof,
	}
}

// documentOf is the catalog document a test registers for operation.
func documentOf(operation string) string {
	sum := sha256.Sum256([]byte(operation))
	return hex.EncodeToString(sum[:])
}

// catalogFor registers each operation at documentOf(operation) -- the
// catalog every row built by liveRow agrees with, so a test that is not
// about DOCUMENT_DRIFT cannot trip over it.
func catalogFor(t *testing.T, operations ...string) Catalog {
	t.Helper()
	pairs := make([][2]string, 0, len(operations))
	seen := map[string]bool{}
	for _, operation := range operations {
		if seen[operation] {
			continue
		}
		seen[operation] = true
		pairs = append(pairs, [2]string{operation, documentOf(operation)})
	}
	catalog, err := NewCatalog(pairs)
	if err != nil {
		t.Fatalf("build the test catalog: %v", err)
	}
	return catalog
}

func snapshot(rows ...OperationRow) *Render {
	return &Render{
		SchemaVersion: 1,
		RenderedAt:    time.Unix(1757, 0).UTC(),
		OpsSha:        strings.Repeat("f", 40),
		SchemaDigest:  "sha256:" + strings.Repeat("d", 64),
		FleetSource:   "docker inspect x",
		Operations:    rows,
	}
}

func TestAReachableUnprovenOperationIsRenderedNotRejected(t *testing.T) {
	// Today: 11 canary operations, zero proof runs. That is the true state,
	// so it must not fail the gate -- it must be counted and marked.
	rows := []OperationRow{liveRow("investmentFull", "canary"), liveRow("hotspots", "shadow")}
	assertClean(t, ValidateRender(snapshot(rows...)))

	if got := UnprovenReachable(rows, catalogFor(t, "investmentFull", "hotspots")); got != 1 {
		t.Fatalf("UnprovenReachable = %d, want 1 (shadow is not reachable to a real client)", got)
	}

	block := RenderOpsBlock(snapshot(rows...), catalogFor(t, "investmentFull", "hotspots"))
	if !strings.Contains(block, "canary / **UNPROVEN**") {
		t.Fatalf("a reachable unproven operation must be marked UNPROVEN in its mode cell; got:\n%s", block)
	}
	if strings.Contains(block, "shadow / **UNPROVEN**") {
		t.Fatalf("shadow does not serve a client request and must not be marked UNPROVEN; got:\n%s", block)
	}
	if !strings.Contains(block, "no deployed-executed proof: **1**") {
		t.Fatalf("the unproven count must be on the page; got:\n%s", block)
	}
}

func TestDeadRowsAreCountedRatherThanFilteredOut(t *testing.T) {
	// PR #2065 moved the SDL digest hours after twelve canary rows were
	// seeded at the old one. Every request fell back to Python, correctly
	// and silently, for six days. Filtering these rows out of the page is
	// how that stayed invisible.
	dead := liveRow("cognitiveLoad", "canary")
	dead.SchemaDigest = "sha256:" + strings.Repeat("9", 64)
	dead.Live = false

	if got := DeadReachable([]OperationRow{dead}); got != 1 {
		t.Fatalf("DeadReachable = %d, want 1", got)
	}
	block := RenderOpsBlock(snapshot(dead), catalogFor(t, "cognitiveLoad"))
	if !strings.Contains(block, "**DEAD** (digest moved)") {
		t.Fatalf("a row at a stale digest must render as DEAD; got:\n%s", block)
	}
	if !strings.Contains(block, "`cognitiveLoad`") {
		t.Fatalf("a dead row must still appear; got:\n%s", block)
	}
}

func TestTwoRowsForOneOperationAtOneDigestFail(t *testing.T) {
	row := liveRow("investmentFull", "canary")
	assertRule(t, ValidateRender(snapshot(row, row)), "R8-duplicate-row")
}

// opus r6 (P2-2): the DOCUMENT_DRIFT shape -- the catalog's document at
// canary and a second, drifted document at primary, one operation, one
// schema digest -- rendered "live, primary, proven" and -check passed. The
// edge serves this operation at CANARY; the page said PRIMARY with proof.
//
// Each assertion below names what it pins, because each is a different
// half of one state:
//   - R8 is keyed on the TRIPLE, so the two documents are NOT a duplicate
//     (g37 reverted R8 to (digest, operation) and survived every test);
//   - the drifted row renders DOCUMENT_DRIFT, the catalog's row "yes";
//   - the drifted row fails the page (R14), exactly once, naming both
//     documents;
//   - the drifted row is counted as drift, not as reachable-unproven.
func TestADocumentDriftRowIsNamedCountedAndFailsThePage(t *testing.T) {
	served := liveRow("featureFlags", "canary")
	drifted := liveRow("featureFlags", "primary")
	drifted.DocumentDigest = strings.Repeat("0", 64)
	catalog := catalogFor(t, "featureFlags")
	render := snapshot(served, drifted)

	for _, violation := range ValidateRender(render) {
		if violation.Rule == "R8-duplicate-row" {
			t.Fatalf("two DIFFERENT documents of one operation were reported as a duplicate (%s): R8 must key on (schema, document, operation) -- keyed on (schema, operation) it fails the page for the wrong reason and hides the real one", violation.Detail)
		}
	}

	drift := ValidateDocumentDrift(render, catalog)
	if len(drift) != 1 || drift[0].Rule != "R14-document-drift" || drift[0].Subject != "featureFlags" {
		t.Fatalf("want exactly one R14-document-drift on featureFlags, got %v", drift)
	}
	for _, want := range []string{drifted.DocumentDigest, served.DocumentDigest, "primary", "DOCUMENT_DRIFT"} {
		if !strings.Contains(drift[0].Detail, want) {
			t.Fatalf("the R14 detail must name %q so an operator can find the row; got: %s", want, drift[0].Detail)
		}
	}

	block := RenderOpsBlock(render, catalog)
	var servedLine, driftedLine string
	for _, line := range strings.Split(block, "\n") {
		if !strings.HasPrefix(line, "| `featureFlags` |") {
			continue
		}
		// By the MODE cell's prefix, not its whole text: the cell reads
		// "primary / **UNPROVEN**" if the render wrongly marks the drifted
		// row reachable-unproven, and the assertion that names THAT defect
		// must be the one that fires, not a failure to find the line.
		if strings.Contains(line, "| primary") {
			driftedLine = line
		} else {
			servedLine = line
		}
	}
	if !strings.Contains(driftedLine, "**DOCUMENT_DRIFT**") || !strings.Contains(driftedLine, "`000000000000…`") {
		t.Fatalf("the drifted row must render as DOCUMENT_DRIFT naming its document; got %q in:\n%s", driftedLine, block)
	}
	if strings.Contains(driftedLine, "UNPROVEN") {
		t.Fatalf("a row the edge cannot dispatch is not a reachable-unproven row; got %q", driftedLine)
	}
	if !strings.Contains(servedLine, "| yes |") || strings.Contains(servedLine, "DOCUMENT_DRIFT") {
		t.Fatalf("the catalog's row must render as live and served; got %q", servedLine)
	}
	if got := DriftedLive(render.Operations, catalog); got != 1 {
		t.Fatalf("DriftedLive = %d, want 1", got)
	}
	if got := UnprovenReachable(render.Operations, catalog); got != 1 {
		t.Fatalf("UnprovenReachable = %d, want 1: the served canary row counts, the drifted primary row does not", got)
	}
	if !strings.Contains(block, "(DOCUMENT_DRIFT, as `dev-hops go-api routing status` reports it): **1**") {
		t.Fatalf("the drift count must be on the page; got:\n%s", block)
	}

	// Control: the catalog's row alone is clean, so R14 above is caused by
	// the drifted row and nothing else.
	if clean := ValidateDocumentDrift(snapshot(served), catalog); len(clean) != 0 {
		t.Fatalf("the catalog's own row alone must not drift, got %v", clean)
	}
}

// A live row at an operation the catalog does not register at all cannot be
// dispatched either -- the edge has no document to resolve it by.
func TestALiveRowAtAnOperationTheCatalogDoesNotRegisterIsDrift(t *testing.T) {
	orphan := liveRow("retiredOperation", "canary")
	violations := ValidateDocumentDrift(snapshot(orphan), catalogFor(t, "featureFlags"))
	assertRule(t, violations, "R14-document-drift")
	if !strings.Contains(violations[0].Detail, "does not register this operation at all") {
		t.Fatalf("the detail must say the operation is unregistered, got: %s", violations[0].Detail)
	}
	// A DEAD row is not drift: it is already DEAD, counted by DeadReachable.
	orphan.Live = false
	if got := ValidateDocumentDrift(snapshot(orphan), catalogFor(t, "featureFlags")); len(got) != 0 {
		t.Fatalf("a dead row must not also be reported as drift, got %v", got)
	}
}

// A live row with no document digest cannot be judged. It is counted on the
// page -- neither assumed served (the silence P2-2 found) nor assumed
// drifted (which would fail every page rendered before the key was carried).
func TestALiveRowWithNoDocumentDigestIsCountedAsUnjudged(t *testing.T) {
	row := liveRow("featureFlags", "canary")
	row.DocumentDigest = ""
	catalog := catalogFor(t, "featureFlags")
	if DocumentDrift(row, catalog) {
		t.Fatal("a row with no document digest was judged drifted")
	}
	if got := UnjudgedLive([]OperationRow{row}); got != 1 {
		t.Fatalf("UnjudgedLive = %d, want 1", got)
	}
	block := RenderOpsBlock(snapshot(row), catalog)
	if !strings.Contains(block, "DOCUMENT_DRIFT cannot be judged for them: **1**") {
		t.Fatalf("the unjudged count must be on the page; got:\n%s", block)
	}
}

// Two rows sharing (schema digest, operation) differ only by document. The
// render sorted them with sort.Slice and no document tiebreak, so the pair's
// order was not a function of the data, and the committed page and -check's
// re-render could disagree on identical rows.
func TestTheOpsBlockDoesNotDependOnTheOrderRowsArriveIn(t *testing.T) {
	a := liveRow("featureFlags", "canary")
	b := liveRow("featureFlags", "python")
	b.DocumentDigest = strings.Repeat("1", 64)
	c := liveRow("featureFlags", "shadow")
	c.DocumentDigest = strings.Repeat("2", 64)
	catalog := catalogFor(t, "featureFlags")
	want := RenderOpsBlock(snapshot(a, b, c), catalog)
	for _, order := range [][]OperationRow{{c, b, a}, {b, a, c}, {c, a, b}, {a, c, b}, {b, c, a}} {
		if got := RenderOpsBlock(snapshot(order...), catalog); got != want {
			t.Fatalf("the rendered block depends on input order:\nwant:\n%s\ngot:\n%s", want, got)
		}
	}
}

func TestTheCatalogLoaderRefusesWhatThePythonLoaderRefuses(t *testing.T) {
	for _, c := range []struct {
		name  string
		pairs [][2]string
	}{
		{"empty", nil},
		{"empty operation", [][2]string{{"", "ab"}}},
		{"blank digest", [][2]string{{"featureFlags", "  "}}},
		{"one digest registered twice", [][2]string{{"featureFlags", "ab"}, {"hotspots", "ab"}}},
	} {
		if _, err := NewCatalog(c.pairs); err == nil {
			t.Fatalf("%s: the catalog was accepted", c.name)
		}
	}
	// Control: one operation at two documents IS a valid catalog -- both
	// are registered, so neither is drift.
	catalog, err := NewCatalog([][2]string{{"featureFlags", "ab"}, {"featureFlags", "cd"}})
	if err != nil {
		t.Fatalf("a valid catalog was refused: %v", err)
	}
	if !catalog.Names("featureFlags", "ab") || !catalog.Names("featureFlags", "cd") || catalog.Names("featureFlags", "ef") {
		t.Fatal("Names does not answer from the registered pairs")
	}
}

func TestTheCommittedCatalogLoads(t *testing.T) {
	catalog, err := LoadCatalog(filepath.Join("..", "..", "src", "dev_health_ops", "api", "graphql", "go_api_operations.json"))
	if err != nil {
		t.Fatalf("the committed operation catalog does not load: %v", err)
	}
	if len(catalog.Documents("featureFlags")) != 1 {
		t.Fatalf("featureFlags should be registered at exactly one document, got %v", catalog.Documents("featureFlags"))
	}
}

func TestARenderWithNoProvenanceFails(t *testing.T) {
	bad := snapshot(liveRow("investmentFull", "canary"))
	bad.OpsSha = "dc4194f58"
	bad.SchemaDigest = "29d509cd"
	bad.FleetSource = ""
	violations := ValidateRender(bad)
	for _, want := range []string{"R7-ops-sha", "R7-schema-digest", "R7-fleet-source"} {
		assertRule(t, violations, want)
	}
}

func TestFreshnessRejectsAnAbbreviatedShaBeforeAnythingElse(t *testing.T) {
	violations := CheckFreshness("e3e2e77c", time.Now(), time.Now(), true, 7*24*time.Hour)
	assertRule(t, violations, "R10-sha-shape")
	if len(violations) != 1 {
		t.Fatalf("an unparseable sha should produce exactly one violation, got %v", rules(violations))
	}
}

func TestFreshnessRejectsAStampFromACommitThisBranchDoesNotContain(t *testing.T) {
	now := time.Now()
	assertRule(t, CheckFreshness(strings.Repeat("a", 40), now, now, false, 7*24*time.Hour), "R10-not-ancestor")
}

func TestFreshnessRejectsAStampOlderThanTheBudget(t *testing.T) {
	now := time.Date(2026, 9, 9, 3, 0, 0, 0, time.UTC)
	sha := strings.Repeat("a", 40)

	// The real stamp on 2026-09-09: five days old, forty merges behind, and
	// still reading as current. Inside a seven-day budget it passes...
	assertClean(t, CheckFreshness(sha, now.AddDate(0, 0, -5), now, true, 7*24*time.Hour))
	// ...and two days later it does not.
	assertRule(t, CheckFreshness(sha, now.AddDate(0, 0, -8), now, true, 7*24*time.Hour), "R10-stale")
}

func TestRenderAgeIsCheckedSeparatelyFromTheStamp(t *testing.T) {
	now := time.Date(2026, 9, 9, 3, 0, 0, 0, time.UTC)
	assertClean(t, CheckRenderAge(now.Add(-time.Hour), now, 7*24*time.Hour))
	assertRule(t, CheckRenderAge(now.AddDate(0, 0, -9), now, 7*24*time.Hour), "R11-stale")
	assertRule(t, CheckRenderAge(time.Time{}, now, 7*24*time.Hour), "R11-no-timestamp")
}

func TestReplaceBlockRefusesAMissingOrDuplicatedMarker(t *testing.T) {
	if _, err := ReplaceBlock("no markers here", FamilyBlockBegin, FamilyBlockEnd, "x"); err == nil {
		t.Fatal("want an error when the begin marker is absent")
	}
	doc := FamilyBlockBegin + "\nold\n" + FamilyBlockBegin + "\n" + FamilyBlockEnd
	if _, err := ReplaceBlock(doc, FamilyBlockBegin, FamilyBlockEnd, "x"); err == nil {
		t.Fatal("want an error when the begin marker appears twice")
	}
	reversed := FamilyBlockEnd + "\n" + FamilyBlockBegin
	if _, err := ReplaceBlock(reversed, FamilyBlockBegin, FamilyBlockEnd, "x"); err == nil {
		t.Fatal("want an error when end precedes begin")
	}
}

func TestReplaceThenExtractRoundTrips(t *testing.T) {
	doc := "before\n" + FamilyBlockBegin + "\nstale\n" + FamilyBlockEnd + "\nafter\n"
	out, err := ReplaceBlock(doc, FamilyBlockBegin, FamilyBlockEnd, "fresh\nrows\n")
	if err != nil {
		t.Fatalf("ReplaceBlock: %v", err)
	}
	got, err := ExtractBlock(out, FamilyBlockBegin, FamilyBlockEnd)
	if err != nil {
		t.Fatalf("ExtractBlock: %v", err)
	}
	if got != "fresh\nrows" {
		t.Fatalf("round trip gave %q", got)
	}
	if !strings.HasPrefix(out, "before\n") || !strings.HasSuffix(out, "after\n") {
		t.Fatalf("content outside the markers was disturbed:\n%s", out)
	}
}

func TestAFamilyWithNoStatusRowStillRendersVisibly(t *testing.T) {
	// A broken page must show that the family exists and has no status,
	// rather than dropping the row -- a missing row reads as "nothing to
	// see", which is the same failure in a different costume.
	ledger := fullLedger(nil)
	delete(ledger.Families, "daily/cicd")
	block := RenderFamilyBlock(ledger, families())
	if !strings.Contains(block, "**NO STATUS ROW**") {
		t.Fatalf("want a visible NO STATUS ROW marker, got:\n%s", block)
	}
}

// LoadCatalog over its whole input domain, one cell per shape a
// go_api_operations.json can take. The contract: refuse everything the
// Python loader refuses, plus blank names and unknown keys; accept the
// canonical file and one operation at two documents.
func TestLoadCatalogOverItsInputDomain(t *testing.T) {
	dir := t.TempDir()
	d1, d2 := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for _, c := range []struct {
		name   string
		body   string
		accept bool
	}{
		{"canonical", `[{"operation":"featureFlags","digest":"` + d1 + `"}]`, true},
		{"one operation at two documents", `[{"operation":"featureFlags","digest":"` + d1 + `"},{"operation":"featureFlags","digest":"` + d2 + `"}]`, true},
		{"absent (empty file)", ``, false},
		{"null", `null`, false},
		{"empty container", `[]`, false},
		{"wrong container type (object)", `{"operation":"featureFlags","digest":"` + d1 + `"}`, false},
		{"entry of wrong type (string)", `["featureFlags"]`, false},
		{"entry null", `[null]`, false},
		{"operation absent", `[{"digest":"` + d1 + `"}]`, false},
		{"digest absent", `[{"operation":"featureFlags"}]`, false},
		{"operation empty", `[{"operation":"","digest":"` + d1 + `"}]`, false},
		{"digest blank", `[{"operation":"featureFlags","digest":"  "}]`, false},
		{"digest = the blank cutset (JSON escapes)", `[{"operation":"featureFlags","digest":" \t\n\u000b\f\r\u00a0"}]`, false},
		{"operation U+2028 (outside the cutset: a name, as Python also loads it)", `[{"operation":"\u2028","digest":"` + d1 + `"}]`, true},
		{"digest wrong scalar type (number)", `[{"operation":"featureFlags","digest":7}]`, false},
		{"digest null", `[{"operation":"featureFlags","digest":null}]`, false},
		{"duplicate digest, two operations", `[{"operation":"featureFlags","digest":"` + d1 + `"},{"operation":"hotspots","digest":"` + d1 + `"}]`, false},
		{"out-of-vocabulary key", `[{"operation":"featureFlags","digest":"` + d1 + `","document":"x"}]`, false},
	} {
		path := filepath.Join(dir, strings.ReplaceAll(c.name, " ", "_")+".json")
		if err := os.WriteFile(path, []byte(c.body), 0o600); err != nil {
			t.Fatalf("write %s: %v", c.name, err)
		}
		_, err := LoadCatalog(path)
		if (err == nil) != c.accept {
			t.Fatalf("%s: accepted=%v, the contract says %v (err=%v)", c.name, err == nil, c.accept, err)
		}
		t.Logf("cell %-62s observed accepted=%-5v contract accepted=%v", c.name, err == nil, c.accept)
	}
}

// ParseRoutingSnapshot over its whole input domain. The payload is what
// RoutingStateSQL emits; anything else is a hand-built file, and each field
// a hand-built file can get wrong is a cell here.
func TestParseRoutingSnapshotOverItsInputDomain(t *testing.T) {
	row := func(extra string) string {
		return `{"selected_operation":"featureFlags","document_digest":"d","mode":"canary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null` + extra + `}`
	}
	for _, c := range []struct {
		name   string
		body   string
		accept bool
		rows   int
		total  int
		proven string
	}{
		{"canonical, no proof", `{"proof_run_total":0,"rows":[` + row("") + `]}`, true, 1, 0, NoProof},
		{"canonical, proven", `{"proof_run_total":3,"rows":[` + strings.Replace(row(""), `"proof_run_id":null`, `"proof_run_id":"p-1234567"`, 1) + `]}`, true, 1, 3, "p-1234567"},
		{"proof id empty string", `{"proof_run_total":0,"rows":[` + strings.Replace(row(""), `"proof_run_id":null`, `"proof_run_id":""`, 1) + `]}`, true, 1, 0, NoProof},
		{"proof id whitespace", `{"proof_run_total":0,"rows":[` + strings.Replace(row(""), `"proof_run_id":null`, `"proof_run_id":"  "`, 1) + `]}`, true, 1, 0, NoProof},
		{"rows null (empty table)", `{"proof_run_total":0,"rows":null}`, true, 0, 0, ""},
		{"rows empty container", `{"proof_run_total":0,"rows":[]}`, true, 0, 0, ""},
		{"absent (empty input)", ``, false, 0, 0, ""},
		{"null payload", `null`, false, 0, 0, ""},
		{"wrong container type (array)", `[` + row("") + `]`, false, 0, 0, ""},
		{"total absent", `{"rows":[` + row("") + `]}`, false, 0, 0, ""},
		{"total null", `{"proof_run_total":null,"rows":[` + row("") + `]}`, false, 0, 0, ""},
		{"total wrong scalar type", `{"proof_run_total":"1","rows":[` + row("") + `]}`, false, 0, 0, ""},
		{"total fractional", `{"proof_run_total":1.5,"rows":[` + row("") + `]}`, false, 0, 0, ""},
		{"rows wrong container type", `{"proof_run_total":0,"rows":{}}`, false, 0, 0, ""},
		{"document digest absent", `{"proof_run_total":0,"rows":[{"selected_operation":"featureFlags","mode":"canary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null}]}`, false, 0, 0, ""},
		{"document digest blank", `{"proof_run_total":0,"rows":[` + strings.Replace(row(""), `"document_digest":"d"`, `"document_digest":" "`, 1) + `]}`, false, 0, 0, ""},
		{"document digest = the blank cutset (JSON escapes)", `{"proof_run_total":0,"rows":[` + strings.Replace(row(""), `"document_digest":"d"`, `"document_digest":" \t\n\u000b\f\r\u00a0"`, 1) + `]}`, false, 0, 0, ""},
		{"document digest U+2028 (outside the cutset: accepted, then judged by the catalog)", `{"proof_run_total":0,"rows":[` + strings.Replace(row(""), `"document_digest":"d"`, `"document_digest":"\u2028"`, 1) + `]}`, true, 1, 0, NoProof},
		{"out-of-vocabulary key", `{"proof_run_total":0,"rows":[` + row(`,"proven":true`) + `]}`, false, 0, 0, ""},
		{"out-of-vocabulary top-level key", `{"proof_run_total":0,"rows":[],"extra":1}`, false, 0, 0, ""},
		{"psql -At text, not JSON (the r6 P3-3 file)", `featureFlags|d|canary|sha256:pin|b|`, false, 0, 0, ""},
	} {
		rows, total, err := ParseRoutingSnapshot([]byte(c.body), "sha256:pin")
		if (err == nil) != c.accept {
			t.Fatalf("%s: accepted=%v, the contract says %v (err=%v)", c.name, err == nil, c.accept, err)
		}
		t.Logf("cell %-62s observed accepted=%-5v rows=%d total=%d contract accepted=%v", c.name, err == nil, len(rows), total, c.accept)
		if !c.accept {
			continue
		}
		if len(rows) != c.rows || total != c.total {
			t.Fatalf("%s: got %d rows total %d, want %d rows total %d", c.name, len(rows), total, c.rows, c.total)
		}
		if c.rows == 1 && (rows[0].Proven != c.proven || !rows[0].Live) {
			t.Fatalf("%s: row %+v, want proven=%q live=true", c.name, rows[0], c.proven)
		}
	}
}

// DocumentDrift over every shape a row can take against a catalog.
func TestDocumentDriftOverItsInputDomain(t *testing.T) {
	catalog := catalogFor(t, "featureFlags", "hotspots")
	for _, c := range []struct {
		name            string
		mutate          func(*OperationRow)
		drift, unjudged bool
	}{
		{"live, the catalog's pair", func(*OperationRow) {}, false, false},
		{"live, another document", func(r *OperationRow) { r.DocumentDigest = strings.Repeat("0", 64) }, true, false},
		{"live, a document the catalog registers for ANOTHER operation", func(r *OperationRow) { r.DocumentDigest = documentOf("hotspots") }, true, false},
		{"live, an operation the catalog does not register", func(r *OperationRow) { r.Operation = "retired" }, true, false},
		{"live, no document digest", func(r *OperationRow) { r.DocumentDigest = "" }, false, true},
		{"live, a document digest made of the blank cutset", func(r *OperationRow) { r.DocumentDigest = " \t\u00a0" }, false, true},
		{"live, a document digest of U+2028 (outside the cutset: a real, undispatchable value)", func(r *OperationRow) { r.DocumentDigest = "\u2028" }, true, false},
		{"dead, another document", func(r *OperationRow) { r.Live = false; r.DocumentDigest = strings.Repeat("0", 64) }, false, false},
		{"dead, no document digest", func(r *OperationRow) { r.Live = false; r.DocumentDigest = "" }, false, false},
		{"live, the catalog's document with different case", func(r *OperationRow) { r.DocumentDigest = strings.ToUpper(r.DocumentDigest) }, true, false},
	} {
		row := liveRow("featureFlags", "canary")
		c.mutate(&row)
		if got := DocumentDrift(row, catalog); got != c.drift {
			t.Fatalf("%s: DocumentDrift=%v, want %v", c.name, got, c.drift)
		}
		if got := DocumentUnjudged(row); got != c.unjudged {
			t.Fatalf("%s: DocumentUnjudged=%v, want %v", c.name, got, c.unjudged)
		}
		if got := len(ValidateDocumentDrift(snapshot(row), catalog)); got != map[bool]int{true: 1, false: 0}[c.drift] {
			t.Fatalf("%s: %d R14 violations, want drift=%v", c.name, got, c.drift)
		}
		t.Logf("cell %-80s observed drift=%-5v unjudged=%-5v R14=%d", c.name, DocumentDrift(row, catalog), DocumentUnjudged(row), len(ValidateDocumentDrift(snapshot(row), catalog)))
	}
}

// R8's proof-id judgments over the one blank definition: a proof id is a
// routing-proof value, so "names nothing" here is goapiproof.NamesNothing,
// the same set every other blank judgment in this change uses.
func TestR8ProofIdCellsUseTheOneBlankDefinition(t *testing.T) {
	for _, c := range []struct {
		name, proven, rule string
	}{
		{"empty", "", "R8-proven-empty"},
		{"spaces", "  ", "R8-proven-empty"},
		{"tab and newline", "\t\n", "R8-proven-empty"},
		{"NBSP (in the cutset, not in ASCII whitespace)", "\u00a0", "R8-proven-empty"},
		{"U+2028 (outside the cutset: a value, too short to identify a run)", "\u2028", "R8-proven-ref"},
		{"too short", "ok", "R8-proven-ref"},
		{"NoProof", NoProof, ""},
		{"a run id", "742b4019-0000-4000-8000-000000000002", ""},
	} {
		row := liveRow("featureFlags", "canary")
		row.Proven = c.proven
		var got []string
		for _, v := range ValidateRender(snapshot(row)) {
			if strings.HasPrefix(v.Rule, "R8-proven") {
				got = append(got, v.Rule)
			}
		}
		want := []string{}
		if c.rule != "" {
			want = []string{c.rule}
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s: proven=%q gave %v, want %v", c.name, c.proven, got, want)
		}
		t.Logf("cell %-66s proven=%q observed %v", c.name, c.proven, got)
	}
}

// The matrix's own composed statement, swept like the clause it composes:
// every escape string in it must be the generated numeric-only literal, and
// nothing else in it may carry a backslash.
func TestTheRoutingStatementCarriesNoVersionDependentLiteral(t *testing.T) {
	statement, err := RoutingStateSQL()
	if err != nil {
		t.Fatalf("RoutingStateSQL: %v", err)
	}
	numeric := regexp.MustCompile(`E'(?:\\x[0-9a-f]{2}|\\u[0-9a-f]{4}|\\U[0-9a-f]{8})*'`)
	literals := numeric.FindAllString(statement, -1)
	rest := numeric.ReplaceAllString(statement, "")
	if len(literals) != 4 {
		t.Fatalf("want the generated literal 4 times (citation and build, two modes), found %d", len(literals))
	}
	if strings.Contains(rest, "\\") || strings.Contains(rest, "E'") {
		t.Fatalf("the routing statement carries a backslash or an escape string that is not the generated literal:\n%s", rest)
	}
	t.Logf("cell RoutingStateSQL: %d escape-string literals, all numeric-only; backslashes or E-strings elsewhere: none", len(literals))
}

// opus r7 (P3-4, mutant g26): the Live derivation moved into
// ParseRoutingSnapshot, and `Live: true` survived every suite -- a row at a
// moved schema digest then rendered "yes" and the silent-fallback count read
// 0: the 2026-09-01 silent death, on the page built to show it, all green.
func TestParseRoutingSnapshotDerivesLiveFromTheSchemaDigest(t *testing.T) {
	body := `{"proof_run_total":0,"rows":[` +
		`{"selected_operation":"featureFlags","document_digest":"d","mode":"canary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null},` +
		`{"selected_operation":"hotspots","document_digest":"h","mode":"canary","schema_digest":"sha256:moved","current_candidate_build":"b","proof_run_id":null}]}`
	rows, _, err := ParseRoutingSnapshot([]byte(body), "sha256:pin")
	if err != nil {
		t.Fatalf("ParseRoutingSnapshot: %v", err)
	}
	live := map[string]bool{}
	for _, row := range rows {
		live[row.Operation] = row.Live
	}
	if !live["featureFlags"] || live["hotspots"] {
		t.Fatalf("Live must be schema_digest == the pin: got %v", live)
	}
	if got := DeadReachable(rows); got != 1 {
		t.Fatalf("DeadReachable = %d, want 1: the canary row at the moved digest is the silent-fallback row", got)
	}
	t.Logf("cell pin row -> live=%v ; moved-digest row -> live=%v ; silent-fallback count=%d", live["featureFlags"], live["hotspots"], DeadReachable(rows))
}

// opus r7 (mutant g28): the document tiebreak in ParseRoutingSnapshot's
// sort. A hand-built -routing file need not be ordered, and the snapshot is
// COMMITTED as last-render.json: without the tiebreak two documents of one
// operation keep the file's order, so the same rows commit differently.
func TestParseRoutingSnapshotOrdersRowsByTheFullRoutingKey(t *testing.T) {
	body := `{"proof_run_total":0,"rows":[` +
		`{"selected_operation":"featureFlags","document_digest":"doc-b","mode":"canary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null},` +
		`{"selected_operation":"featureFlags","document_digest":"doc-a","mode":"primary","schema_digest":"sha256:pin","current_candidate_build":"b","proof_run_id":null}]}`
	rows, _, err := ParseRoutingSnapshot([]byte(body), "sha256:pin")
	if err != nil {
		t.Fatalf("ParseRoutingSnapshot: %v", err)
	}
	if rows[0].DocumentDigest != "doc-a" || rows[1].DocumentDigest != "doc-b" {
		t.Fatalf("rows are not ordered by (schema, operation, document): %s then %s", rows[0].DocumentDigest, rows[1].DocumentDigest)
	}
	t.Logf("cell input order doc-b,doc-a -> output order %s,%s", rows[0].DocumentDigest, rows[1].DocumentDigest)
}

// The R14 message names the state `routing status` reports and a remedy the
// shipped tooling can perform (opus r7 P2-1, P3-2).
func TestR14NamesTheStatusStateAndAnExecutableRemedy(t *testing.T) {
	catalog := catalogFor(t, "featureFlags")
	drifted := liveRow("featureFlags", "primary")
	drifted.DocumentDigest = strings.Repeat("0", 64)
	orphan := liveRow("retiredOperation", "primary")
	for _, c := range []struct {
		row   OperationRow
		state string
	}{{drifted, "reports it DOCUMENT_DRIFT"}, {orphan, "reports it UNREGISTERED"}} {
		v := ValidateDocumentDrift(snapshot(c.row), catalog)
		if len(v) != 1 || !strings.Contains(v[0].Detail, c.state) {
			t.Fatalf("%s: R14 must say status %q, got %v", c.row.Operation, c.state, v)
		}
		remedy := "DELETE FROM go_api_routing_state WHERE schema_digest = '" + c.row.SchemaDigest + "' AND document_digest = '" + c.row.DocumentDigest + "' AND selected_operation = '" + c.row.Operation + "'"
		if !strings.Contains(v[0].Detail, remedy) {
			t.Fatalf("%s: R14 must name the full-key removal, got: %s", c.row.Operation, v[0].Detail)
		}
		t.Logf("cell %-16s R14 says %q and names the full-key removal", c.row.Operation, c.state)
	}
}
