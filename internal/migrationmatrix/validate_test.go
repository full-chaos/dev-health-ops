package migrationmatrix

import (
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
		CandidateBuild: strings.Repeat("e", 40),
		Live:           true,
		Proven:         NoProof,
	}
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

	if got := UnprovenReachable(rows); got != 1 {
		t.Fatalf("UnprovenReachable = %d, want 1 (shadow is not reachable to a real client)", got)
	}

	block := RenderOpsBlock(snapshot(rows...))
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
	block := RenderOpsBlock(snapshot(dead))
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
