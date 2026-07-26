package domaingrants

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Tests for the three assertions codex found could not fail.
//
// Each had the same underlying shape: the test proved a value reached some data
// structure, while the code that ACTS on that value was never exercised. Proving
// a fact is available is not proving anything is done with it.

// TestAdvisoryReportCoversEveryCategory is the successor to the enforcement test,
// under the advisory posture.
//
// Nothing gates any more, so "deleting the enforcement is undetected" is no longer
// the risk. The risk that remains is a whole CATEGORY of reporting silently
// vanishing -- which, for a tool whose only output is its report, is the same
// failure wearing different clothes: a category with nothing to say and a category
// that is no longer printed look identical to the reader.
//
// Driving AdvisoryReport directly is what makes that assertable. When each
// category was printed by its own loop in the surface test, a unit test could
// prove the value reached the data structure while the loop that printed it was
// deletable with everything still green.
func TestAdvisoryReportCoversEveryCategory(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	criticalFinding := Finding{
		Severity: Critical, Role: RoleCoordinator, Table: "some_table", Privilege: PrivUpdate,
		Summary: "synthetic critical",
	}
	knownOpen := Finding{
		Severity: Critical, Role: RoleCoordinator, Table: "sync_dispatch_outbox", Privilege: PrivInsert,
		Summary: "synthetic known-open",
	}
	advisoryFinding := Finding{
		Severity: Advisory, Role: RoleDomain, Table: "other_table", Privilege: PrivSelect,
		Summary: "synthetic advisory",
	}

	report := &RoleReport{
		Stats:       []string{"synthetic stats line"},
		Findings:    []Finding{criticalFinding, knownOpen, advisoryFinding},
		SharedPairs: []string{"shared_table SELECT (evidence on: coordinator+domain)"},
		Incomplete: []IncompleteRoleSurface{{
			Role:                 RoleCoordinator,
			UndeclaredByEvidence: []PostureGapWithoutEvidence{{Table: "nobody_touches_it", Privilege: PrivUpdate}},
			DynamicSQL:           []DynamicSite{{File: "internal/brand/new.go", Line: 3, Reason: "not a constant"}},
			UnparsedLocks:        []UnparsedLockSite{{File: "internal/x/y.go", Line: 9, Statement: "LOCK \"a.b\""}},
			FuncValueConflicts:   []FuncValueConflictSite{{File: "cmd/x/deps.go", Line: 7, Field: "sources.newThing", Reason: "two implementations"}},
			WiringHops:           []UnresolvedCallSite{{File: "internal/x/y.go", Line: 11, Callee: "", Reason: "function value"}},
			NameSeedOverrides:    []NameSeedOverride{{File: "cmd/x/deps.go", Line: 5, Name: "domainPool", Function: "build", ObservedRoles: []string{"coordinator"}}},
			UnresolvedTx:         []UnresolvedTxSite{{File: "internal/x/y.go", Line: 13, Function: "Repo.Step"}},
		}},
	}

	seen := map[AdvisoryCategory]int{}
	for _, line := range AdvisoryReport(report) {
		seen[line.Category]++
	}
	for _, category := range AllAdvisoryCategories {
		if seen[category] == 0 {
			t.Errorf("AdvisoryReport emitted nothing for category %s, but the input contains one. "+
				"A category that stops being reported is indistinguishable from a category with "+
				"nothing to say, and this report is the tool's ONLY output", category)
		}
	}

	// WIRING-HOP specifically: it was collected and never reported for three
	// review rounds. Its line must name the count, because "taint stopped in N
	// places" is the number that tells a reader how much of the surface is unseen.
	var wiringLine string
	for _, line := range AdvisoryReport(report) {
		if line.Category == CategoryWiringHop {
			wiringLine = line.Text
		}
	}
	if !strings.Contains(wiringLine, "taint STOPPED") {
		t.Errorf("the wiring-hop line must say what a hop MEANS -- that SQL beyond it is invisible: %q",
			wiringLine)
	}

	// A ticketed known-open critical must be distinguishable from an untriaged
	// one, or the report flattens "someone is fixing this" into "unreviewed".
	var sawKnownOpen bool
	for _, line := range AdvisoryReport(report) {
		if line.Category == CategoryKnownOpen {
			sawKnownOpen = true
		}
	}
	if !sawKnownOpen {
		t.Error("a ticketed known-open critical must be reported under its own category")
	}
}

// TestKnownOpenLifecycleIsFullyReported covers the whole known-open lifecycle,
// not just the accepted set.
//
// An earlier version of AdvisoryReport called PartitionKnownOpen and used only its
// `accepted` result, discarding stale and unticketed, and labelled accepted entries
// with a generic "[ticketed, known open]" naming neither ticket nor reason. So the
// documented claim -- "expiry is suspended but the properties are still REPORTED"
// -- was false for two of the three, and an entry with an empty ticket was
// presented as ticketed. Same stale-claim defect as the doc comments, but in
// BEHAVIOUR: prose can be re-read, a dropped code path cannot.
func TestKnownOpenLifecycleIsFullyReported(t *testing.T) {
	real := knownOpenCriticals
	t.Cleanup(func() { knownOpenCriticals = real })

	// One entry that reproduces (accepted, with a ticket and reason), one that no
	// longer reproduces (stale), and one with no ticket at all.
	knownOpenCriticals = []KnownOpenCritical{
		{Role: RoleCoordinator, Table: "reproduces", Privilege: PrivInsert,
			Ticket: "CHAOS-1111", Why: "the documented reason"},
		{Role: RoleCoordinator, Table: "vanished", Privilege: PrivUpdate,
			Ticket: "CHAOS-2222", Why: "was real once"},
		{Role: RoleDomain, Table: "untracked", Privilege: PrivDelete,
			Ticket: "", Why: "nobody owns this"},
	}

	report := &RoleReport{Findings: []Finding{
		{Severity: Critical, Role: RoleCoordinator, Table: "reproduces", Privilege: PrivInsert,
			Summary: "synthetic reproducing critical"},
		{Severity: Critical, Role: RoleDomain, Table: "untracked", Privilege: PrivDelete,
			Summary: "synthetic untracked critical"},
	}}

	var knownOpen string
	for _, line := range AdvisoryReport(report) {
		if line.Category == CategoryKnownOpen {
			knownOpen += line.Text + "\n"
		}
	}

	// Accepted: ticket AND reason must both be visible. "[ticketed]" with neither
	// tells a reader nothing they can act on.
	if !strings.Contains(knownOpen, "CHAOS-1111") {
		t.Errorf("an accepted entry must name its TICKET, or nothing connects it to the fix:\n%s", knownOpen)
	}
	if !strings.Contains(knownOpen, "the documented reason") {
		t.Errorf("an accepted entry must carry its recorded reason:\n%s", knownOpen)
	}

	// Stale: reported, and the report must say what suspension MEANS for it.
	if !strings.Contains(knownOpen, "STALE") || !strings.Contains(knownOpen, "vanished") {
		t.Errorf("an entry that no longer reproduces must be reported as stale -- under gating it "+
			"would have failed and forced its own deletion:\n%s", knownOpen)
	}
	if !strings.Contains(knownOpen, "indefinitely") {
		t.Errorf("the stale line must state the consequence of suspended expiry, not merely the fact "+
			"of staleness:\n%s", knownOpen)
	}

	// Unticketed: must never be presented as ticketed.
	if !strings.Contains(knownOpen, "NO TICKET") || !strings.Contains(knownOpen, "untracked") {
		t.Errorf("an entry with no ticket must be reported as untracked, not labelled ticketed:\n%s",
			knownOpen)
	}
}

// TestAcknowledgementsRequireAReason makes the mandatory-reason check
// falsifiable. It could not fail before, because every real acknowledgement
// already carries a reason -- so the check had no input that would exercise it.
func TestAcknowledgementsRequireAReason(t *testing.T) {
	realBlind := acknowledgedBlindSpots
	realDynamic := acknowledgedDynamicSQL
	t.Cleanup(func() {
		acknowledgedBlindSpots = realBlind
		acknowledgedDynamicSQL = realDynamic
	})

	acknowledgedBlindSpots = []AcknowledgedBlindSpot{
		{Role: RoleDomain, Table: "t", Privilege: PrivSelect, Why: ""},
	}
	acknowledgedDynamicSQL = []AcknowledgedDynamicSQL{
		{File: "internal/x/y.go", Why: "   "},
	}

	surface := IncompleteRoleSurface{
		Role:                 RoleDomain,
		UndeclaredByEvidence: []PostureGapWithoutEvidence{{Table: "t", Privilege: PrivSelect}},
		DynamicSQL:           []DynamicSite{{File: "internal/x/y.go", Line: 1, Reason: "dynamic"}},
	}

	if _, _, unreasoned := PartitionBlindSpots([]IncompleteRoleSurface{surface}); len(unreasoned) != 1 {
		t.Errorf("a blind-spot acknowledgement with no reason must be reported, got %d", len(unreasoned))
	}
	if _, _, unreasoned := PartitionDynamicSQL([]IncompleteRoleSurface{surface}); len(unreasoned) != 1 {
		t.Errorf("a dynamic-SQL acknowledgement with a whitespace-only reason must be reported, got %d",
			len(unreasoned))
	}

	// And it must reach the REPORT, not just the partition function. Under the
	// advisory posture this no longer fails anything, but a reasonless
	// acknowledgement that is never printed is an unexplained suppression nobody
	// can review.
	var reported string
	for _, line := range AdvisoryReport(&RoleReport{Incomplete: []IncompleteRoleSurface{surface}}) {
		if line.Category == CategoryAcknowledgement {
			reported += line.Text + "\n"
		}
	}
	if !strings.Contains(reported, "has no reason recorded") ||
		!strings.Contains(reported, "has no reasoning recorded") {
		t.Errorf("reasonless acknowledgements must appear in the advisory report:\n%s", reported)
	}
}

// TestBuildPoolParamRolesMarksPartialEvidenceIncomplete exercises
// buildPoolParamRoles through a REAL package load, on the exact shape codex
// found: a parameter spelled for one role, reached by one recognised call from
// the other role AND one call the pass cannot classify.
//
// The existing H1 tests injected poolParamRoles directly and never ran the
// collector, which is precisely why this defect was invisible to them -- they
// tested the consumer against a hand-built state the producer would not have
// produced. The fixture below is a self-contained module (its own local package
// named pgxpool, so isPgxPoolPtr matches) and needs no network.
func TestBuildPoolParamRolesMarksPartialEvidenceIncomplete(t *testing.T) {
	dir := writeFixtureModule(t, `package main

import "fixture/pgxpool"

type pools struct {
	Coordinator *pgxpool.Pool
	Other       *pgxpool.Pool
}

// build executes REAL SQL through its domain-spelled parameter. The table is the
// observable consequence: an earlier version of this test had an empty body and
// asserted only on audit metadata, so it could not tell whether the SQL survived
// in the domain surface -- which is the entire property under test.
func build(domainPool *pgxpool.Pool) {
	_ = domainPool.Exec(nil, "SELECT id FROM public.fixture_partial")
}

func wireOne(p *pools) { build(p.Coordinator) }
func wireTwo(p *pools) { build(p.Other) }

func main() { wireOne(nil); wireTwo(nil) }
`)

	derived, err := DeriveForRole(dir, RoleDomain)
	if err != nil {
		t.Fatalf("DeriveForRole: %v", err)
	}
	// wireTwo passes an argument this pass cannot classify, so the observed role
	// set is a LOWER BOUND. It must not suppress the domain seed -- if it did, the
	// SQL would vanish from the domain surface entirely: a false ABSENCE.
	if surface := derived.Tables["fixture_partial"]; surface == nil || !surface.Privileges.Has(PrivSelect) {
		t.Error("the domain surface LOST fixture_partial. Partial call-site evidence suppressed the " +
			"domain seed, so real SQL disappeared -- the false absence this mechanism exists to avoid")
	}
	if len(derived.NameSeedOverrides) != 0 {
		t.Errorf("partial evidence must not be reported as an override: %+v", derived.NameSeedOverrides)
	}
}

// TestBuildPoolParamRolesOverridesOnCompleteContradictingEvidence is the control:
// with EVERY call site classified and none of them this role, the override fires
// and the SQL leaves this role's surface. Without this half the companion test
// passes for a build that simply never overrides anything.
//
// It asserts on the TABLE, not on the override record, for the same reason: a
// regression that records an override while still treating the expression as
// tainted would keep an audit-metadata assertion green while attributing real SQL
// to the wrong role.
func TestBuildPoolParamRolesOverridesOnCompleteContradictingEvidence(t *testing.T) {
	dir := writeFixtureModule(t, `package main

import "fixture/pgxpool"

type pools struct {
	Coordinator *pgxpool.Pool
}

func build(domainPool *pgxpool.Pool) {
	_ = domainPool.Exec(nil, "SELECT id FROM public.fixture_control")
}

func wireOne(p *pools) { build(p.Coordinator) }
func wireTwo(p *pools) { build(p.Coordinator) }

func main() { wireOne(nil); wireTwo(nil) }
`)

	domain, err := DeriveForRole(dir, RoleDomain)
	if err != nil {
		t.Fatalf("DeriveForRole(domain): %v", err)
	}
	if surface := domain.Tables["fixture_control"]; surface != nil && surface.Privileges.Has(PrivSelect) {
		t.Error("every call site passes the COORDINATOR pool, so this SQL must not appear in the " +
			"DOMAIN surface -- the parameter's spelling is the only thing suggesting otherwise, and " +
			"call-site evidence is supposed to outrank it")
	}
	if len(domain.NameSeedOverrides) == 0 {
		t.Error("the override must be REPORTED, not silent: a naming convention lied, and a reader " +
			"seeing a smaller surface deserves to know why")
	}

	// And it must land on the coordinator, or the override moved the SQL nowhere.
	coordinator, err := DeriveForRole(dir, RoleCoordinator)
	if err != nil {
		t.Fatalf("DeriveForRole(coordinator): %v", err)
	}
	if surface := coordinator.Tables["fixture_control"]; surface == nil || !surface.Privileges.Has(PrivSelect) {
		t.Error("the SQL must appear in the COORDINATOR surface: that is where the pool actually " +
			"comes from, and an override that removes SQL from one role without adding it to the " +
			"other has lost it")
	}
}

// writeFixtureModule builds a self-contained module with its own local pgxpool
// package (so isPgxPoolPtr matches) and no external dependencies, so these tests
// exercise the real package loader without network access.
func writeFixtureModule(t *testing.T, mainGo string) string {
	t.Helper()
	dir := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.25\n")
	write("pgxpool/pgxpool.go", `package pgxpool

type Pool struct{}

func (p *Pool) Exec(ctx interface{}, sql string, args ...interface{}) error { return nil }
`)
	write("main.go", mainGo)
	return dir
}
