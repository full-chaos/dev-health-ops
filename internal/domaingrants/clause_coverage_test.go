package domaingrants

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

// This file exists because of a measurement error in how this package's own tests
// were validated.
//
// Mutating each new predicate as a UNIT reported 13/13 mutations killed. Mutating
// the same predicates CLAUSE BY CLAUSE reported 12/26 -- nine individual clauses
// were dead, dominated by their siblings, or had no test that could distinguish
// them. A compound condition mutated wholesale changes behaviour for some input,
// so some test fails, so the verdict is KILLED -- and that verdict says nothing
// about whether any particular clause matters.
//
// Every test below targets exactly one surviving clause, with the sibling clauses
// holding their real values so the clause under test is the only difference.

// --- H3.a: the lock downgrade needs BOTH clauses -------------------------------
//
// `len(exclusive) == 0 && othersLock`. The existing shared-evidence test has both
// true, so dropping the exclusivity clause changed nothing. This case has
// othersLock TRUE and an exclusive site present: the finding must stay CRITICAL,
// because this role provably locks the table somewhere the other role does not.
func TestLockDowngradeRequiresBothExclusivityAndOtherRoleLocking(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	lockAt := func(role PoolRole, sites ...int) *DerivedSurface {
		requirement, _ := lockRequirementForMode("SHARE ROW EXCLUSIVE")
		held := requirement
		s := &TableSurface{Table: "outbox", LockRequirement: &held}
		s.Privileges.add(PrivSelect)
		for _, line := range sites {
			s.WriteLockEvidence = append(s.WriteLockEvidence, Evidence{
				File: "internal/shared/repo.go", Line: line,
				Statement: "LOCK TABLE public.outbox IN SHARE ROW EXCLUSIVE MODE",
			})
		}
		return &DerivedSurface{Role: role, Tables: map[string]*TableSurface{"outbox": s}}
	}

	// Domain locks at line 42. Coordinator locks at 42 AND at 99 -- so 99 is
	// coordinator-exclusive even though the roles share 42.
	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: lockAt(RoleDomain, 42),
			Truth: truthWith(map[string]PrivilegeSet{"outbox": selectOnly})},
		{Role: RoleCoordinator, Derived: lockAt(RoleCoordinator, 42, 99),
			Truth: truthWith(map[string]PrivilegeSet{"outbox": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range report.Findings {
		if f.Severity == Critical && f.Role == RoleCoordinator && strings.Contains(f.Summary, "LOCK IN") {
			found = true
		}
	}
	if !found {
		t.Error("the coordinator has a LOCK site the domain role does not, so the finding must stay " +
			"CRITICAL -- downgrading merely because the OTHER role also locks the table would hide a " +
			"real, role-exclusive denial")
	}
}

// --- H3.c: the straddle needs the traced-co-residency premise ------------------
//
// The existing straddle test uses a "txorigin:" group, so `traced` was already
// true and forcing it true changed nothing. A COARSE group must downgrade.
func TestTransactionStraddleWithCoarseGroupingIsAdvisory(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	// Same shape as the CRITICAL straddle test, but the TxGroup is the coarse
	// package-qualified fallback rather than a traced Begin() origin.
	const coarse = "github.com/full-chaos/dev-health-ops/internal/mat.Materializer.Step"
	surface := surfaceWith(RoleCoordinator, "coord_table", PrivSelect, "internal/mat.go", 10, coarse)
	domainOwned := &TableSurface{Table: "domain_table"}
	domainOwned.Privileges.add(PrivSelect)
	domainOwned.Evidence = append(domainOwned.Evidence, Evidence{
		File: "internal/mat.go", Line: 11, Privilege: PrivSelect,
		TxGroup: coarse, Statement: "SELECT 1",
	})
	surface.Tables["domain_table"] = domainOwned

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth: truthWith(map[string]PrivilegeSet{"domain_table": selectOnly})},
		{Role: RoleCoordinator, Derived: surface,
			Truth: truthWith(map[string]PrivilegeSet{"coord_table": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range report.Findings {
		if f.Severity == Critical && strings.Contains(f.Summary, "STRADDLES") {
			t.Errorf("a straddle grouped only by the COARSE same-function-body fallback was reported "+
				"CRITICAL. That grouping is explicitly not proof of co-residency, so blocking on it "+
				"tells maintainers to dual-grant on a guess: %s", f.Summary)
		}
	}
	joined := strings.Join(findingSummaries(report, Advisory), "\n")
	if !strings.Contains(joined, "COARSE") {
		t.Errorf("expected an ADVISORY naming the coarse grouping, got:\n%s", joined)
	}
}

// --- H5.d: implied-SELECT needs the has-other-evidence clause -----------------
//
// SELECT is synthesized onto every posture row, so a WRITE-only table legitimately
// lacks derived SELECT evidence. But a table with NO evidence at all must NOT be
// excused that way -- that is a genuine blind spot, and marking it implied would
// silently drop it from the gate.
func TestImpliedSelectOnlyExcusesTablesThatHaveOtherEvidence(t *testing.T) {
	var selectInsert PrivilegeSet
	selectInsert.add(PrivSelect)
	selectInsert.add(PrivInsert)
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	// writes_only: posture says SELECT+INSERT, derivation proves INSERT only.
	writesOnly := &TableSurface{Table: "writes_only"}
	writesOnly.Privileges.add(PrivInsert)
	writesOnly.Evidence = append(writesOnly.Evidence, Evidence{
		File: "internal/w.go", Line: 1, Privilege: PrivInsert, Statement: "INSERT", TxGroup: "tx",
	})
	// invisible: posture says SELECT, derivation proves nothing at all.
	derived := &DerivedSurface{Role: RoleCoordinator,
		Tables: map[string]*TableSurface{"writes_only": writesOnly}}

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth: truthWith(map[string]PrivilegeSet{"unrelated": selectOnly})},
		{Role: RoleCoordinator, Derived: derived,
			Truth: truthWith(map[string]PrivilegeSet{"writes_only": selectInsert, "invisible": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}

	var coordinator IncompleteRoleSurface
	for _, s := range report.Incomplete {
		if s.Role == RoleCoordinator {
			coordinator = s
		}
	}
	byTable := map[string]PostureGapWithoutEvidence{}
	for _, gap := range coordinator.UndeclaredByEvidence {
		byTable[gap.Table] = gap
	}

	if got, ok := byTable["writes_only"]; !ok || !got.ImpliedSelect {
		t.Errorf("a table with INSERT evidence must have its missing SELECT marked as implied by the "+
			"posture model, not treated as a blind spot: %+v", got)
	}
	if got, ok := byTable["invisible"]; !ok || got.ImpliedSelect {
		t.Errorf("a table with NO evidence at all must NOT be excused as implied-SELECT -- that is a "+
			"real blind spot and excusing it drops it from the gate: %+v", got)
	}

	// And the invisible one must actually reach the failure path.
	unacknowledged, _, _ := PartitionBlindSpots(report.Incomplete)
	if !strings.Contains(strings.Join(unacknowledged, "\n"), "invisible") {
		t.Errorf("the no-evidence table must be gated as unacknowledged, got %v", unacknowledged)
	}
	if strings.Contains(strings.Join(unacknowledged, "\n"), "writes_only") {
		t.Errorf("the write-only table's implied SELECT must NOT be gated, got %v", unacknowledged)
	}
}

// --- H5.e: stale acknowledged blind spots must be reported --------------------
//
// The equivalent property for knownOpenCriticals had a test; this one did not, so
// deleting the staleness check entirely was invisible.
func TestStaleAcknowledgedBlindSpotIsReported(t *testing.T) {
	// No incompleteness at all: every real acknowledgement must report stale,
	// because the gaps they excuse no longer exist.
	unacknowledged, stale, _ := PartitionBlindSpots(nil)
	if len(unacknowledged) != 0 {
		t.Fatalf("no incompleteness should mean nothing unacknowledged, got %v", unacknowledged)
	}
	if len(stale) != len(acknowledgedBlindSpots) {
		t.Fatalf("expected all %d acknowledgements reported stale against an empty incompleteness set, "+
			"got %d -- without this the list silently outlives the blind spots it excuses",
			len(acknowledgedBlindSpots), len(stale))
	}
	// And every entry must carry a reason.
	_, _, unreasoned := PartitionBlindSpots(nil)
	if len(unreasoned) != 0 {
		t.Errorf("acknowledgements without a reason: %v", unreasoned)
	}
}

// --- H5.f: pair granularity, on the exact case codex named --------------------
//
// SELECT stays visible while the UPDATE path goes dark. Under table granularity
// the table is nonempty, so it contributes NO gap and the (table, UPDATE) hole
// appears in no output at all.
func TestPairGranularityCatchesADarkUpdatePathOnAVisibleTable(t *testing.T) {
	var selectUpdate PrivilegeSet
	selectUpdate.add(PrivSelect)
	selectUpdate.add(PrivUpdate)
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	// The posture authorizes SELECT+UPDATE; the derivation proves only SELECT.
	partly := &TableSurface{Table: "half_visible"}
	partly.Privileges.add(PrivSelect)
	partly.Evidence = append(partly.Evidence, Evidence{
		File: "internal/p.go", Line: 5, Privilege: PrivSelect, Statement: "SELECT 1", TxGroup: "tx",
	})

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth: truthWith(map[string]PrivilegeSet{"unrelated": selectOnly})},
		{Role: RoleCoordinator,
			Derived: &DerivedSurface{Role: RoleCoordinator,
				Tables: map[string]*TableSurface{"half_visible": partly}},
			Truth: truthWith(map[string]PrivilegeSet{"half_visible": selectUpdate})},
	})
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, s := range report.Incomplete {
		for _, gap := range s.UndeclaredByEvidence {
			if s.Role == RoleCoordinator && gap.Table == "half_visible" && gap.Privilege == PrivUpdate {
				found = true
				if gap.ImpliedSelect {
					t.Error("UPDATE must never be excused as an implied SELECT")
				}
			}
		}
	}
	if !found {
		t.Error("the (half_visible, UPDATE) gap was not enumerated. The table still has SELECT evidence, " +
			"so a table-level check treats it as seen and this exact hole vanishes from every output -- " +
			"which is why the enumeration is per (table, privilege)")
	}

	unacknowledged, _, _ := PartitionBlindSpots(report.Incomplete)
	if !strings.Contains(strings.Join(unacknowledged, "\n"), "half_visible UPDATE") {
		t.Errorf("the dark UPDATE path must reach the failure path, got %v", unacknowledged)
	}
}

// --- H4.b: the two-implementation refusal, tested directly --------------------
//
// No in-module field in this repo has two production builders, so this branch
// cannot be reached end-to-end. It is the whole point of fail-closed resolution,
// so it is tested against the extracted decision function instead of left dead.
func TestResolveTargetConflictRefusesTwoImplementations(t *testing.T) {
	// A real FileSet and well-formed literals: `display` renders a file:line, so a
	// bare &ast.FuncLit{} (nil Type, no position) is a state the producer never
	// creates and panics here. Same discipline as the Callee="" fix -- a test whose
	// input the production path cannot produce proves nothing.
	fset := token.NewFileSet()
	file := fset.AddFile("cmd/x/dependencies.go", -1, 4096)
	newLit := func(offset int) *ast.FuncLit {
		return &ast.FuncLit{
			Type: &ast.FuncType{Func: file.Pos(offset), Params: &ast.FieldList{}},
			Body: &ast.BlockStmt{},
		}
	}
	a := &analyzer{fset: fset, rootModule: "/"}
	first := funcValueTarget{lit: newLit(10)}
	second := funcValueTarget{lit: newLit(20)}

	// First assignment: accepted.
	got, reason := a.resolveTargetConflict(funcValueTarget{}, false, first)
	if reason != "" || !got.sameAs(first) {
		t.Fatalf("a single assignment must be accepted, got %+v reason=%q", got, reason)
	}

	// Same target again: still accepted, no conflict (the fixed point revisits
	// sites, so idempotence matters).
	if _, reason := a.resolveTargetConflict(first, true, first); reason != "" {
		t.Errorf("re-observing the SAME target must not conflict: %q", reason)
	}

	// A DIFFERENT target: refused, with a reason naming both.
	got, reason = a.resolveTargetConflict(first, true, second)
	if reason == "" {
		t.Fatal("two different implementations must be refused -- picking one would analyze a body " +
			"that may not be the one that runs")
	}
	if !got.empty() {
		t.Error("a refused field must resolve to NOTHING, not to one of the candidates")
	}
	if !strings.Contains(reason, "MORE THAN ONE") {
		t.Errorf("the reason must say what happened, since it is the only thing reported: %q", reason)
	}

	// An unresolvable value: also refused, with a DIFFERENT reason, so the report
	// can distinguish "two builders" from "cannot see the value".
	_, unresolvable := a.resolveTargetConflict(funcValueTarget{}, false, funcValueTarget{})
	if unresolvable == "" || strings.Contains(unresolvable, "MORE THAN ONE") {
		t.Errorf("an unresolvable assignment needs its own distinct reason: %q", unresolvable)
	}
}

// --- H4.a: a recorded conflict must reach the failure path --------------------
func TestFuncValueConflictReachesTheGate(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: surfaceWith(RoleDomain, "t", PrivSelect, "internal/d.go", 1, "tx"),
			Truth: truthWith(map[string]PrivilegeSet{"t": selectOnly})},
		{Role: RoleCoordinator,
			Derived: &DerivedSurface{Role: RoleCoordinator,
				Tables: map[string]*TableSurface{},
				FuncValueConflicts: []FuncValueConflictSite{
					{File: "cmd/x/dependencies.go", Line: 7, Field: "sources.newRepository",
						Reason: "assigned MORE THAN ONE implementation"},
					// Third-party: must be filtered, or the gate is pure noise.
					{File: "../../pkg/mod/encoding/json/stream.go", Line: 1, Field: "scanner.step",
						Reason: "assigned MORE THAN ONE implementation"},
				}},
			Truth: truthWith(map[string]PrivilegeSet{"t": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	var coordinator IncompleteRoleSurface
	for _, s := range report.Incomplete {
		if s.Role == RoleCoordinator {
			coordinator = s
		}
	}
	if len(coordinator.FuncValueConflicts) != 1 {
		t.Fatalf("expected exactly the in-module conflict to survive filtering, got %+v",
			coordinator.FuncValueConflicts)
	}
	if coordinator.FuncValueConflicts[0].Field != "sources.newRepository" {
		t.Errorf("the wrong conflict survived: %+v", coordinator.FuncValueConflicts[0])
	}
}

// --- H1.a: the override only applies to THIS role's seed names ---------------
func TestNameOverrideOnlyAppliesToSeedNamedParameters(t *testing.T) {
	poolType := fakePoolType()
	// A pool-typed parameter named something that is NOT a seed name for any role.
	param := ast.NewIdent("pool")
	funcType := &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{{
		Names: []*ast.Ident{param}, Type: ast.NewIdent("ignored"),
	}}}}
	decl := &ast.FuncDecl{Name: ast.NewIdent("build"), Type: funcType, Body: &ast.BlockStmt{}}
	use := ast.NewIdent("pool")
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{use: {Type: poolType}}}
	ctx := funcCtx{decl: decl, info: info}

	domainSeeds, domainBarrier := seedsFor(RoleDomain)
	// A REAL *types.Func with call-site evidence naming only the coordinator. An
	// earlier version of this test passed fn=nil, which short-circuits on the
	// `fn == nil` clause and MASKED the clause under test -- the mutation removing
	// the seed-name check survived. Every sibling clause must be satisfied so the
	// clause under test is the only thing that can decide the result.
	fn := types.NewFunc(token.NoPos, nil, "build", nil)
	a := &analyzer{
		seeds: domainSeeds, barrier: domainBarrier,
		poolParamRoles: map[*types.Func]map[int]map[PoolRole]bool{
			fn: {0: {RoleCoordinator: true}},
		},
	}
	// The parameter is `pool`, which is not a seed name for any role, so the domain
	// run never seeded it by name and there is nothing to override. Firing here
	// would be a phantom correction of a convention that was never applied.
	if a.nameSeedContradicted(use, ctx, fn) {
		t.Error("the override must only fire for parameters this role would have seeded BY NAME; " +
			"`pool` is not a seed name, so no name-based seed exists to contradict")
	}
	// Control: the SAME state with a seed-named parameter MUST fire, proving the
	// negative above comes from the seed-name clause and not from something else.
	seedNamed := ast.NewIdent("domainPool")
	seedCtx := funcCtx{
		decl: &ast.FuncDecl{Name: ast.NewIdent("build"),
			Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{{
				Names: []*ast.Ident{ast.NewIdent("domainPool")}, Type: ast.NewIdent("ignored"),
			}}}},
			Body: &ast.BlockStmt{}},
		info: &types.Info{Types: map[ast.Expr]types.TypeAndValue{seedNamed: {Type: poolType}}},
	}
	if !a.nameSeedContradicted(seedNamed, seedCtx, fn) {
		t.Error("control failed: a seed-named parameter with contradicting call sites must fire")
	}
}

// TestStrictestLockModeWinsViaRecordSQLText exercises the PRODUCTION escalation
// line rather than a reimplementation of it.
//
// The first version of this test rebuilt the "keep the stricter rank" comparison
// inside the test body, so it passed while analyze.go's own copy was mutated away.
// A test that reimplements the logic it is checking cannot fail with it.
func TestStrictestLockModeWinsViaRecordSQLText(t *testing.T) {
	// Two SEPARATE recordSQLText calls, as two distinct call sites would produce.
	// Both orders, because the analyzer walks a map and the order is not stable.
	for _, order := range [][]string{
		{"LOCK TABLE public.outbox IN ROW EXCLUSIVE MODE", "LOCK TABLE public.outbox IN ACCESS EXCLUSIVE MODE"},
		{"LOCK TABLE public.outbox IN ACCESS EXCLUSIVE MODE", "LOCK TABLE public.outbox IN ROW EXCLUSIVE MODE"},
	} {
		fset := token.NewFileSet()
		file := fset.AddFile("internal/x/y.go", -1, 4096)
		a := &analyzer{fset: fset, rootModule: "/", tables: map[string]*TableSurface{}}
		for i, sql := range order {
			a.recordSQLText(sql, fset.Position(file.Pos(10*(i+1))), "tx")
		}
		surface := a.tables["outbox"]
		if surface == nil || surface.LockRequirement == nil {
			t.Fatalf("no lock requirement recorded for order %v", order)
		}
		if surface.LockRequirement.Mode != "ACCESS EXCLUSIVE" {
			t.Errorf("order %v: strictest mode did not win across call sites, got %s",
				order, surface.LockRequirement.Mode)
		}
		var selectInsert PrivilegeSet
		selectInsert.add(PrivSelect)
		selectInsert.add(PrivInsert)
		if lockSatisfiedBy(*surface.LockRequirement, selectInsert) {
			t.Errorf("order %v: INSERT must not satisfy the escalated requirement", order)
		}
	}
}

// TestTracedStraddleWithSharedEvidenceIsAdvisory covers the straddle's ATTRIBUTION
// premise on its own. The coarse-grouping test has traced=false, so `attributed`
// never decided anything there and the mutations neutralising it survived. Here
// co-residency IS proven and only the attribution is missing.
func TestTracedStraddleWithSharedEvidenceIsAdvisory(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	const traced = "txorigin:internal/shared/repo.go:5"
	// Both roles derive the SAME traced group at the SAME sites -- a type fed by
	// both pools -- so neither role can claim the transaction.
	build := func(role PoolRole) *DerivedSurface {
		coord := &TableSurface{Table: "coord_table"}
		coord.Privileges.add(PrivSelect)
		coord.Evidence = append(coord.Evidence, Evidence{
			File: "internal/shared/repo.go", Line: 10, Privilege: PrivSelect,
			TxGroup: traced, Statement: "SELECT 1",
		})
		dom := &TableSurface{Table: "domain_table"}
		dom.Privileges.add(PrivSelect)
		dom.Evidence = append(dom.Evidence, Evidence{
			File: "internal/shared/repo.go", Line: 11, Privilege: PrivSelect,
			TxGroup: traced, Statement: "SELECT 1",
		})
		return &DerivedSurface{Role: role,
			Tables: map[string]*TableSurface{"coord_table": coord, "domain_table": dom}}
	}

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: build(RoleDomain),
			Truth: truthWith(map[string]PrivilegeSet{"domain_table": selectOnly})},
		{Role: RoleCoordinator, Derived: build(RoleCoordinator),
			Truth: truthWith(map[string]PrivilegeSet{"coord_table": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range report.Findings {
		if f.Severity == Critical && strings.Contains(f.Summary, "STRADDLES") {
			t.Errorf("a traced straddle whose every evidence site is SHARED between roles was reported "+
				"CRITICAL. Co-residency is proven but attribution is not, so blocking tells maintainers "+
				"to dual-grant on a guess about whose pool runs it: %s", f.Summary)
		}
	}
	joined := strings.Join(findingSummaries(report, Advisory), "\n")
	if !strings.Contains(joined, "SHARED with another role") {
		t.Errorf("expected the straddle to downgrade citing shared evidence, got:\n%s", joined)
	}
}

// TestLockWithNoEvidenceStaysCritical pins the `othersLock` clause of the lock
// downgrade. It guards a degenerate state -- a LockRequirement recorded with no
// lock evidence -- which production never produces (analyze.go sets both together)
// but which a future refactor could. Without the clause that state would silently
// DOWNGRADE, turning a missing grant into an advisory.
func TestLockWithNoEvidenceStaysCritical(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	requirement, _ := lockRequirementForMode("SHARE ROW EXCLUSIVE")
	held := requirement
	// LockRequirement set, WriteLockEvidence deliberately empty.
	s := &TableSurface{Table: "outbox", LockRequirement: &held}
	s.Privileges.add(PrivSelect)

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth: truthWith(map[string]PrivilegeSet{"unrelated": selectOnly})},
		{Role: RoleCoordinator,
			Derived: &DerivedSurface{Role: RoleCoordinator, Tables: map[string]*TableSurface{"outbox": s}},
			Truth:   truthWith(map[string]PrivilegeSet{"outbox": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range report.Findings {
		if f.Severity == Critical && strings.Contains(f.Summary, "LOCK IN") {
			found = true
		}
	}
	if !found {
		t.Error("a lock requirement with no recorded evidence must stay CRITICAL: with no sites there " +
			"is nothing to call shared, so downgrading would hide a denial behind an advisory")
	}
}
