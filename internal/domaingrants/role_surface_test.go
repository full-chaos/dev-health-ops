package domaingrants

import (
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"testing"
)

// TestReportCoordinatorGrantSurface is a REPORT, not a gate, and the name says so
// deliberately.
//
// # Why advisory
//
// Three adversarial review rounds each found new places where THE ANALYSIS CANNOT
// SEE SOMETHING AND THE CHECK PASSES ANYWAY: unresolved callees, unresolved
// interface dispatch, function-valued fields with no single target, dynamic SQL,
// unparsed statements, non-convergence, unrecognised lock forms, quoted
// identifiers. Each was fixed where it was found and reappeared elsewhere -- one
// defect with many addresses, discovered one review at a time.
//
// An advisory tool that is sometimes wrong is useful: it puts derived evidence in
// front of a reviewer who can weigh it. A GATE that passes when the analysis is
// blind is worse than no gate, because it LICENSES the hand-written rows it was
// built to check -- which is how this epic produced green tickets over dormant
// code in the first place.
//
// So: this test PRINTS everything and FAILS on nothing about the surface. A
// passing run here is NOT evidence that the coordinator posture is correct.
// TestDomainGrantSurfaceMatchesQuerySurface continues to gate the domain role as
// it always has.
//
// Promoting this to a gate requires the blind-spot closure argument: a partition
// of everything the analysis can fail to see, with each cell either failing the
// check or documented as safe to accept PER SITE (not per file). Tracked as
// CHAOS-3164.
//
// The only failures here are TOOL-BROKEN conditions -- the analysis could not run
// at all. Those are not findings about the code under analysis, and reporting
// nothing because the tool crashed would be the same silence this whole checker
// exists to avoid.
func TestReportCoordinatorGrantSurface(t *testing.T) {
	root := findModuleRoot(t)

	inputs := make([]RoleInput, 0, len(AllPoolRoles))
	for _, role := range AllPoolRoles {
		derived, err := DeriveForRole(root, role)
		if err != nil {
			t.Fatalf("DeriveForRole(%s): %v -- the analysis could not run, so this report says "+
				"nothing about the posture either way", role, err)
		}
		truth, err := LoadGroundTruthForRole(role)
		if err != nil {
			t.Fatalf("LoadGroundTruthForRole(%s): %v", role, err)
		}
		inputs = append(inputs, RoleInput{Role: role, Derived: derived, Truth: truth})
	}

	report, err := CompareRoles(inputs)
	if err != nil {
		t.Fatalf("CompareRoles: %v", err)
	}

	byCategory := map[AdvisoryCategory][]string{}
	for _, line := range AdvisoryReport(report) {
		byCategory[line.Category] = append(byCategory[line.Category], line.Text)
	}
	for _, category := range AllAdvisoryCategories {
		entries := byCategory[category]
		t.Logf("=== %s (%d) ===", category, len(entries))
		for _, entry := range entries {
			t.Logf("    %s", entry)
		}
	}
	t.Log("ADVISORY ONLY: nothing above fails this test. A passing run is NOT evidence that the " +
		"coordinator posture is correct -- see this test's doc comment for why, and what would be " +
		"required to promote it to a gate.")
}

// TestPoolRoleSeedsMatchLiveWiringSites is the anti-vacuity guard for the whole
// per-role derivation. Every seed name is a bet that the wiring code still spells
// the pool that way. If a rename made a role's seed set match nothing, that
// role's surface would derive as EMPTY and every check against it would pass
// vacuously -- the gate would go green precisely because it had stopped looking.
//
// So: assert each role derives a non-trivial surface AND that its seed sites
// land in the binaries that are supposed to own that pool.
func TestPoolRoleSeedsMatchLiveWiringSites(t *testing.T) {
	root := findModuleRoot(t)

	// Binaries each role's pool must be wired in. The coordinator set is the
	// CHAOS-3114 partition: reconciler, scheduler, workerctl and nothing else.
	expectedSeedOwners := map[PoolRole][]string{
		RoleDomain: {
			"cmd/dev-health-worker/",
			"cmd/dev-health-stream-runner/",
			"cmd/dev-health-workerctl/",
			"cmd/dev-health-reconciler/",
		},
		RoleCoordinator: {
			"cmd/dev-health-reconciler/",
			"cmd/dev-health-scheduler/",
			"cmd/dev-health-workerctl/",
		},
	}

	for _, role := range AllPoolRoles {
		derived, err := DeriveForRole(root, role)
		if err != nil {
			t.Fatalf("DeriveForRole(%s): %v", role, err)
		}
		if len(derived.SeedSites) == 0 {
			t.Fatalf("role %s derived ZERO seed sites: its seed names in poolRoleSeeds no longer match any "+
				"wiring site, so its surface is empty and every check against it passes vacuously", role)
		}
		if len(derived.Tables) == 0 {
			t.Fatalf("role %s derived ZERO tables from %d seed sites", role, len(derived.SeedSites))
		}
		for _, prefix := range expectedSeedOwners[role] {
			found := false
			for _, site := range derived.SeedSites {
				if strings.HasPrefix(site.File, prefix) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("role %s has no seed site under %s, but that binary is supposed to wire this pool -- "+
					"either the wiring was removed or it was renamed out of poolRoleSeeds' reach", role, prefix)
			}
		}
	}
}

// TestSeedsForBuildsTheOtherRolesAsBarrier pins the barrier's composition: it
// must be exactly the union of every OTHER role's seed names, and it must not be
// empty. An empty barrier silently turns a per-role derivation back into a
// union-of-all-pools derivation wearing one role's name.
func TestSeedsForBuildsTheOtherRolesAsBarrier(t *testing.T) {
	for _, role := range AllPoolRoles {
		seeds, barrier := seedsFor(role)
		if len(barrier.fields) == 0 && len(barrier.getters) == 0 && len(barrier.idents) == 0 {
			t.Fatalf("role %s got an EMPTY barrier: the other role's pool roots would fall through "+
				"exprTainted's permissive rules into this role's surface", role)
		}
		for name := range seeds.fields {
			if barrier.fields[name] {
				t.Errorf("role %s: %q is both a seed and a barrier field", role, name)
			}
		}
		for other, otherSeeds := range poolRoleSeeds {
			if other == role {
				continue
			}
			for name := range otherSeeds.fields {
				if !barrier.fields[name] {
					t.Errorf("role %s barrier is missing role %s's field seed %q", role, other, name)
				}
			}
			for name := range otherSeeds.getters {
				if !barrier.getters[name] {
					t.Errorf("role %s barrier is missing role %s's getter seed %q", role, other, name)
				}
			}
			for name := range otherSeeds.idents {
				if !barrier.idents[name] {
					t.Errorf("role %s barrier is missing role %s's ident seed %q", role, other, name)
				}
			}
		}
	}
}

// fakePoolType builds a *types.Type that isPgxPoolPtr accepts, so the barrier
// can be exercised without loading the real driver.
func fakePoolType() types.Type {
	pkg := types.NewPackage("github.com/jackc/pgx/v5/pgxpool", "pgxpool")
	named := types.NewNamed(
		types.NewTypeName(token.NoPos, pkg, "Pool", nil),
		types.NewStruct(nil, nil),
		nil,
	)
	return types.NewPointer(named)
}

// TestBarrierStopsTaintFromTheOtherRolesPool is the behavioural test for the
// barrier, and it is deliberately independent of whether the barrier currently
// changes this repo's derived surface (measured: it does not, because no
// multi-pool struct today reaches a SQL sink through the wrong field). It fails
// if the barrier is removed or moved AFTER exprTainted's permissive
// fall-throughs.
//
// The shape it models is the real hazard: `pools.Domain` selected off a receiver
// that is already tainted because a SIBLING field holds the coordinator pool.
// exprTainted's "anything selected off a tainted base is tainted" rule would
// return true for it, and the barrier is the only thing that says otherwise.
func TestBarrierStopsTaintFromTheOtherRolesPool(t *testing.T) {
	poolType := fakePoolType()

	// `pools.Domain`, where `pools` is a local the caller has already tainted.
	base := ast.NewIdent("pools")
	selector := &ast.SelectorExpr{X: base, Sel: ast.NewIdent("Domain")}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{
		selector: {Type: poolType},
		base:     {Type: types.Typ[types.Int]}, // irrelevant, must merely exist
	}}
	locals := map[string]bool{"pools": true}

	seeds, barrier := seedsFor(RoleCoordinator)
	coordinator := &analyzer{seeds: seeds, barrier: barrier}
	if coordinator.exprTainted(selector, info, locals) {
		t.Error("coordinator run treated `pools.Domain` as coordinator-tainted: the barrier is not " +
			"stopping the other role's pool root, so this role's surface silently includes the other's")
	}
	if !coordinator.isBarrierExpr(selector, info) {
		t.Error("isBarrierExpr did not recognize `pools.Domain` as the domain role's root")
	}

	// Control: the SAME expression must still be tainted in a DOMAIN run, so the
	// test proves the barrier is role-directed rather than just always false.
	domainSeeds, domainBarrier := seedsFor(RoleDomain)
	domain := &analyzer{seeds: domainSeeds, barrier: domainBarrier}
	if !domain.exprTainted(selector, info, locals) {
		t.Error("domain run did NOT treat `pools.Domain` as tainted -- the seed lookup itself is broken, " +
			"which would empty the domain surface")
	}
}

// TestCallSiteEvidenceOutranksParameterSpelling pins the fix for the attribution
// INVERSION: a parameter spelled like one role's pool but only ever passed
// another role's pool must NOT be seeded by its name.
//
// Why this is the worst failure mode in the package: it does not merely lose
// surface, it MISDIRECTS it. `build(domainPool *pgxpool.Pool)` called only with
// the coordinator pool would be seeded by the domain run from its spelling while
// the coordinator run's barrier discards it, so downstream SQL looks
// domain-EXCLUSIVE -- and role-exclusive evidence is exactly what this gate
// escalates to CRITICAL. It would confidently tell a maintainer to grant the
// privilege to the wrong role, and every "is it granted somewhere" check would
// still pass.
func TestCallSiteEvidenceOutranksParameterSpelling(t *testing.T) {
	poolType := fakePoolType()

	// `func build(domainPool *pgxpool.Pool)` -- one pool-typed parameter, spelled
	// for the DOMAIN role.
	param := ast.NewIdent("domainPool")
	funcType := &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{{
		Names: []*ast.Ident{param},
		Type:  ast.NewIdent("ignored"),
	}}}}
	decl := &ast.FuncDecl{Name: ast.NewIdent("build"), Type: funcType, Body: &ast.BlockStmt{}}
	use := ast.NewIdent("domainPool")
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{use: {Type: poolType}}}
	ctx := funcCtx{decl: decl, info: info}
	fn := types.NewFunc(token.NoPos, nil, "build", nil)

	domainSeeds, domainBarrier := seedsFor(RoleDomain)

	// Case 1: call sites pass ONLY the coordinator pool. The domain run must
	// refuse the name-based seed.
	contradicted := &analyzer{
		seeds: domainSeeds, barrier: domainBarrier,
		poolParamRoles: map[*types.Func]map[int]map[PoolRole]bool{
			fn: {0: {RoleCoordinator: true}},
		},
	}
	if !contradicted.nameSeedContradicted(use, ctx, fn) {
		t.Error("a parameter named domainPool that is only ever passed the coordinator pool was still " +
			"treated as a domain seed -- this is the attribution inversion, and it directs the grant " +
			"to the wrong role")
	}

	// Case 2: call sites pass the domain pool too. The name agrees with the
	// evidence, so it must still seed -- otherwise the fix would delete real
	// surface rather than correcting a lie.
	agreeing := &analyzer{
		seeds: domainSeeds, barrier: domainBarrier,
		poolParamRoles: map[*types.Func]map[int]map[PoolRole]bool{
			fn: {0: {RoleDomain: true, RoleCoordinator: true}},
		},
	}
	if agreeing.nameSeedContradicted(use, ctx, fn) {
		t.Error("call sites DO pass the domain pool, so the domain seed must stand")
	}

	// Case 3: no resolvable call site. The convention is the only signal left and
	// dropping it would lose real surface, so the name still applies.
	noEvidence := &analyzer{
		seeds: domainSeeds, barrier: domainBarrier,
		poolParamRoles: map[*types.Func]map[int]map[PoolRole]bool{},
	}
	if noEvidence.nameSeedContradicted(use, ctx, fn) {
		t.Error("with no call-site evidence the name must still seed; suppressing it would trade a " +
			"false attribution for a false absence")
	}

	// Case 4: a LOCAL of the same name has no call sites to consult, so the
	// override must not fire on it (paramIndex < 0).
	localCtx := funcCtx{
		decl: &ast.FuncDecl{Name: ast.NewIdent("other"),
			Type: &ast.FuncType{Params: &ast.FieldList{}}, Body: &ast.BlockStmt{}},
		info: info,
	}
	if contradicted.nameSeedContradicted(use, localCtx, fn) {
		t.Error("a local variable is not a parameter and has no call sites; the override must not fire")
	}
}

// TestFuncValueTargetsResolveTheKnownWiringHops pins the function-value
// resolution to the specific hops it was built for. Without it the entire
// scheduler and fixed-engine surface is invisible to the coordinator
// derivation -- 8 of CoordinatorPosture()'s tables -- and the gate would pass
// while checking nothing about them.
//
// Pinning the CALL SITES rather than a count is deliberate: a count keeps
// passing when one hop stops resolving and an unrelated one starts.
func TestFuncValueTargetsResolveTheKnownWiringHops(t *testing.T) {
	root := findModuleRoot(t)
	derived, err := DeriveForRole(root, RoleCoordinator)
	if err != nil {
		t.Fatalf("DeriveForRole(coordinator): %v", err)
	}

	// Assert the MECHANISM and the TARGET, not a count and not merely that some
	// table showed up.
	//
	// Why that distinction is load-bearing: taint ALSO re-seeds on the naming
	// convention (a parameter literally named `coordinatorPool` is a seed root), so
	// a table can become visible EITHER through this hop resolution OR because
	// someone renamed a parameter. Those are not equivalent -- the convention is a
	// comment enforced by nothing, and these three scheduler hops are the proof:
	// they carried the coordinator pool with their parameter named `pool` and
	// nothing caught it. A test asserting a table's presence, or a total count of
	// resolved hops, would keep passing after such a rename while the mechanism it
	// exists to pin had silently stopped working.
	wantTargets := map[string]string{
		"schedulerRuntimeSources.newRepository":         "func literal",
		"schedulerRuntimeSources.newOccurrences":        "func literal",
		"schedulerRuntimeSources.newFixedLoop":          "buildFixedScheduleLoop",
		"reconcilerDependencySources.buildRelay":        "buildReconcilerRelay",
		"reconcilerDependencySources.buildSyncMutation": "buildSyncMutationPipeline",
	}
	got := map[string]string{}
	for _, resolved := range derived.FuncValueResolved {
		got[resolved.Field] = resolved.Target
	}
	for field, wantTarget := range wantTargets {
		target, ok := got[field]
		if !ok {
			t.Errorf("function-typed field %s no longer resolves through func-value resolution; "+
				"taint stops there and everything it builds becomes invisible to this role's surface. "+
				"NOTE: the affected tables may still appear via the coordinatorPool naming convention, "+
				"which is exactly why this test asserts the mechanism and not the tables", field)
			continue
		}
		if !strings.Contains(target, wantTarget) {
			t.Errorf("function-typed field %s resolved to %q, want something containing %q -- "+
				"it builds something else now, so what this hop proves has changed", field, target, wantTarget)
			continue
		}
		t.Logf("%s -> %s", field, target)
	}

	// Corroboration only: the surface those hops unlock is present. Deliberately
	// secondary to the assertions above -- after a `pool` -> `coordinatorPool`
	// rename anywhere in the scheduler wiring this block becomes satisfiable by the
	// naming convention alone, and stops being evidence about the hop.
	for _, table := range []string{
		"scheduled_jobs",
		"scheduled_sync_occurrences",
		"fixed_schedule_occurrences",
		"sync_configurations",
	} {
		if surface := derived.Tables[table]; surface == nil || surface.Privileges.Empty() {
			t.Errorf("coordinator surface lost %q entirely -- neither the wiring hop nor the "+
				"naming convention reaches it now", table)
		}
	}
}

// TestLockTableRequirementIsModeAware pins the LOCK TABLE rule per role, per
// MODE, against the mapping measured on PostgreSQL 18.4 (see LockRequirement in
// sql.go for the full table and the probe it came from).
//
// History worth keeping, because it is two different mistakes:
//
//   - Mutation testing found the rule was UNCOVERED: deleting the whole check
//     left the repo-wide gate green, since every table this repo LOCKs already
//     holds a satisfying privilege for some other reason.
//   - The first version of this test then pinned the rule as "any one of
//     INSERT/UPDATE/DELETE, for every mode", asserting that INSERT alone
//     SATISFIES a SHARE ROW EXCLUSIVE lock. Measurement says it does not. That
//     assertion made a wrong rule authoritative, which is worse than the
//     uncovered rule was -- and it is the CHAOS-3113 defect family exactly
//     (workerctl's route rollback LOCKs worker_job_outbox IN SHARE ROW EXCLUSIVE
//     MODE while the domain role holds only SELECT+INSERT).
//
// Three distinct rules live in this area and none may be collapsed into another:
// ACCESS SHARE (SELECT suffices), ROW SHARE / ROW EXCLUSIVE (any of I/U/D), and
// everything stricter (U/D only). `SELECT ... FOR UPDATE` is a fourth shape,
// requiring UPDATE specifically, handled as an ordinary privilege elsewhere.
func TestLockTableRequirementIsModeAware(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)
	var selectInsert PrivilegeSet
	selectInsert.add(PrivSelect)
	selectInsert.add(PrivInsert)
	var selectUpdate PrivilegeSet
	selectUpdate.add(PrivSelect)
	selectUpdate.add(PrivUpdate)
	var selectDelete PrivilegeSet
	selectDelete.add(PrivSelect)
	selectDelete.add(PrivDelete)
	var updateOnly PrivilegeSet
	updateOnly.add(PrivUpdate)

	lockSurface := func(role PoolRole, mode string) *DerivedSurface {
		requirement, recognized := lockRequirementForMode(mode)
		s := &TableSurface{Table: "outbox"}
		s.Privileges.add(PrivSelect)
		s.Evidence = append(s.Evidence, Evidence{
			File: "internal/jobroute/control.go", Line: 197, Privilege: PrivSelect,
			Statement: "LOCK TABLE public.outbox IN " + mode + " MODE", TxGroup: "tx",
		})
		if recognized {
			held := requirement
			s.LockRequirement = &held
			s.WriteLockEvidence = append(s.WriteLockEvidence, Evidence{
				File: "internal/jobroute/control.go", Line: 197,
				Statement: "LOCK TABLE public.outbox IN " + mode + " MODE",
			})
		}
		return &DerivedSurface{Role: role, Tables: map[string]*TableSurface{"outbox": s}}
	}

	// One row per (mode, posture) cell of the measured table.
	cases := []struct {
		mode        string
		posture     PrivilegeSet
		postureName string
		wantFinding bool
	}{
		// ACCESS SHARE: SELECT is enough -- and so is UPDATE ALONE, because the
		// demand is a disjunction rather than a conjunction with SELECT.
		{"ACCESS SHARE", selectOnly, "SELECT", false},
		{"ACCESS SHARE", updateOnly, "UPDATE only (no SELECT)", false},

		// ROW SHARE and ROW EXCLUSIVE: SELECT alone is NOT enough (the docs say
		// ROW SHARE needs only SELECT; measurement says otherwise), and INSERT IS.
		{"ROW SHARE", selectOnly, "SELECT", true},
		{"ROW SHARE", selectInsert, "SELECT+INSERT", false},
		{"ROW EXCLUSIVE", selectOnly, "SELECT", true},
		{"ROW EXCLUSIVE", selectInsert, "SELECT+INSERT", false},

		// Stricter modes: INSERT does NOT satisfy them. This is the row the
		// previous version of this test got backwards.
		{"SHARE UPDATE EXCLUSIVE", selectInsert, "SELECT+INSERT", true},
		{"SHARE", selectInsert, "SELECT+INSERT", true},
		{"SHARE ROW EXCLUSIVE", selectInsert, "SELECT+INSERT", true},
		{"EXCLUSIVE", selectInsert, "SELECT+INSERT", true},
		{"ACCESS EXCLUSIVE", selectInsert, "SELECT+INSERT", true},

		// UPDATE or DELETE satisfies the strictest mode.
		{"SHARE ROW EXCLUSIVE", selectUpdate, "SELECT+UPDATE", false},
		{"SHARE ROW EXCLUSIVE", selectDelete, "SELECT+DELETE", false},
		{"ACCESS EXCLUSIVE", selectUpdate, "SELECT+UPDATE", false},

		// A lock-only path authorized by UPDATE alone needs NO SELECT. If the
		// model required SELECT as well, this would report a finding.
		{"SHARE ROW EXCLUSIVE", updateOnly, "UPDATE only (no SELECT)", false},
	}

	for _, tc := range cases {
		// Only the coordinator locks, so its LOCK evidence is role-exclusive and
		// the shared-evidence downgrade does not apply.
		report, err := CompareRoles([]RoleInput{
			{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
				Truth: truthWith(map[string]PrivilegeSet{"unrelated": selectOnly})},
			{Role: RoleCoordinator, Derived: lockSurface(RoleCoordinator, tc.mode),
				Truth: truthWith(map[string]PrivilegeSet{"outbox": tc.posture})},
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
		if found != tc.wantFinding {
			verb := "did not report"
			if found {
				verb = "reported"
			}
			t.Errorf("LOCK IN %s MODE with posture %s: %s a CRITICAL, want wantFinding=%v. "+
				"The measured PostgreSQL 18.4 mapping is in LockRequirement's doc comment; if this "+
				"disagrees, re-run the probe rather than adjusting the expectation",
				tc.mode, tc.postureName, verb, tc.wantFinding)
		}
	}
}

// TestLockTableSharedEvidenceIsDowngraded is the H3 half for the lock path: the
// shared-evidence downgrade must apply here too. A repository type constructed
// from both pools has its locking method attributed to both, so a blocking
// "grant the coordinator too" would be the same over-widening the per-privilege
// path already refuses.
func TestLockTableSharedEvidenceIsDowngraded(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	// The SAME file:line locks the table for BOTH roles.
	shared := func(role PoolRole) *DerivedSurface {
		requirement, _ := lockRequirementForMode("SHARE ROW EXCLUSIVE")
		held := requirement
		s := &TableSurface{Table: "outbox", LockRequirement: &held}
		s.Privileges.add(PrivSelect)
		s.WriteLockEvidence = append(s.WriteLockEvidence, Evidence{
			File: "internal/shared/repo.go", Line: 42,
			Statement: "LOCK TABLE public.outbox IN SHARE ROW EXCLUSIVE MODE",
		})
		return &DerivedSurface{Role: role, Tables: map[string]*TableSurface{"outbox": s}}
	}

	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: shared(RoleDomain),
			Truth: truthWith(map[string]PrivilegeSet{"outbox": selectOnly})},
		{Role: RoleCoordinator, Derived: shared(RoleCoordinator),
			Truth: truthWith(map[string]PrivilegeSet{"outbox": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range report.Findings {
		if f.Severity == Critical && strings.Contains(f.Summary, "LOCK IN") {
			t.Errorf("a LOCK whose every site is shared between roles was reported CRITICAL, which "+
				"tells maintainers to widen both postures on evidence that cannot attribute either: %s", f.Summary)
		}
	}
	joined := strings.Join(findingSummaries(report, Advisory), "\n")
	if !strings.Contains(joined, "SHARED with another role") {
		t.Errorf("expected the lock path to emit the shared-evidence ADVISORY, got:\n%s", joined)
	}
}

// TestCompareRolesRefusesASingleRole pins the refusal rather than the happy
// path: a one-role run cannot check attribution at all, and returning a clean
// report for it would be the exact "green gate that checked nothing" failure this
// package exists to prevent.
func TestCompareRolesRefusesASingleRole(t *testing.T) {
	truth := &GroundTruth{
		RequiredTablePrivileges: map[string]PrivilegeSet{"t": {}},
		ColumnScopedTables:      map[string]bool{},
	}
	_, err := CompareRoles([]RoleInput{{
		Role:    RoleDomain,
		Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
		Truth:   truth,
	}})
	if err == nil {
		t.Fatal("CompareRoles accepted a single role: attribution is unverifiable with one role, " +
			"and a nil error here reads as 'attribution checked and clean'")
	}
}

// --- Synthetic-surface unit tests for the comparison logic itself. ---
//
// These build DerivedSurface/GroundTruth values by hand so each rule can be
// killed independently. The whole-repo gate above cannot do that: it passes
// today, so it cannot demonstrate that any individual rule still bites.

func surfaceWith(role PoolRole, table string, p Privilege, file string, line int, txGroup string) *DerivedSurface {
	s := &TableSurface{Table: table}
	s.Privileges.add(p)
	s.Evidence = append(s.Evidence, Evidence{File: file, Line: line, Privilege: p, TxGroup: txGroup, Statement: "SELECT 1"})
	return &DerivedSurface{Role: role, Tables: map[string]*TableSurface{table: s}}
}

func truthWith(tables map[string]PrivilegeSet) *GroundTruth {
	return &GroundTruth{RequiredTablePrivileges: tables, ColumnScopedTables: map[string]bool{}}
}

func findingSummaries(report *RoleReport, severity Severity) []string {
	var out []string
	for _, f := range report.Findings {
		if f.Severity == severity {
			out = append(out, f.Summary)
		}
	}
	sort.Strings(out)
	return out
}

// TestRoleExclusiveEvidenceIsCritical: a pair proven ONLY through this role's
// pool and absent from its posture is a real 42501.
func TestRoleExclusiveEvidenceIsCritical(t *testing.T) {
	var domainSet PrivilegeSet
	domainSet.add(PrivSelect)

	report, err := CompareRoles([]RoleInput{
		{
			Role:    RoleDomain,
			Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth:   truthWith(map[string]PrivilegeSet{"other_table": domainSet}),
		},
		{
			Role:    RoleCoordinator,
			Derived: surfaceWith(RoleCoordinator, "outbox", PrivInsert, "internal/mat.go", 87, "tx"),
			Truth:   truthWith(map[string]PrivilegeSet{"outbox": domainSet}), // SELECT only
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	criticals := findingSummaries(report, Critical)
	if len(criticals) != 1 || !strings.Contains(criticals[0], "needs INSERT") {
		t.Fatalf("expected one CRITICAL naming the missing INSERT, got %v", criticals)
	}
	if !strings.Contains(criticals[0], "ONLY through the coordinator pool") {
		t.Errorf("CRITICAL did not state the evidence was role-exclusive: %s", criticals[0])
	}
}

// TestSharedEvidenceIsAdvisoryNotCritical is the remaining_metric_* case: when
// every proving site is shared with the other role, the pair comes from a type
// fed by both pools and this tool cannot attribute it. Reporting it CRITICAL
// would push a posture WIDER than the hand derivation, which is how a
// least-privilege split degrades.
func TestSharedEvidenceIsAdvisoryNotCritical(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	// The SAME file:line proves UPDATE for both roles -- a dual-constructed type.
	const file, line = "internal/jobs/metrics/remaining/postgres.go", 418
	report, err := CompareRoles([]RoleInput{
		{
			Role:    RoleDomain,
			Derived: surfaceWith(RoleDomain, "metric_runs", PrivUpdate, file, line, "tx"),
			Truth:   truthWith(map[string]PrivilegeSet{"metric_runs": selectOnly}),
		},
		{
			Role:    RoleCoordinator,
			Derived: surfaceWith(RoleCoordinator, "metric_runs", PrivUpdate, file, line, "tx"),
			Truth:   truthWith(map[string]PrivilegeSet{"metric_runs": selectOnly}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if criticals := findingSummaries(report, Critical); len(criticals) != 0 {
		t.Fatalf("shared-evidence pair reported CRITICAL, which would license widening a posture "+
			"beyond what the code proves: %v", criticals)
	}
	advisories := findingSummaries(report, Advisory)
	joined := strings.Join(advisories, "\n")
	if !strings.Contains(joined, "SHARED with another role") {
		t.Fatalf("expected an ADVISORY explaining the shared-evidence limitation, got:\n%s", joined)
	}
	if !strings.Contains(joined, "DO NOT widen") {
		t.Errorf("ADVISORY did not warn against widening the posture: %s", joined)
	}
	if len(report.SharedPairs) != 1 {
		t.Errorf("expected the pair in the DERIVED shared set, got %v", report.SharedPairs)
	}
}

// TestTransactionStraddleIsCritical is the veto check: one transaction, tables
// on opposite sides of the partition. It cannot be satisfied by routing, because
// a transaction runs on exactly one pool.
func TestTransactionStraddleIsCritical(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	// One traced transaction on the coordinator pool touching two tables, only
	// one of which the coordinator declares. The other belongs to domain.
	coordinatorSurface := surfaceWith(RoleCoordinator, "coord_table", PrivSelect,
		"internal/mat.go", 10, "txorigin:internal/mat.go:5")
	domainOwned := &TableSurface{Table: "domain_table"}
	domainOwned.Privileges.add(PrivSelect)
	domainOwned.Evidence = append(domainOwned.Evidence, Evidence{
		File: "internal/mat.go", Line: 11, Privilege: PrivSelect,
		TxGroup: "txorigin:internal/mat.go:5", Statement: "SELECT 1",
	})
	coordinatorSurface.Tables["domain_table"] = domainOwned

	report, err := CompareRoles([]RoleInput{
		{
			Role:    RoleDomain,
			Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth:   truthWith(map[string]PrivilegeSet{"domain_table": selectOnly}),
		},
		{
			Role:    RoleCoordinator,
			Derived: coordinatorSurface,
			Truth:   truthWith(map[string]PrivilegeSet{"coord_table": selectOnly}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// ADVISORY by decision, never Critical: co-residency is INFERRED at both
	// precisions. A "txorigin" group means the pgx.Tx traced to one Begin() source
	// POSITION -- buildTxOrigins does no control-flow analysis, so mutually
	// exclusive branches after that Begin carry the same origin and would be
	// reported as one transaction. This finding can only widen a posture, so it
	// must not block on an inference.
	if criticals := strings.Join(findingSummaries(report, Critical), "\n"); strings.Contains(criticals, "STRADDLE") {
		t.Fatalf("a straddle must never be CRITICAL -- co-residency is inferred, and a blocking "+
			"finding here would tell maintainers to dual-grant on a guess:\n%s", criticals)
	}
	joined := strings.Join(findingSummaries(report, Advisory), "\n")
	if !strings.Contains(joined, "POSSIBLE TRANSACTION STRADDLE") {
		t.Fatalf("straddling transaction was not reported at all:\n%s", joined)
	}
	if !strings.Contains(joined, "NOT proof that these statements co-execute") {
		t.Errorf("the traced-origin finding must state what the tracing does and does not prove: %s", joined)
	}
}

// TestMisattributionCandidateIsReported covers the leak DIRECTION the static
// analysis cannot prove: a posture row this role has no evidence for while the
// other role does. Advisory by design (see roles.go's header), but it must never
// be silent -- a privilege granted to the wrong role passes every "granted
// somewhere" check.
func TestMisattributionCandidateIsReported(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	report, err := CompareRoles([]RoleInput{
		{
			Role:    RoleDomain,
			Derived: surfaceWith(RoleDomain, "only_domain_touches_it", PrivSelect, "internal/d.go", 1, "tx"),
			Truth:   truthWith(map[string]PrivilegeSet{"only_domain_touches_it": selectOnly}),
		},
		{
			Role:    RoleCoordinator,
			Derived: &DerivedSurface{Role: RoleCoordinator, Tables: map[string]*TableSurface{}},
			// Coordinator declares it but has no call site for it.
			Truth: truthWith(map[string]PrivilegeSet{"only_domain_touches_it": selectOnly}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(findingSummaries(report, Advisory), "\n")
	if !strings.Contains(joined, "POSSIBLE MISATTRIBUTION") {
		t.Fatalf("a posture row with no evidence for its own role, held by another role, was not "+
			"flagged as a misattribution candidate:\n%s", joined)
	}
}

// TestIncompletenessNamesUndeclaredPostureTables pins the silence-is-not-
// confirmation property: a posture table the derivation never saw must appear in
// the enumerated incompleteness, not vanish.
func TestIncompletenessNamesUndeclaredPostureTables(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	report, err := CompareRoles([]RoleInput{
		{
			Role:    RoleDomain,
			Derived: surfaceWith(RoleDomain, "seen", PrivSelect, "internal/d.go", 1, "tx"),
			Truth:   truthWith(map[string]PrivilegeSet{"seen": selectOnly}),
		},
		{
			Role: RoleCoordinator,
			Derived: &DerivedSurface{Role: RoleCoordinator, Tables: map[string]*TableSurface{},
				Unresolved: []UnresolvedCallSite{
					// KEPT: a call through a function-typed field. The callee is the
					// BARE FIELD NAME, which is what handleCall actually records for
					// this shape (calleeDisplay = fn.Sel.Name) -- an earlier version
					// of this test used Callee:"" instead, a state selector calls
					// never produce, so it exercised a path the analyzer cannot
					// reach and let the real one regress.
					{File: "cmd/x/dependencies.go", Line: 42, Callee: "newRepository", Reason: "function-typed field"},
					// KEPT: in-module interface dispatch with no unique implementer.
					{File: "internal/x/y.go", Line: 7,
						Callee: "(github.com/full-chaos/dev-health-ops/internal/x.Thing).Do",
						Reason: "multiple implementers"},
					// DROPPED: a third-party sink. A tainted argument reaching
					// pgx.Tx.Exec IS the SQL site, already extracted -- calling it
					// "taint stopped here" is backwards and it would bury the
					// hops above.
					{File: "internal/x/y.go", Line: 8,
						Callee: "(github.com/jackc/pgx/v5.Tx).Exec", Reason: "third party"},
					// DROPPED: outside this module.
					{File: "../../pkg/mod/pgx/conn.go", Line: 1, Callee: "", Reason: "third party"},
					// DROPPED: duplicate of the first site; the fixed point can
					// reach one site by several paths.
					{File: "cmd/x/dependencies.go", Line: 42, Callee: "newRepository", Reason: "function-typed field"},
				}},
			Truth: truthWith(map[string]PrivilegeSet{"never_seen": selectOnly}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var coordinator *IncompleteRoleSurface
	for i := range report.Incomplete {
		if report.Incomplete[i].Role == RoleCoordinator {
			coordinator = &report.Incomplete[i]
		}
	}
	if coordinator == nil {
		t.Fatal("no incompleteness record for the coordinator role")
	}
	// Pair-granular, and the "nobody has evidence" case must be marked as such --
	// that is the variant which used to vanish from every output.
	if len(coordinator.UndeclaredByEvidence) != 1 {
		t.Fatalf("posture pair with no derived evidence not enumerated: %v", coordinator.UndeclaredByEvidence)
	}
	gap := coordinator.UndeclaredByEvidence[0]
	if gap.Table != "never_seen" || gap.Privilege != PrivSelect {
		t.Errorf("gap = %+v, want never_seen SELECT", gap)
	}
	if len(gap.OtherRolesWithEvidence) != 0 {
		t.Errorf("no role has evidence for never_seen, so OtherRolesWithEvidence must be empty: %+v", gap)
	}
	if !strings.Contains(gap.String(), "NO role has evidence") {
		t.Errorf("the louder case must say so in its rendering: %q", gap.String())
	}
	// Exactly the two signal-bearing hops, deduplicated, with both third-party
	// kinds filtered out.
	if len(coordinator.WiringHops) != 2 {
		t.Fatalf("expected 2 signal hops (function-typed field call + in-module interface dispatch), "+
			"with third-party sinks, out-of-module files and the duplicate site filtered: %v",
			coordinator.WiringHops)
	}
	if coordinator.WiringHops[0].File != "cmd/x/dependencies.go" ||
		coordinator.WiringHops[0].Callee != "newRepository" {
		t.Errorf("first hop should be the function-typed field call, kept because a BARE callee name "+
			"cannot be a third-party method: %+v", coordinator.WiringHops[0])
	}
	if !strings.Contains(coordinator.WiringHops[1].Callee, "internal/x.Thing") {
		t.Errorf("second hop should be the in-module interface dispatch: %+v", coordinator.WiringHops[1])
	}
}

// firstNHops caps the printed hop list. The COUNT is the load-bearing number --
// it is what tells a reader how much of the surface is unverified -- so the cap
// applies only to the detail lines, never to the count above them.
func firstNHops(hops []UnresolvedCallSite, n int) []UnresolvedCallSite {
	if len(hops) <= n {
		return hops
	}
	return hops[:n]
}

// TestKnownOpenAllowlistFailsOnStaleAndUnticketedEntries keeps the known-open
// list from becoming a suppression list. Both failure modes are pinned: an entry
// whose defect no longer reproduces, and a CRITICAL that is not on the list.
func TestKnownOpenAllowlistFailsOnStaleAndUnticketedEntries(t *testing.T) {
	// The real list against NO findings: every entry must be reported stale,
	// because "the gate found nothing" means those defects are gone and the
	// entries are now blind spots for nothing.
	blocking, accepted, stale, _ := PartitionKnownOpen(nil)
	if len(blocking) != 0 || len(accepted) != 0 {
		t.Fatalf("no findings should mean no blocking and no accepted, got %d/%d", len(blocking), len(accepted))
	}
	if len(stale) != len(knownOpenCriticals) {
		t.Fatalf("expected all %d known-open entries reported stale against an empty finding set, got %d",
			len(knownOpenCriticals), len(stale))
	}

	// A CRITICAL for a DIFFERENT privilege on an allowlisted table must still
	// block. Matching by table alone would silently absorb new defects on any
	// table that ever had one.
	blocking, _, _, _ = PartitionKnownOpen([]Finding{{
		Severity: Critical, Role: RoleCoordinator,
		Table: "sync_dispatch_outbox", Privilege: PrivDelete,
		Summary: "synthetic DELETE finding",
	}})
	if len(blocking) != 1 {
		t.Fatalf("a DELETE finding on an INSERT-allowlisted table must still block the gate, got %d blocking",
			len(blocking))
	}

	// Same table and privilege but the OTHER role must also still block:
	// attribution is the whole point, so the role is part of the identity.
	blocking, _, _, _ = PartitionKnownOpen([]Finding{{
		Severity: Critical, Role: RoleDomain,
		Table: "sync_dispatch_outbox", Privilege: PrivInsert,
		Summary: "synthetic domain finding",
	}})
	if len(blocking) != 1 {
		t.Fatalf("the same (table, privilege) for a DIFFERENT role must still block, got %d blocking", len(blocking))
	}

	for _, entry := range knownOpenCriticals {
		if strings.TrimSpace(entry.Ticket) == "" {
			t.Errorf("known-open entry %s/%s %s has no ticket", entry.Role, entry.Table, entry.Privilege)
		}
		if strings.TrimSpace(entry.Why) == "" {
			t.Errorf("known-open entry %s/%s %s has no justification", entry.Role, entry.Table, entry.Privilege)
		}
	}
}
