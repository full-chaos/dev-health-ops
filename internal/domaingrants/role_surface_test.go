package domaingrants

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"testing"
)

// TestRoleGrantSurfacesMatchQuerySurfaces is the CI gate for the COORDINATOR
// half of CHAOS-3033's grant derivation, and the role-attribution half for both
// roles.
//
// It derives, separately per pool, which (table, privilege) pairs the code
// reachable through that pool actually needs, and checks each against that
// role's own declared posture -- DomainPosture() and CoordinatorPosture() in
// internal/storage/postgres/domain_authorization.go.
//
// Why a second gate rather than an extension of
// TestDomainGrantSurfaceMatchesQuerySurface: that test's job is the DOMAIN
// role's two-hand-maintained-lists problem (runtimeGrantStatements vs
// DomainPosture, which agreed with each other and drifted from the code). The
// coordinator has only one list, so its whole failure surface is different, and
// the interesting new property -- attribution BETWEEN roles -- is not a property
// of either role alone. Both gates run; neither replaces the other.
//
// Read the CRITICAL/ADVISORY split in roles.go's header before acting on output.
// The short version: a CRITICAL means real code on that pool executes something
// the posture forbids. An ADVISORY means this tool cannot attribute the pair per
// role, and a hand derivation from the construction site is more precise --
// notably NOT a licence to widen a posture.
func TestRoleGrantSurfacesMatchQuerySurfaces(t *testing.T) {
	root := findModuleRoot(t)

	inputs := make([]RoleInput, 0, len(AllPoolRoles))
	for _, role := range AllPoolRoles {
		derived, err := DeriveForRole(root, role)
		if err != nil {
			t.Fatalf("DeriveForRole(%s): %v", role, err)
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

	for _, line := range report.Stats {
		t.Log(line)
	}

	// The derived shared set, printed so it is reviewable. It makes the dual-grant
	// question checkable rather than purely asserted, but it is NOT authoritative
	// on its own and does not replace the role-partition manifest's whitelist --
	// see RoleReport.SharedPairs for the three reasons (pair-level vs table-level,
	// dual-constructed-type artifacts, and inherited blind spots).
	t.Logf("derived shared pairs -- proven call sites on more than one pool (%d). Reviewable signal, not authoritative; see RoleReport.SharedPairs:", len(report.SharedPairs))
	for _, pair := range report.SharedPairs {
		t.Logf("    %s", pair)
	}

	// Enumerated incompleteness. This block is the reason the gate can be
	// trusted: it states what was NOT verified, so a table this tool cannot see
	// never looks the same as a table it checked and approved.
	for _, gap := range report.Incomplete {
		t.Logf("INCOMPLETENESS for role %s -- the gate proves NOTHING about the following, and silence here is not confirmation:", gap.Role)
		t.Logf("    posture tables with no derived call site (%d): %s",
			len(gap.UndeclaredByEvidence), strings.Join(gap.UndeclaredByEvidence, ", "))
		t.Logf("    in-module wiring hops where taint stopped (%d, first 25 shown):", len(gap.WiringHops))
		for _, hop := range firstNHops(gap.WiringHops, 25) {
			callee := hop.Callee
			if callee == "" {
				callee = "<function value>"
			}
			t.Logf("        %s:%d callee=%s", hop.File, hop.Line, callee)
		}
		t.Logf("    non-constant SQL sites (%d):", len(gap.DynamicSQL))
		for _, site := range gap.DynamicSQL {
			t.Logf("        %s:%d %s", site.File, site.Line, site.Reason)
		}
	}

	var critical, advisory []Finding
	for _, f := range report.Findings {
		if f.Severity == Critical {
			critical = append(critical, f)
		} else {
			advisory = append(advisory, f)
		}
	}
	for _, f := range advisory {
		t.Logf("[ADVISORY] %s", f.Summary)
	}

	blocking, accepted, stale, unticketed := PartitionKnownOpen(critical)
	for _, f := range accepted {
		t.Logf("[KNOWN-OPEN CRITICAL, ticketed] %s", f.Summary)
	}
	// A stale entry means the defect was fixed and the allowlist is now carrying
	// a blind spot for nothing. Failing here is what stops this list from
	// becoming permanent suppression.
	for _, entry := range stale {
		t.Errorf("STALE known-open entry: %s/%s %s (%s) no longer reproduces. "+
			"The fix has landed -- DELETE this entry from knownOpenCriticals. "+
			"Leaving it there means the gate would not fail if the defect came back",
			entry.Role, entry.Table, entry.Privilege, entry.Ticket)
	}
	for _, entry := range unticketed {
		t.Errorf("known-open entry %s/%s %s has no ticket; every accepted-open finding must name one",
			entry.Role, entry.Table, entry.Privilege)
	}
	if len(blocking) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\n%d CRITICAL role-grant disagreement(s) between a derived per-pool query surface and that role's declared posture:\n\n", len(blocking))
	for i, f := range blocking {
		fmt.Fprintf(&b, "%d) [%s] %s\n", i+1, f.Role, f.Summary)
		for _, e := range f.Evidence {
			fmt.Fprintf(&b, "     evidence: %s:%d  %s\n", e.File, e.Line, e.Statement)
		}
	}
	fmt.Fprintf(&b, "\nFix: add the row to the OWNING role's posture in "+
		"internal/storage/postgres/domain_authorization.go (domainPosture or coordinatorPosture). "+
		"The coordinator's GRANT statements are projected from CoordinatorPosture() by "+
		"cmd/dev-health-worker-migrate, so the posture is the only edit for that role; the domain role "+
		"additionally needs the matching runtimeGrantStatements entry in internal/storage/river/migrate.go "+
		"in the SAME commit, or CheckDomainAuthorization fails closed for every domain worker.\n\n"+
		"Before widening anything, check whether the finding names SHARED evidence sites: those are an "+
		"artifact of this tool's (type, field) taint granularity, not a missing grant. See roles.go's header.\n")
	t.Fatal(b.String())
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

// TestLockTableRequiresAnyWritePrivilege pins the LOCK TABLE rule per role.
//
// This test exists because mutation testing found the rule was NOT covered:
// deleting the whole check left the repo-wide gate green, since every table this
// repo LOCKs already holds a write privilege for some other reason. A rule that
// cannot fail is not a rule, and this one has already shipped a production 42501
// (CHAOS-3113: workerctl's route rollback runs LOCK worker_job_outbox IN SHARE
// ROW EXCLUSIVE MODE while the domain role holds only SELECT+INSERT).
//
// Postgres wants at least ONE of INSERT/UPDATE/DELETE for a LOCK mode stricter
// than ROW EXCLUSIVE -- any one, not a specific one. That is a DIFFERENT shape
// from `SELECT ... FOR UPDATE`, which requires UPDATE specifically and is
// modelled as an ordinary UPDATE requirement. Both halves are asserted so the
// asymmetry cannot be "simplified" into a single rule later.
func TestLockTableRequiresAnyWritePrivilege(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)
	var selectInsert PrivilegeSet
	selectInsert.add(PrivSelect)
	selectInsert.add(PrivInsert)

	lockSurface := func(role PoolRole) *DerivedSurface {
		s := &TableSurface{Table: "outbox"}
		s.Privileges.add(PrivSelect)
		s.Evidence = append(s.Evidence, Evidence{
			File: "internal/jobroute/control.go", Line: 197, Privilege: PrivSelect,
			Statement: "LOCK TABLE public.outbox IN SHARE ROW EXCLUSIVE MODE", TxGroup: "tx",
		})
		s.RequiresAnyWriteLock = true
		s.WriteLockEvidence = append(s.WriteLockEvidence, Evidence{
			File: "internal/jobroute/control.go", Line: 197,
			Statement: "LOCK TABLE public.outbox IN SHARE ROW EXCLUSIVE MODE",
		})
		return &DerivedSurface{Role: role, Tables: map[string]*TableSurface{"outbox": s}}
	}

	// SELECT-only posture: the LOCK is denied at runtime. Must be CRITICAL.
	report, err := CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth: truthWith(map[string]PrivilegeSet{"unrelated": selectOnly})},
		{Role: RoleCoordinator, Derived: lockSurface(RoleCoordinator),
			Truth: truthWith(map[string]PrivilegeSet{"outbox": selectOnly})},
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(findingSummaries(report, Critical), "\n")
	if !strings.Contains(joined, "LOCK TABLE") {
		t.Fatalf("a LOCK TABLE against a SELECT-only posture was not reported CRITICAL -- that is the "+
			"CHAOS-3113 defect shape exactly, and it must fail closed:\n%s", joined)
	}

	// SELECT+INSERT posture: any ONE write privilege satisfies Postgres, so there
	// must be no finding. This half is what stops the rule degenerating into
	// "report every table that is ever LOCKed".
	report, err = CompareRoles([]RoleInput{
		{Role: RoleDomain, Derived: &DerivedSurface{Role: RoleDomain, Tables: map[string]*TableSurface{}},
			Truth: truthWith(map[string]PrivilegeSet{"unrelated": selectOnly})},
		{Role: RoleCoordinator, Derived: lockSurface(RoleCoordinator),
			Truth: truthWith(map[string]PrivilegeSet{"outbox": selectInsert})},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range report.Findings {
		if f.Severity == Critical && strings.Contains(f.Summary, "LOCK TABLE") {
			t.Errorf("INSERT alone should satisfy the LOCK requirement (Postgres accepts any one of "+
				"INSERT/UPDATE/DELETE), but it was still reported: %s", f.Summary)
		}
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
	joined := strings.Join(findingSummaries(report, Critical), "\n")
	if !strings.Contains(joined, "TRANSACTION STRADDLES THE ROLE PARTITION") {
		t.Fatalf("straddling transaction was not reported CRITICAL:\n%s", joined)
	}
	if !strings.Contains(joined, "PROVABLY share one transaction") {
		t.Errorf("straddle finding did not distinguish a traced tx origin from the coarse fallback: %s", joined)
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
					// KEPT: an unnamed function value -- the shape that hid the
					// whole scheduler surface before funcvalue.go.
					{File: "cmd/x/dependencies.go", Line: 42, Callee: "", Reason: "function-typed field"},
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
					{File: "cmd/x/dependencies.go", Line: 42, Callee: "", Reason: "function-typed field"},
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
	if len(coordinator.UndeclaredByEvidence) != 1 || coordinator.UndeclaredByEvidence[0] != "never_seen" {
		t.Errorf("posture table with no derived evidence not enumerated: %v", coordinator.UndeclaredByEvidence)
	}
	// Exactly the two signal-bearing hops, deduplicated, with both third-party
	// kinds filtered out.
	if len(coordinator.WiringHops) != 2 {
		t.Fatalf("expected 2 signal hops (unnamed function value + in-module interface dispatch), "+
			"with third-party sinks, out-of-module files and the duplicate site filtered: %v",
			coordinator.WiringHops)
	}
	if coordinator.WiringHops[0].File != "cmd/x/dependencies.go" || coordinator.WiringHops[0].Callee != "" {
		t.Errorf("first hop should be the unnamed function value: %+v", coordinator.WiringHops[0])
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
