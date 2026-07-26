package domaingrants

import (
	"fmt"
	"sort"
	"strings"
)

// This file is the multi-role half of the checker: Option B of CHAOS-3033 split
// one domain role into domain + coordinator, and a two-role posture has a
// failure mode one role does not -- a privilege granted to the WRONG role. Every
// check that asks "is this granted somewhere" passes while least-privilege is
// silently gone. So the comparison has to be per-role, and it has to say
// something about the other role too.
//
// What this file can and cannot prove, stated up front because overstating it
// would license exactly the hand-written rows the gate exists to adjudicate:
//
//   - PROVABLE, and reported CRITICAL: real code on role R's pool executes a
//     statement R's posture does not authorize, where at least one proving call
//     site is reachable ONLY through R's pool. Under-approximation cuts the safe
//     way here -- if the analyzer sees the call site, the call site is real.
//   - NOT PROVABLE, reported ADVISORY: a (table, privilege) missing from a
//     posture whose every proving site is SHARED with another role. See the
//     granularity note below -- this is the one place a finding can be wrong in
//     the WIDENING direction, so it must not be a CRITICAL.
//   - NOT PROVABLE HERE, reported ADVISORY: "role B must not hold X." The
//     derivation is a sound-but-INCOMPLETE under-approximation, so "B has no
//     evidence for X" never implies "B does not need X" -- it may simply be a
//     path this analyzer cannot see (see IncompleteRoleSurface). Asserting the
//     negative direction statically would fire on every blind-spot table.
//     The negative direction is proven at RUNTIME instead, by the union of each
//     role calling CheckRolePosture against its own manifest: that query's
//     "everything outside my own manifest is illegal for me to hold" catch-all
//     inspects the CALLING role, so checking every role covers both leak
//     directions. See CheckRolePosture's doc comment in
//     internal/storage/postgres/domain_authorization.go.
//
// The one over-approximation that DOES bite, and why a finding here can be
// wrong in the widening direction: taint is tracked per (type, field), not per
// construction site. A repository type constructed from BOTH pools has its
// whole method surface attributed to both roles. internal/jobs/metrics/remaining
// is the live instance -- PostgresStore.pool is fed the coordinator pool by the
// scheduler's fixed producers, so CancelRun/CompletePartition/FinalizeRun get
// attributed to the coordinator even though only the domain worker calls them.
// That is why a pair whose every evidence site is SHARED with the other role is
// reported ADVISORY, never CRITICAL: for those, a careful hand derivation is
// strictly more precise than this tool, and coordinatorPosture()'s
// remaining_metric_runs/{true,false,false} is correctly NARROWER than what this
// tool derives. Do not widen a posture on a shared-evidence finding.

// KnownOpenCritical is one CRITICAL finding that is ACCEPTED AS REAL, has a
// ticket, and is being fixed by a lane that does not own this checker.
//
// This is not a suppression list and must not be used as one. It exists because
// the checker's first run found a genuine latent 42501 whose fix is a posture edit
// in another lane.
//
// # ENFORCEMENT IS SUSPENDED under the advisory posture
//
// The coordinator check REPORTS and gates nothing (see AdvisoryReport), so none of
// the three properties below currently FAIL anything -- they are reported instead.
// That suspension is a consequence of the advisory decision, and it is written
// here deliberately rather than left to be inferred from the absence of a
// consumer: a comment claiming enforcement that no longer happens is worse than no
// comment, because a reader trusts it. RESTORING these three is part of promoting
// the check to a gate, tracked in CHAOS-3164.
//
// The three properties, all currently REPORTED rather than enforced:
//
//  1. An entry matches EXACTLY one (role, table, privilege), so a different
//     privilege on the same table is not absorbed by it.
//  2. An entry that STOPS reproducing is flagged stale, so it cannot outlive the
//     fix -- which is what turns allowlists into permanent blindness.
//  3. Every entry names a ticket.
//
// Emptying this list is the goal state, not a special case.
type KnownOpenCritical struct {
	Role      PoolRole
	Table     string
	Privilege Privilege
	Ticket    string
	Why       string
}

// knownOpenCriticals is the accepted-and-ticketed set. See KnownOpenCritical.
var knownOpenCriticals = []KnownOpenCritical{
	{
		Role:      RoleCoordinator,
		Table:     "sync_dispatch_outbox",
		Privilege: PrivInsert,
		Ticket:    "CHAOS-3079",
		Why: "syncreconciler.Materializer.Step runs on the coordinator pool " +
			"(cmd/dev-health-reconciler/dependencies.go:208 NewMaterializer(coordinatorPool)) and executes four " +
			"INSERT INTO public.sync_dispatch_outbox (materializer.go:125/235/345/450), but coordinatorPosture() " +
			"declares {\"sync_dispatch_outbox\", false, true, false} -- allow_insert=false. Latent only because " +
			"checkedInReconcilerActivation.syncMutation is false today; a 42501 the moment that flag flips. " +
			"This gate's first run found it, independently confirming the CUT-06 lane's hand-derived row. " +
			"The fix is a coordinatorPosture edit, which belongs to the posture lane, not to this checker.",
	},
}

// matches reports whether f is this known-open entry.
func (k KnownOpenCritical) matches(f Finding) bool {
	return f.Role == k.Role && f.Table == k.Table && f.Privilege == k.Privilege
}

// PartitionKnownOpen splits criticals into the ones already accepted-and-ticketed
// and the ones that would block, and separately reports allowlist entries that no
// longer reproduce.
//
// Under the advisory posture NOTHING here fails; the four results are reported by
// AdvisoryReport. A stale entry still MATTERS -- it means the fix landed and the
// list is now carrying an exemption for a defect that no longer exists, which is
// how a list like this rots into permanent suppression -- but saying so is
// currently all this does. Enforcement returns with CHAOS-3164.
func PartitionKnownOpen(criticals []Finding) (blocking []Finding, accepted []Finding, stale []KnownOpenCritical, unticketed []KnownOpenCritical) {
	seen := make([]bool, len(knownOpenCriticals))
	for _, f := range criticals {
		matchedAny := false
		for i, known := range knownOpenCriticals {
			if known.matches(f) {
				seen[i] = true
				matchedAny = true
				break
			}
		}
		if matchedAny {
			accepted = append(accepted, f)
			continue
		}
		blocking = append(blocking, f)
	}
	for i, known := range knownOpenCriticals {
		if strings.TrimSpace(known.Ticket) == "" {
			unticketed = append(unticketed, known)
		}
		if !seen[i] {
			stale = append(stale, known)
		}
	}
	return blocking, accepted, stale, unticketed
}

// AcknowledgedBlindSpot is one posture-declared (role, table, privilege) that
// this derivation cannot see, reviewed and accepted with a reason.
//
// It records which evidence-free posture rows a human has actually looked at, and
// why each is safe to accept. Same three properties as knownOpenCriticals: exact
// tuple match, a mandatory reason, and a stale entry flagged so the list cannot
// outlive the blind spot it excuses.
//
// ENFORCEMENT IS SUSPENDED under the advisory posture -- an unacknowledged row is
// REPORTED, not failed. The distinction still earns its keep as documentation:
// an acknowledged row carries a traceable reason, an unacknowledged one is a row
// nobody has examined, and the report says which is which. Enforcement returns
// with CHAOS-3164.
//
// Known limitation carried into that ticket: acknowledgements here are keyed by
// (role, table, privilege), but the DYNAMIC-SQL equivalent is keyed by FILE, so a
// new dynamic statement in an already-acknowledged file is accepted without
// review. Per-site acceptance is part of the partition work.
type AcknowledgedBlindSpot struct {
	Role      PoolRole
	Table     string
	Privilege Privilege
	Why       string
}

// AcknowledgedDynamicSQL is one SQL-execution site whose statement text is not a
// compile-time constant, reviewed and accepted.
//
// These must be gated, not merely logged. PartitionBlindSpots only inspects
// privileges ALREADY in a posture, so a runtime-built query that starts writing a
// table absent from every posture produces no derived pair AND no posture-gap
// tuple -- nothing anywhere reports it, and the gate stays green until a 42501.
// The set is small and slow-changing, so acknowledging each one by site is
// tractable; a NEW dynamic-SQL site fails until someone looks at it.
type AcknowledgedDynamicSQL struct {
	File string
	Why  string
}

// acknowledgedDynamicSQL is the reviewed set, keyed by file because line numbers
// move for unrelated reasons and a churning acknowledgement gets ignored.
var acknowledgedDynamicSQL = []AcknowledgedDynamicSQL{
	{
		File: "internal/providerfoundation/repository_postgres.go",
		Why: "builds its integration_credentials query by runtime string concatenation " +
			"(`query := literal; if ... { query += ... }`). Hand-traced in " +
			"grant-surface-derivation.md: the reachable variants read integration_credentials " +
			"only, which the domain posture already grants SELECT on.",
	},
	{
		File: "internal/scheduler/sync/transaction.go",
		Why: "assembles the handoff statement from named constants chosen by ownership policy. " +
			"Both variants target scheduled_jobs/scheduled_sync_occurrences/sync_configurations, " +
			"which the coordinator posture already declares (and which the non-dynamic sites in " +
			"the same package independently prove).",
	},
	{
		File: "internal/syncroute/control.go",
		Why: "builds a route-record read whose column list varies. Its table, " +
			"sync_dispatch_transport_routes, is proven by the constant-SQL sites in the same " +
			"controller, so the dynamic variant adds no new table.",
	},
}

// PartitionDynamicSQL splits dynamic-SQL sites into acknowledged and not, and
// reports acknowledgements that no longer correspond to a real site. Same
// self-cleaning discipline as the other two lists.
func PartitionDynamicSQL(incomplete []IncompleteRoleSurface) (unacknowledged []string, stale []AcknowledgedDynamicSQL, unreasoned []AcknowledgedDynamicSQL) {
	seen := make([]bool, len(acknowledgedDynamicSQL))
	for _, surface := range incomplete {
		for _, site := range surface.DynamicSQL {
			matched := false
			for i, ack := range acknowledgedDynamicSQL {
				if ack.File == site.File {
					seen[i] = true
					matched = true
					break
				}
			}
			if !matched {
				unacknowledged = append(unacknowledged,
					fmt.Sprintf("%s: %s:%d %s", surface.Role, site.File, site.Line, site.Reason))
			}
		}
	}
	for i, ack := range acknowledgedDynamicSQL {
		if strings.TrimSpace(ack.Why) == "" {
			unreasoned = append(unreasoned, ack)
		}
		if !seen[i] {
			stale = append(stale, ack)
		}
	}
	sort.Strings(unacknowledged)
	return unacknowledged, stale, unreasoned
}

// acknowledgedBlindSpots is the reviewed set. Every entry is a posture row whose
// justification lives OUTSIDE this tool's reach; each says where.
var acknowledgedBlindSpots = []AcknowledgedBlindSpot{
	{
		Role: RoleDomain, Table: "integration_credentials", Privilege: PrivSelect,
		Why: "providerfoundation/repository_postgres.go:36 builds its query by runtime string " +
			"concatenation, so the SQL text is not a compile-time constant and no table can be " +
			"extracted. The grant is correct -- verified by hand in grant-surface-derivation.md -- " +
			"and it is the one dynamic-SQL site in the domain surface.",
	},
	{
		Role: RoleCoordinator, Table: "organizations", Privilege: PrivSelect,
		Why: "the coordinator side is scheduler/fixed/organizations.go's PostgresOrganizationLister, " +
			"which holds no pool: it receives a pgx.Tx PARAMETER through an OrganizationLister " +
			"interface, so taint does not reach it from a pool root. The role-partition manifest " +
			"records organizations as a verified dual-grant.",
	},
	{
		Role: RoleCoordinator, Table: "work_graph_execution_requests", Privilege: PrivSelect,
		Why: "added to coordinatorPosture by CHAOS-3114 for the fixed engine's work-graph producer. " +
			"The SQL lives in internal/jobs/workgraph, reached through a producer interface the " +
			"unique-implementer heuristic cannot resolve (see the Producer.Produce wiring hop).",
	},
	{
		Role: RoleCoordinator, Table: "work_graph_execution_requests", Privilege: PrivInsert,
		Why: "same producer path as the SELECT above; workgraph/publisher.go:37 does the INSERT.",
	},
}

func (a AcknowledgedBlindSpot) matches(role PoolRole, gap PostureGapWithoutEvidence) bool {
	return a.Role == role && a.Table == gap.Table && a.Privilege == gap.Privilege && !gap.ColumnScoped
}

// PartitionBlindSpots splits every role's evidence-free posture rows into the
// acknowledged and the unacknowledged, and reports acknowledgements that no
// longer correspond to a real gap.
//
// Unacknowledged rows FAIL the gate: each is a posture privilege nothing in the
// analysis justifies, and leaving it unreported is how a wrong row survives.
// Stale acknowledgements fail too -- the evidence arrived (or the row was
// removed), so the entry is now suppressing nothing and must go.
func PartitionBlindSpots(incomplete []IncompleteRoleSurface) (unacknowledged []string, stale []AcknowledgedBlindSpot, unreasoned []AcknowledgedBlindSpot) {
	seen := make([]bool, len(acknowledgedBlindSpots))
	for _, surface := range incomplete {
		for _, gap := range surface.UndeclaredByEvidence {
			matched := false
			for i, ack := range acknowledgedBlindSpots {
				if ack.matches(surface.Role, gap) {
					seen[i] = true
					matched = true
					break
				}
			}
			if matched {
				continue
			}
			// A pair another role DOES have evidence for is already reported as a
			// misattribution ADVISORY with its own explanation; requiring a second
			// acknowledgement for it would be duplicate bookkeeping. The dangerous
			// case -- nobody has evidence -- is what must be acknowledged or fail.
			if len(gap.OtherRolesWithEvidence) > 0 || gap.ColumnScoped ||
				gap.ImpliedSelect || gap.JustifiedByLock != "" {
				continue
			}
			unacknowledged = append(unacknowledged,
				fmt.Sprintf("%s: %s", surface.Role, gap.String()))
		}
	}
	for i, ack := range acknowledgedBlindSpots {
		if strings.TrimSpace(ack.Why) == "" {
			unreasoned = append(unreasoned, ack)
		}
		if !seen[i] {
			stale = append(stale, ack)
		}
	}
	sort.Strings(unacknowledged)
	return unacknowledged, stale, unreasoned
}

// AdvisoryCategory names one kind of line the report can contain. Enumerated so a
// test can assert the report still covers every one -- a category silently
// dropped from the output is indistinguishable from a category with nothing to
// say, which is the failure mode this whole checker exists to avoid.
type AdvisoryCategory string

const (
	CategoryCritical         AdvisoryCategory = "CRITICAL"
	CategoryKnownOpen        AdvisoryCategory = "KNOWN-OPEN"
	CategoryAdvisory         AdvisoryCategory = "ADVISORY"
	CategorySharedPair       AdvisoryCategory = "SHARED-PAIR"
	CategoryNoEvidence       AdvisoryCategory = "POSTURE-ROW-WITHOUT-EVIDENCE"
	CategoryDynamicSQL       AdvisoryCategory = "DYNAMIC-SQL"
	CategoryUnparsedLock     AdvisoryCategory = "UNPARSED-LOCK"
	CategoryFuncValue        AdvisoryCategory = "UNRESOLVED-FUNCTION-VALUE"
	CategoryWiringHop        AdvisoryCategory = "WIRING-HOP"
	CategoryNameSeedOverride AdvisoryCategory = "NAME-SEED-OVERRIDE"
	CategoryUnresolvedTx     AdvisoryCategory = "UNRESOLVED-TX-ORIGIN"
	CategoryAcknowledgement  AdvisoryCategory = "ACKNOWLEDGEMENT"
	CategoryStats            AdvisoryCategory = "STATS"
)

// AllAdvisoryCategories is every category the report must be able to emit.
var AllAdvisoryCategories = []AdvisoryCategory{
	CategoryStats, CategoryCritical, CategoryKnownOpen, CategoryAdvisory,
	CategorySharedPair, CategoryNoEvidence, CategoryDynamicSQL, CategoryUnparsedLock,
	CategoryFuncValue, CategoryWiringHop, CategoryNameSeedOverride, CategoryUnresolvedTx,
	CategoryAcknowledgement,
}

// AdvisoryLine is one reportable statement, tagged with its category.
type AdvisoryLine struct {
	Category AdvisoryCategory
	Text     string
}

// AdvisoryReport returns everything this checker has to say about a report.
//
// # This is a REPORT, not a gate, and that is a deliberate decision
//
// The coordinator surface is advisory because the blind-spot partition does not
// exist yet. Three review rounds each found new places where THE ANALYSIS CANNOT
// SEE SOMETHING AND THE CHECK PASSES ANYWAY -- unresolved callees, unresolved
// interface dispatch, function-valued fields with no single target, dynamic SQL,
// unparsed statements, non-convergence, unrecognised lock forms, quoted
// identifiers. Each was fixed where it was found, and reappeared somewhere else.
// That is one defect with many addresses, and the addresses were being discovered
// one review at a time.
//
// An advisory tool that is sometimes wrong is useful: it puts derived evidence in
// front of a reviewer who can weigh it. A GATE that passes when the analysis is
// blind is worse than no gate, because it LICENSES the hand-written rows it was
// built to check -- which is exactly how this epic produced green tickets over
// dormant code in the first place.
//
// Promoting this to a gate requires the closure argument: a partition of
// everything the analysis can fail to see, with each cell either failing the
// check or documented as safe to accept PER SITE. Tracked as CHAOS-3164. Until
// that exists, a passing coordinator check is not evidence and must not be read
// as any.
//
// Returning tagged lines from ONE function is also what makes the reporting
// assertable. When each category was printed by its own loop in the test, a unit
// test could prove a value reached the data structure while the loop that
// reported it could be deleted with everything still green.
func AdvisoryReport(report *RoleReport) []AdvisoryLine {
	var lines []AdvisoryLine
	add := func(category AdvisoryCategory, text string) {
		lines = append(lines, AdvisoryLine{Category: category, Text: text})
	}

	for _, stat := range report.Stats {
		add(CategoryStats, stat)
	}

	critical, advisory := splitBySeverity(report.Findings)
	_, accepted, _, _ := PartitionKnownOpen(critical)
	acceptedSet := map[string]bool{}
	for _, f := range accepted {
		acceptedSet[f.Summary] = true
	}
	for _, f := range critical {
		if acceptedSet[f.Summary] {
			add(CategoryKnownOpen, "[ticketed, known open] "+f.Summary)
			continue
		}
		add(CategoryCritical, f.Summary)
	}
	for _, f := range advisory {
		add(CategoryAdvisory, f.Summary)
	}
	for _, pair := range report.SharedPairs {
		add(CategorySharedPair, pair)
	}

	for _, surface := range report.Incomplete {
		for _, gap := range surface.UndeclaredByEvidence {
			add(CategoryNoEvidence, fmt.Sprintf("role %s: %s", surface.Role, gap.String()))
		}
		for _, site := range surface.DynamicSQL {
			add(CategoryDynamicSQL, fmt.Sprintf(
				"role %s: %s:%d %s -- no table or privilege could be derived from this statement",
				surface.Role, site.File, site.Line, site.Reason))
		}
		for _, lock := range surface.UnparsedLocks {
			add(CategoryUnparsedLock, fmt.Sprintf(
				"role %s: %s:%d %q -- the privilege this LOCK demands was NOT derived, and its target "+
					"may not appear in the surface at all",
				surface.Role, lock.File, lock.Line, lock.Statement))
		}
		for _, conflict := range surface.FuncValueConflicts {
			add(CategoryFuncValue, fmt.Sprintf(
				"role %s: %s:%d %s -- %s. Pool-tainted arguments cross this call and NOTHING beyond "+
					"it was analyzed",
				surface.Role, conflict.File, conflict.Line, conflict.Field, conflict.Reason))
		}
		if len(surface.WiringHops) > 0 {
			add(CategoryWiringHop, fmt.Sprintf(
				"role %s: %d in-module call site(s) where pool taint STOPPED -- any SQL beyond them is "+
					"invisible to this analysis. First: %s",
				surface.Role, len(surface.WiringHops), describeHops(surface.WiringHops, 5)))
		}
		if len(surface.UnresolvedTx) > 0 {
			add(CategoryUnresolvedTx, fmt.Sprintf(
				"role %s: %d SQL site(s) run through a pgx.Tx parameter whose Begin() could NOT be "+
					"traced to a single origin, so their transaction grouping fell back to the coarse "+
					"function-scoped fallback -- any transaction-shaped reading of these is incomplete, "+
					"not a clean no-conflict signal. First: %s",
				surface.Role, len(surface.UnresolvedTx), describeTxSites(surface.UnresolvedTx, 5)))
		}
		for _, override := range surface.NameSeedOverrides {
			add(CategoryNameSeedOverride, fmt.Sprintf(
				"role %s: %s:%d parameter %q in %s is spelled for this role but its call sites pass %v "+
					"-- call-site evidence wins, and the parameter name is misleading",
				surface.Role, override.File, override.Line, override.Name, override.Function,
				override.ObservedRoles))
		}
	}

	// Acknowledgement bookkeeping. Under the advisory posture these no longer
	// fail anything, but they must still be SAID: an acknowledgement that has
	// stopped corresponding to a real gap is suppressing nothing and should be
	// deleted, and a gap nobody has acknowledged is one nobody has looked at.
	unacknowledged, staleAcks, unreasoned := PartitionBlindSpots(report.Incomplete)
	for _, entry := range unacknowledged {
		add(CategoryAcknowledgement, "UNACKNOWLEDGED posture row without evidence: "+entry+
			" -- either an over-declaration (remove the row) or a blind spot (add it to "+
			"acknowledgedBlindSpots with the reason and where the justification lives)")
	}
	for _, entry := range staleAcks {
		add(CategoryAcknowledgement, fmt.Sprintf(
			"STALE acknowledged blind spot: %s/%s %s no longer appears as an evidence-free posture "+
				"row -- delete it, or it will silently excuse a future one",
			entry.Role, entry.Table, entry.Privilege))
	}
	for _, entry := range unreasoned {
		add(CategoryAcknowledgement, fmt.Sprintf(
			"acknowledged blind spot %s/%s %s has no reason recorded",
			entry.Role, entry.Table, entry.Privilege))
	}
	unackDynamic, staleDynamic, unreasonedDynamic := PartitionDynamicSQL(report.Incomplete)
	for _, entry := range unackDynamic {
		add(CategoryAcknowledgement, "UNACKNOWLEDGED dynamic SQL: "+entry+
			" -- hand-trace which tables it can reach and record that, or make the SQL constant")
	}
	for _, entry := range staleDynamic {
		add(CategoryAcknowledgement, "STALE acknowledged dynamic SQL: "+entry.File+
			" no longer has a dynamic site -- delete it")
	}
	for _, entry := range unreasonedDynamic {
		add(CategoryAcknowledgement, "acknowledged dynamic SQL "+entry.File+" has no reasoning recorded")
	}

	return lines
}

func describeTxSites(sites []UnresolvedTxSite, n int) string {
	var parts []string
	for i, site := range sites {
		if i >= n {
			parts = append(parts, "...")
			break
		}
		parts = append(parts, fmt.Sprintf("%s:%d in %s", site.File, site.Line, site.Function))
	}
	return strings.Join(parts, "; ")
}

func describeHops(hops []UnresolvedCallSite, n int) string {
	var parts []string
	for i, hop := range hops {
		if i >= n {
			parts = append(parts, "...")
			break
		}
		callee := hop.Callee
		if callee == "" {
			callee = "<function value>"
		}
		parts = append(parts, fmt.Sprintf("%s:%d %s", hop.File, hop.Line, callee))
	}
	return strings.Join(parts, "; ")
}

func splitBySeverity(findings []Finding) (critical, advisory []Finding) {
	for _, f := range findings {
		if f.Severity == Critical {
			critical = append(critical, f)
		} else {
			advisory = append(advisory, f)
		}
	}
	return critical, advisory
}

// RoleInput is one role's derived surface paired with the posture it is
// checked against.
type RoleInput struct {
	Role    PoolRole
	Derived *DerivedSurface
	Truth   *GroundTruth
}

// IncompleteRoleSurface is the enumerated, first-class statement of what this
// role's derivation could NOT see. It exists because silence must never read as
// confirmation: a gate that reports nothing about a table it cannot analyze
// looks identical to a gate that verified it, and that is what licenses
// hand-written rows.
type IncompleteRoleSurface struct {
	Role PoolRole
	// WiringHops are calls that received a pool-tainted argument but whose
	// callee could not be resolved, restricted to this module's own code (the
	// pgx-internal ones are noise). Taint stops here, so everything downstream
	// is invisible.
	WiringHops []UnresolvedCallSite
	// DynamicSQL are SQL-shaped calls whose statement text is not a
	// compile-time constant, so no table could be extracted.
	DynamicSQL []DynamicSite
	// FuncValueConflicts are tainted calls through a function-typed field the
	// resolver deliberately refused (two implementations, or an unresolvable
	// value). These FAIL the gate: pool-tainted arguments cross them and nothing
	// beyond was analyzed, so they are unanalyzed surface, not a known limit.
	FuncValueConflicts []FuncValueConflictSite
	// UnparsedLocks are LOCK statements the parser could not understand or could
	// not represent faithfully (including quoted targets). The demand is real and
	// its target may not even appear in the derived surface.
	UnparsedLocks []UnparsedLockSite
	// NameSeedOverrides are places call-site evidence overruled a parameter's
	// spelling. Carried here so the advisory report has ONE source for everything
	// it prints.
	NameSeedOverrides []NameSeedOverride
	// UnresolvedTx are SQL sites issued through a pgx.Tx PARAMETER whose origin
	// could not be traced to a single Begin(). Their TxGroup fell back to the
	// coarse function-scoped grouping, so any transaction-shaped observation
	// involving them is incomplete rather than a clean "no conflict" signal.
	UnresolvedTx []UnresolvedTxSite
	// UndeclaredByEvidence are the (table, privilege) PAIRS this role's posture
	// declares that this role's derivation found no call site for. Each is either
	// a legitimate over-declaration or a path inside a blind spot -- this tool
	// cannot tell which, and says so rather than guessing.
	//
	// Pair granularity is load-bearing, not tidiness. A table-level list hides
	// the case that matters most: if SELECT stays visible while the UPDATE path
	// becomes unreachable, the table is nonempty, so a table-level list omits it
	// entirely and the (table, UPDATE) gap disappears from every output.
	UndeclaredByEvidence []PostureGapWithoutEvidence
}

// PostureGapWithoutEvidence is one posture-declared privilege with no derived
// call site for its own role.
type PostureGapWithoutEvidence struct {
	Table     string
	Privilege Privilege
	// ColumnScoped marks a column-scoped declaration rather than a table row.
	ColumnScoped bool
	// ImpliedSelect marks a SELECT that loadPosture synthesized onto a posture row
	// rather than the posture declaring it, on a table that HAS other derived
	// evidence. An artifact of the representation, not a blind spot.
	ImpliedSelect bool
	// JustifiedByLock names the LOCK mode that already demands this privilege, if
	// any. A lock's demand is a disjunction, so the per-privilege loop cannot
	// record it as evidence even though the lock fully justifies the row.
	JustifiedByLock string
	// OtherRolesWithEvidence names roles that DO have a proven call site for this
	// pair. Non-empty means "possible misattribution" (the privilege may belong
	// to that role instead). EMPTY is the more dangerous case and the one that
	// used to vanish: nothing anywhere justifies the row, so either it is an
	// over-declaration or it sits inside a blind spot -- and either way no other
	// output mentions it.
	OtherRolesWithEvidence []string
}

func (g PostureGapWithoutEvidence) String() string {
	suffix := ""
	if g.ColumnScoped {
		suffix = " (column-scoped)"
	}
	if g.ImpliedSelect {
		suffix += " (SELECT implied by the posture model; table has other evidence)"
	}
	if g.JustifiedByLock != "" {
		suffix += " (justified by LOCK IN " + g.JustifiedByLock + " MODE)"
	}
	if len(g.OtherRolesWithEvidence) > 0 {
		suffix += " [evidence exists for: " + strings.Join(g.OtherRolesWithEvidence, "+") + "]"
	} else {
		suffix += " [NO role has evidence]"
	}
	return g.Table + " " + g.Privilege.String() + suffix
}

// RoleReport is the multi-role comparison result.
type RoleReport struct {
	Findings   []Finding
	Incomplete []IncompleteRoleSurface
	// SharedPairs are the (table, privilege) pairs with proven call sites on more
	// than one pool. It is a REVIEWABLE SIGNAL, deliberately not a drop-in
	// replacement for the role-partition manifest's hand-maintained dual-grant
	// whitelist, and three differences matter before anyone treats it as one:
	//
	//  1. It is PAIR-level, the whitelist is TABLE-level. A table granted to both
	//     roles with DIFFERENT privileges (sync_dispatch_transport_routes: domain
	//     SELECT, coordinator UPDATE) is a shared TABLE but has no shared PAIR, so
	//     it does not appear here.
	//  2. It cannot distinguish a genuine dual-grant from a dual-constructed-type
	//     artifact. remaining_metric_runs/partitions appear here only because
	//     PostgresStore.pool is fed both pools, not because both roles run every
	//     statement -- see the granularity note in this file's header.
	//  3. It inherits the derivation's blind spots. `organizations` is a real
	//     dual-grant that does NOT appear here, because the coordinator side has no
	//     derived evidence (it is in IncompleteRoleSurface instead).
	//
	// So: useful for review, and it makes the shared set visible and checkable
	// rather than purely asserted. Not authoritative on its own.
	SharedPairs []string
	Stats       []string
}

// HasCritical reports whether any finding is Critical -- the CI-gate signal.
func (r *RoleReport) HasCritical() bool {
	for _, f := range r.Findings {
		if f.Severity == Critical {
			return true
		}
	}
	return false
}

// pairKey identifies one (table, privilege) requirement.
type pairKey struct {
	table string
	priv  Privilege
}

// evidenceSites returns the distinct "file:line" sites proving pair for a
// surface, so two roles' evidence can be compared for exclusivity.
func evidenceSites(surface *TableSurface, p Privilege) map[string]bool {
	out := map[string]bool{}
	if surface == nil {
		return out
	}
	for _, e := range surface.Evidence {
		if e.Privilege == p {
			out[fmt.Sprintf("%s:%d", e.File, e.Line)] = true
		}
	}
	return out
}

// lockEvidenceSites returns the distinct "file:line" LOCK TABLE sites for a
// surface, so two roles' lock evidence can be compared for exclusivity exactly
// the way per-privilege evidence is.
func lockEvidenceSites(surface *TableSurface) map[string]bool {
	out := map[string]bool{}
	if surface == nil {
		return out
	}
	for _, e := range surface.WriteLockEvidence {
		out[fmt.Sprintf("%s:%d", e.File, e.Line)] = true
	}
	return out
}

// rolesWithEvidence names the roles OTHER than exclude that have a proven call
// site for (table, p), sorted.
func rolesWithEvidence(byRole map[PoolRole]RoleInput, exclude PoolRole, table string, p Privilege) []string {
	var out []string
	for role, other := range byRole {
		if role == exclude {
			continue
		}
		if surface := other.Derived.Tables[table]; surface != nil && surface.Privileges.Has(p) {
			out = append(out, string(role))
		}
	}
	sort.Strings(out)
	return out
}

// txGroupSites returns the distinct "file:line" sites contributing evidence to
// one transaction group in a role's surface, so two roles' claim on the same
// group can be compared for exclusivity.
func txGroupSites(derived *DerivedSurface, group string) map[string]bool {
	out := map[string]bool{}
	if derived == nil {
		return out
	}
	for _, surface := range derived.Tables {
		for _, e := range surface.Evidence {
			if e.TxGroup == group {
				out[fmt.Sprintf("%s:%d", e.File, e.Line)] = true
			}
		}
	}
	return out
}

// exclusiveTo returns the sorted subset of mine that appears in NONE of the
// other roles' site sets. Shared by the per-privilege and LOCK paths so both get
// the same shared-evidence downgrade -- a second copy would let one path drift
// into emitting blocking findings the other correctly downgrades.
func exclusiveTo(mine map[string]bool, others []map[string]bool) []string {
	var exclusive []string
	for site := range mine {
		shared := false
		for _, otherSites := range others {
			if otherSites[site] {
				shared = true
				break
			}
		}
		if !shared {
			exclusive = append(exclusive, site)
		}
	}
	sort.Strings(exclusive)
	return exclusive
}

// privilegeSetNames renders a disjunction of privileges for a message.
func privilegeSetNames(set PrivilegeSet) string {
	var names []string
	for p := Privilege(0); p < numPrivileges; p++ {
		if set.Has(p) {
			names = append(names, p.String())
		}
	}
	return strings.Join(names, "/")
}

// CompareRoles cross-checks every role's derived surface against its own
// posture, and cross-checks the roles against each other. See this file's
// header for exactly which of those comparisons are proofs and which are
// advisories.
func CompareRoles(inputs []RoleInput) (*RoleReport, error) {
	if len(inputs) < 2 {
		return nil, fmt.Errorf("domaingrants: CompareRoles needs every role's surface, got %d; "+
			"a single-role run cannot check attribution at all and must not be mistaken for one that did", len(inputs))
	}
	report := &RoleReport{}

	byRole := map[PoolRole]RoleInput{}
	for _, in := range inputs {
		if in.Derived == nil || in.Truth == nil {
			return nil, fmt.Errorf("domaingrants: role %q has a nil surface or posture", in.Role)
		}
		byRole[in.Role] = in
	}

	// Which roles have PROVEN evidence for each pair. This is the derived
	// shared/exclusive classification everything below keys off.
	rolesForPair := map[pairKey]map[PoolRole]bool{}
	for _, in := range inputs {
		for table, surface := range in.Derived.Tables {
			for p := Privilege(0); p < numPrivileges; p++ {
				if !surface.Privileges.Has(p) {
					continue
				}
				key := pairKey{table: table, priv: p}
				if rolesForPair[key] == nil {
					rolesForPair[key] = map[PoolRole]bool{}
				}
				rolesForPair[key][in.Role] = true
			}
		}
	}

	for _, in := range inputs {
		report.Stats = append(report.Stats, fmt.Sprintf(
			"role %s: %d tables derived, %d posture tables, %d dynamic SQL sites, %d unresolved call sites, %d devirtualized, %d function-value hops resolved",
			in.Role, len(in.Derived.Tables), len(in.Truth.RequiredTablePrivileges),
			len(in.Derived.Dynamic), len(in.Derived.Unresolved),
			len(in.Derived.Devirtualized), len(in.Derived.FuncValueResolved),
		))
		report.Findings = append(report.Findings, compareOneRole(in, byRole, rolesForPair)...)
		report.Incomplete = append(report.Incomplete, incompleteFor(in, byRole))
	}

	report.Findings = append(report.Findings, transactionStraddleFindings(inputs, byRole)...)

	for key, roles := range rolesForPair {
		if len(roles) < 2 {
			continue
		}
		names := make([]string, 0, len(roles))
		for role := range roles {
			names = append(names, string(role))
		}
		sort.Strings(names)
		report.SharedPairs = append(report.SharedPairs,
			fmt.Sprintf("%s %s (evidence on: %s)", key.table, key.priv, strings.Join(names, "+")))
	}
	sort.Strings(report.SharedPairs)

	sort.SliceStable(report.Findings, func(i, j int) bool {
		if report.Findings[i].Severity != report.Findings[j].Severity {
			return report.Findings[i].Severity < report.Findings[j].Severity
		}
		if report.Findings[i].Table != report.Findings[j].Table {
			return report.Findings[i].Table < report.Findings[j].Table
		}
		return report.Findings[i].Privilege < report.Findings[j].Privilege
	})
	return report, nil
}

// compareOneRole is the per-role half: does this role's posture authorize what
// this role's code actually executes?
func compareOneRole(
	in RoleInput,
	byRole map[PoolRole]RoleInput,
	rolesForPair map[pairKey]map[PoolRole]bool,
) []Finding {
	var findings []Finding

	for _, table := range in.Derived.SortedTables() {
		surface := in.Derived.Tables[table]
		required := in.Truth.RequiredTablePrivileges[table]
		columnScoped := in.Truth.ColumnScopedTables[table]

		for p := Privilege(0); p < numPrivileges; p++ {
			if !surface.Privileges.Has(p) {
				continue
			}
			key := pairKey{table: table, priv: p}
			if columnScoped {
				if !required.Has(p) {
					findings = append(findings, Finding{
						Severity: Advisory, Table: table, Privilege: p, Role: in.Role,
						Summary: fmt.Sprintf(
							"role %s, table %q: derived surface requires %s and the table is column-scoped in this role's posture -- verify the column-level grant covers it (this tool does not model column grants)",
							in.Role, table, p),
						Evidence: evidenceFor(surface, p),
					})
				}
				continue
			}
			if required.Has(p) {
				continue
			}

			// The discriminator: is there at least one evidence site for this
			// pair that ONLY this role has? If yes, this role's own pool
			// provably executes it and the posture is wrong. If every site is
			// shared with another role, the pair is reached through a type fed
			// by more than one pool, and per-role attribution is beyond this
			// tool's (type, field) granularity -- advisory, because a hand
			// derivation is strictly more precise there. See the file header.
			others := make([]map[string]bool, 0, len(byRole))
			for role, other := range byRole {
				if role == in.Role {
					continue
				}
				others = append(others, evidenceSites(other.Derived.Tables[table], p))
			}
			exclusive := exclusiveTo(evidenceSites(surface, p), others)
			if len(exclusive) == 0 && len(rolesForPair[key]) > 1 {
				findings = append(findings, Finding{
					Severity: Advisory, Table: table, Privilege: p, Role: in.Role,
					Summary: fmt.Sprintf(
						"role %s, table %q: %s is derived but every proving call site is SHARED with another role's derivation, so it comes from a repository type constructed from more than one pool -- this tool's (type, field) taint granularity cannot attribute it per role. DO NOT widen %s's posture on this finding alone; a hand derivation from the construction site is more precise here",
						in.Role, table, p, in.Role),
					Evidence: evidenceFor(surface, p),
				})
				continue
			}
			findings = append(findings, Finding{
				Severity: Critical, Table: table, Privilege: p, Role: in.Role,
				Summary: fmt.Sprintf(
					"role %s, table %q needs %s, proven by %d call site(s) reachable ONLY through the %s pool (%s), but this role's posture does not authorize it -- a 42501 the moment that path runs",
					in.Role, table, p, len(exclusive), in.Role, strings.Join(firstN(exclusive, 3), ", ")),
				Evidence: evidenceFor(surface, p),
			})
		}

		// LOCK TABLE. Postgres's demand is a DISJUNCTION over a mode-specific set
		// (see LockRequirement in sql.go for the measured mapping), so this cannot
		// be folded into the per-privilege loop above -- and it is a different
		// shape again from the FOR UPDATE / FOR SHARE row-lock clauses, which
		// require UPDATE specifically and ARE handled above as ordinary UPDATE
		// requirements. Three distinct rules; do not collapse them.
		if surface.LockRequirement != nil && !columnScoped {
			requirement := *surface.LockRequirement
			if !lockSatisfiedBy(requirement, required) {
				// Route through the SAME shared-evidence downgrade the
				// per-privilege path uses. Without this the lock path could emit a
				// blocking "grant the coordinator too" for a table whose locking
				// method only the other role actually invokes -- exactly the
				// over-widening the downgrade exists to prevent, and the lock path
				// has no more attribution precision than any other.
				var otherLockSites []map[string]bool
				othersLock := false
				for role, other := range byRole {
					if role == in.Role {
						continue
					}
					sites := lockEvidenceSites(other.Derived.Tables[table])
					if len(sites) > 0 {
						othersLock = true
					}
					otherLockSites = append(otherLockSites, sites)
				}
				exclusive := exclusiveTo(lockEvidenceSites(surface), otherLockSites)

				severity := Critical
				attribution := fmt.Sprintf("proven at %d LOCK site(s) reachable ONLY through the %s pool (%s)",
					len(exclusive), in.Role, strings.Join(firstN(exclusive, 3), ", "))
				remedy := "a 42501 the moment that path runs"
				if len(exclusive) == 0 && othersLock {
					severity = Advisory
					attribution = "but every LOCK site is SHARED with another role's derivation, so the locking method is reached through a type constructed from more than one pool"
					remedy = fmt.Sprintf("this tool's (type, field) taint granularity cannot attribute it per role -- DO NOT widen %s's posture on this finding alone", in.Role)
				}
				findings = append(findings, Finding{
					Severity: severity, Table: table, Role: in.Role,
					Summary: fmt.Sprintf(
						"role %s, table %q: LOCK IN %s MODE requires at least ONE of %s -- a disjunction, NOT a conjunction with SELECT -- and this role's posture grants none of them; %s -- %s",
						in.Role, table, requirement.Mode, privilegeSetNames(requirement.Satisfying), attribution, remedy),
					Evidence: surface.WriteLockEvidence,
				})
			}
		}
	}

	// Possible MISATTRIBUTION: this role's posture declares a pair it has no
	// evidence for, while another role does have evidence. Advisory, not
	// critical: see the file header for why the negative direction is not
	// statically provable.
	for table, privs := range in.Truth.RequiredTablePrivileges {
		for p := Privilege(0); p < numPrivileges; p++ {
			if !privs.Has(p) {
				continue
			}
			surface := in.Derived.Tables[table]
			if surface != nil && surface.Privileges.Has(p) {
				continue
			}
			var others []string
			for role, other := range byRole {
				if role == in.Role {
					continue
				}
				if s := other.Derived.Tables[table]; s != nil && s.Privileges.Has(p) {
					others = append(others, string(role))
				}
			}
			if len(others) == 0 {
				// Nobody has evidence for this pair. NOT discarded: it is carried
				// by IncompleteRoleSurface.UndeclaredByEvidence with
				// OtherRolesWithEvidence empty, which is the louder of the two
				// cases and used to vanish from every output.
				continue
			}
			sort.Strings(others)
			findings = append(findings, Finding{
				Severity: Advisory, Table: table, Privilege: p, Role: in.Role,
				Summary: fmt.Sprintf(
					"role %s, table %q: posture authorizes %s but only role(s) %s have proven call sites for it -- POSSIBLE MISATTRIBUTION (granted to the wrong role, which every \"is it granted somewhere\" check passes) OR a path inside this role's enumerated blind spot. Resolve from the construction site, not from this list",
					in.Role, table, p, strings.Join(others, "+")),
			})
		}
	}
	return findings
}

// transactionStraddleFindings implements the transaction-span veto: a single
// transaction cannot span two connection pools, so every table a transaction
// touches must be authorized for the ONE role whose pool runs it. A transaction
// whose tables land on opposite sides of the partition either forces them into
// the same role or needs restructuring -- and it fails as a runtime 42501, which
// is what this check exists to catch first.
//
// TxGroup precision matters here and is not uniform: a "txorigin:" group was
// traced to a single unambiguous Begin() across function boundaries, while a
// package-qualified group is the coarse same-function-body fallback. Straddles
// found in a coarse group are still reported, flagged as such, because a
// possible straddle is worth a human's attention and the alternative is
// silence.
func transactionStraddleFindings(inputs []RoleInput, byRole map[PoolRole]RoleInput) []Finding {
	var findings []Finding
	for _, in := range inputs {
		groups := map[string]map[string]bool{}
		for table, surface := range in.Derived.Tables {
			for _, e := range surface.Evidence {
				if e.TxGroup == "" {
					continue
				}
				if groups[e.TxGroup] == nil {
					groups[e.TxGroup] = map[string]bool{}
				}
				groups[e.TxGroup][table] = true
			}
		}
		for _, group := range sortedKeys(boolMapKeys(groups)) {
			tables := sortedKeys(groups[group])
			if len(tables) < 2 {
				continue
			}
			var mine, elsewhere []string
			for _, table := range tables {
				if _, declared := in.Truth.RequiredTablePrivileges[table]; declared ||
					in.Truth.ColumnScopedTables[table] {
					mine = append(mine, table)
					continue
				}
				// Not in this role's posture at all. Is it in another's? That is
				// the straddle: one transaction, tables on both sides.
				var holders []string
				for role, other := range byRole {
					if role == in.Role {
						continue
					}
					if _, declared := other.Truth.RequiredTablePrivileges[table]; declared ||
						other.Truth.ColumnScopedTables[table] {
						holders = append(holders, string(role))
					}
				}
				if len(holders) > 0 {
					sort.Strings(holders)
					elsewhere = append(elsewhere, fmt.Sprintf("%s (declared for %s)", table, strings.Join(holders, "+")))
				} else {
					elsewhere = append(elsewhere, table+" (declared for NO role)")
				}
			}
			if len(elsewhere) == 0 {
				continue
			}

			// A straddle is only BLOCKING when both of its premises are proven:
			// that the statements really share one transaction, and that this
			// transaction really runs on THIS role's pool.
			//
			//  - co-residency: a "txorigin:" group was traced to one unambiguous
			//    Begin(); anything else is the coarse same-function-body fallback,
			//    which this package's own doc comment calls unproven.
			//  - attribution: the group's evidence must include at least one site
			//    the other role does not also have. Otherwise the group is reached
			//    through a type constructed from more than one pool, and the
			//    (type, field) taint granularity cannot say whose transaction it
			//    is -- the same limit the per-privilege and LOCK paths downgrade for.
			//
			// Emitting CRITICAL without both would tell maintainers to dual-grant on
			// the strength of a guess, which is precisely the over-widening this
			// checker refuses to do by hand.
			// DECISION: a straddle is never BLOCKING, because co-residency is
			// inferred rather than proven at either precision.
			//
			// A "txorigin:" group means every statement's pgx.Tx traced back to the
			// same Begin() SOURCE POSITION. That is a fact about where the handle
			// came from, not about what executes: buildTxOrigins does no
			// control-flow analysis, so two MUTUALLY EXCLUSIVE branches after one
			// Begin -- an if/else, an early return, a switch -- carry the same
			// origin and are reported as one transaction touching both sides of the
			// partition. Calling that "proven" was wrong, and a premise labelled
			// proven is worse than one labelled inferred because nobody re-checks it.
			//
			// Downgrading rather than doing the path analysis, for the same reason
			// the remaining_metric_* over-approximation is advisory: this finding
			// can only push a posture WIDER (dual-grant these tables), and an
			// over-grant emitted on an inference is exactly what the two-role split
			// exists to prevent. The blocking signal is not lost -- a table genuinely
			// missing from the executing role's posture still produces its own
			// per-privilege CRITICAL when its evidence is role-exclusive. What is
			// lost is only the transaction FRAMING, which is a review aid.
			//
			// The traced/coarse distinction is still reported, because it tells a
			// reader how much weight the grouping carries.
			traced := strings.HasPrefix(group, "txorigin:")
			groupSites := txGroupSites(in.Derived, group)
			var otherGroupSites []map[string]bool
			for role, other := range byRole {
				if role == in.Role {
					continue
				}
				otherGroupSites = append(otherGroupSites, txGroupSites(other.Derived, group))
			}
			// One exclusive site is necessary AND sufficient here. A group key only
			// exists because this role's own evidence produced it, so groupSites is
			// never empty; therefore "no other role has this group" already implies
			// every site is exclusive. An earlier version also OR'd in an explicit
			// !othersHaveGroup clause, which mutation testing showed to be dead --
			// unreachable rather than merely untested. Removed instead of left in
			// place, because a dead clause reads as a second safeguard that is
			// actually doing nothing.
			exclusive := exclusiveTo(groupSites, otherGroupSites)
			attributed := len(exclusive) > 0

			// ALWAYS Advisory. The two axes below describe how much the grouping is
			// worth, not whether it blocks.
			var coResidency string
			if traced {
				coResidency = "every statement's pgx.Tx traced to the same Begin() SOURCE POSITION -- " +
					"which is where the handle came from, NOT proof that these statements co-execute: " +
					"mutually exclusive branches after one Begin share this origin"
			} else {
				coResidency = "grouped only by the COARSE same-function-body fallback, so co-residency is a guess"
			}
			attribution := "at least one evidence site is reachable only through this role's pool"
			if !attributed {
				attribution = fmt.Sprintf("every evidence site is SHARED with another role, so attributing "+
					"the transaction to %s is beyond this tool's (type, field) granularity", in.Role)
			}
			findings = append(findings, Finding{
				Severity: Advisory, Table: strings.Join(tables, ", "), Role: in.Role,
				Summary: fmt.Sprintf(
					"POSSIBLE TRANSACTION STRADDLE (advisory -- co-residency is inferred, never proven): group %s "+
						"appears to run on the %s pool and touch %s, but %s not authorized for %s. Co-residency: %s. "+
						"Attribution: %s. A transaction cannot span two pools, so IF these really do co-execute the "+
						"options are dual-grant or restructure -- but confirm from the source before changing any "+
						"posture, because this finding can only widen one",
					group, in.Role, strings.Join(tables, "+"),
					strings.Join(elsewhere, "; "), in.Role, coResidency, attribution),
			})
		}
	}
	return findings
}

// incompleteFor enumerates what this role's derivation could not see.
//
// The filter matters as much as the list. Unresolved-callee records are dominated
// by two kinds that are NOT blind spots and would bury the ones that are:
//
//   - third-party callees (pgx's Tx.Exec/Row.Scan, valkey's Do). A tainted
//     argument reaching pgx.Tx.Exec is the SQL sink itself -- already extracted
//     by recordSQLSite. Reporting it as "taint stopped here" is backwards.
//   - files outside this module (the driver's own source, loaded as a dependency).
//
// What survives is what actually hides code: a call through a function-typed
// value the resolver could not pin down, and in-module interface dispatch with
// no unique implementer. Those are the shapes that hid the whole scheduler
// surface before funcvalue.go, so those are the ones worth a human's eyes.
// Deduplicated by site, because the fixed point can reach one site by several
// paths and a repeated line adds no information.
func incompleteFor(in RoleInput, byRole map[PoolRole]RoleInput) IncompleteRoleSurface {
	out := IncompleteRoleSurface{Role: in.Role}
	seen := map[string]bool{}
	for _, u := range in.Derived.Unresolved {
		if strings.HasPrefix(u.File, "..") || strings.Contains(u.File, "/pkg/mod/") {
			continue
		}
		// Keep only unnamed function values and this module's own unresolved
		// interface dispatch; everything else is a third-party sink, not a gap.
		inModule := strings.HasPrefix(u.Callee, "github.com/full-chaos/dev-health-ops") ||
			strings.HasPrefix(u.Callee, "(github.com/full-chaos/dev-health-ops")
		// A BARE name (no package path, no receiver parens) is a call through a
		// function-typed field or local -- in-module by construction, and the exact
		// shape that hid the scheduler surface. An earlier version of this filter
		// kept only empty or module-qualified callees, which dropped those records
		// because their callee is just the field name.
		bareName := u.Callee != "" && !strings.ContainsAny(u.Callee, ".(")
		if u.Callee != "" && !inModule && !bareName {
			continue
		}
		site := fmt.Sprintf("%s:%d", u.File, u.Line)
		if seen[site] {
			continue
		}
		seen[site] = true
		out.WiringHops = append(out.WiringHops, u)
	}
	out.DynamicSQL = in.Derived.Dynamic
	for _, conflict := range in.Derived.FuncValueConflicts {
		// In-module only. Third-party function-typed fields (encoding/json's
		// scanner.step, pgx's QueuedQuery.Fn) are refused for the same mechanical
		// reason but say nothing about this repo's wiring, and gating on them would
		// drown the signal this check exists for.
		if strings.HasPrefix(conflict.File, "..") || strings.Contains(conflict.File, "/pkg/mod/") {
			continue
		}
		out.FuncValueConflicts = append(out.FuncValueConflicts, conflict)
	}
	out.UnparsedLocks = in.Derived.UnparsedLocks
	out.NameSeedOverrides = in.Derived.NameSeedOverrides
	out.UnresolvedTx = in.Derived.UnresolvedTx

	// PAIR-granular, and computed against THIS role's own derivation. A
	// table-level check would miss the case that matters most: SELECT still
	// visible while the UPDATE path became unreachable leaves the table nonempty,
	// so the (table, UPDATE) gap would appear in no output at all.
	for table, privs := range in.Truth.RequiredTablePrivileges {
		for p := Privilege(0); p < numPrivileges; p++ {
			if !privs.Has(p) {
				continue
			}
			surface := in.Derived.Tables[table]
			if surface != nil && surface.Privileges.Has(p) {
				continue
			}
			gap := PostureGapWithoutEvidence{
				Table: table, Privilege: p,
				OtherRolesWithEvidence: rolesWithEvidence(byRole, in.Role, table, p),
			}
			// SELECT is SYNTHESIZED onto every posture row by loadPosture, not
			// declared, so a table the code only writes will always lack derived
			// SELECT evidence. That is the model implying a privilege, not a blind
			// spot, and gating it would demand acknowledgements for an artifact of
			// our own representation. A table with NO evidence at all still counts.
			if p == PrivSelect && surface != nil && !surface.Privileges.Empty() {
				gap.ImpliedSelect = true
			}
			// A privilege that SATISFIES a derived LOCK is justified by that lock,
			// even though the per-privilege loop cannot record it (the demand is a
			// disjunction). coordinator worker_job_outbox UPDATE is exactly this: it
			// exists for jobroute's SHARE ROW EXCLUSIVE lock.
			//
			// But ONLY when it is the SOLE satisfying privilege the posture holds. A
			// disjunction needs exactly one member; if the posture grants two, one of
			// them is redundant. Excusing every satisfying privilege -- as an earlier
			// version did -- meant a posture holding both UPDATE and DELETE had BOTH
			// evidence-free gaps waved through, preserving a redundant over-grant
			// that nothing else would ever report.
			if surface != nil && surface.LockRequirement != nil &&
				surface.LockRequirement.Satisfying.Has(p) {
				held := 0
				for q := Privilege(0); q < numPrivileges; q++ {
					if surface.LockRequirement.Satisfying.Has(q) && privs.Has(q) {
						held++
					}
				}
				if held == 1 {
					gap.JustifiedByLock = surface.LockRequirement.Mode
				}
			}
			out.UndeclaredByEvidence = append(out.UndeclaredByEvidence, gap)
		}
	}
	for table := range in.Truth.ColumnScopedTables {
		if surface := in.Derived.Tables[table]; surface == nil || surface.Privileges.Empty() {
			out.UndeclaredByEvidence = append(out.UndeclaredByEvidence, PostureGapWithoutEvidence{
				Table: table, ColumnScoped: true,
			})
		}
	}
	sort.Slice(out.UndeclaredByEvidence, func(i, j int) bool {
		if out.UndeclaredByEvidence[i].Table != out.UndeclaredByEvidence[j].Table {
			return out.UndeclaredByEvidence[i].Table < out.UndeclaredByEvidence[j].Table
		}
		return out.UndeclaredByEvidence[i].Privilege < out.UndeclaredByEvidence[j].Privilege
	})
	sort.SliceStable(out.WiringHops, func(i, j int) bool {
		if out.WiringHops[i].File != out.WiringHops[j].File {
			return out.WiringHops[i].File < out.WiringHops[j].File
		}
		return out.WiringHops[i].Line < out.WiringHops[j].Line
	})
	return out
}
