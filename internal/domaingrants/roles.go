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
//     statement R's posture does not authorize. Under-approximation cuts the
//     other way here -- if the analyzer sees the call site, the call site is
//     real.
//   - PROVABLE, and reported CRITICAL: a (table, privilege) with evidence on
//     BOTH pools that is missing from EITHER posture. This is the derived
//     dual-grant check, and it replaces a hand-maintained shared-table
//     whitelist with a computed property of the evidence.
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
// the gate's first run found a genuine latent 42501 whose fix is a posture edit
// in another lane, and a gate that cannot be landed until someone else's commit
// merges never gets landed at all. The three properties that keep it honest:
//
//  1. It matches EXACTLY one (role, table, privilege). Any other CRITICAL fails
//     the gate, including a different privilege on the same table.
//  2. An entry that STOPS reproducing fails the gate too ("stale entry, delete
//     it"). So it cannot outlive the fix, which is what turns allowlists into
//     permanent blindness.
//  3. Every entry names a ticket. An entry without one fails the gate.
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
// and the ones that must fail the gate, and separately reports allowlist entries
// that no longer reproduce. A stale entry is itself a failure: it means the fix
// landed and the gate is now carrying a blind spot for a defect that no longer
// exists, which is exactly how a list like this rots into permanent suppression.
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
	// UndeclaredByEvidence are tables this role's posture declares that the
	// derivation found NO call site for. Each is either a legitimate
	// over-declaration or a path inside a blind spot -- this tool cannot tell
	// which, and says so rather than guessing.
	UndeclaredByEvidence []string
}

// RoleReport is the multi-role comparison result.
type RoleReport struct {
	Findings   []Finding
	Incomplete []IncompleteRoleSurface
	// SharedPairs are the (table, privilege) pairs with proven call sites on
	// more than one pool -- the DERIVED dual-grant set. Every one of these
	// legitimately appears in more than one posture and must not be read as a
	// leak.
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
		report.Incomplete = append(report.Incomplete, incompleteFor(in))
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
			mine := evidenceSites(surface, p)
			exclusive := map[string]bool{}
			for site := range mine {
				shared := false
				for role, other := range byRole {
					if role == in.Role {
						continue
					}
					if evidenceSites(other.Derived.Tables[table], p)[site] {
						shared = true
						break
					}
				}
				if !shared {
					exclusive[site] = true
				}
			}
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
			sites := make([]string, 0, len(exclusive))
			for site := range exclusive {
				sites = append(sites, site)
			}
			sort.Strings(sites)
			findings = append(findings, Finding{
				Severity: Critical, Table: table, Privilege: p, Role: in.Role,
				Summary: fmt.Sprintf(
					"role %s, table %q needs %s, proven by %d call site(s) reachable ONLY through the %s pool (%s), but this role's posture does not authorize it -- a 42501 the moment that path runs",
					in.Role, table, p, len(exclusive), in.Role, strings.Join(firstN(sites, 3), ", ")),
				Evidence: evidenceFor(surface, p),
			})
		}

		if surface.RequiresAnyWriteLock && !columnScoped {
			// Postgres wants at least ONE of INSERT/UPDATE/DELETE for a LOCK
			// TABLE mode stricter than ROW EXCLUSIVE -- any one, not a specific
			// one. Deliberately a different shape from the FOR UPDATE / FOR
			// SHARE row-lock clauses, which require UPDATE specifically and are
			// modelled as an ordinary UPDATE requirement above. Both forms are
			// modelled; do not collapse them.
			if !required.Has(PrivInsert) && !required.Has(PrivUpdate) && !required.Has(PrivDelete) {
				findings = append(findings, Finding{
					Severity: Critical, Table: table, Role: in.Role,
					Summary: fmt.Sprintf(
						"role %s, table %q: LOCK TABLE in an exclusive-ish mode requires at least one of INSERT/UPDATE/DELETE, but this role's posture grants none of them",
						in.Role, table),
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
			precision := "COARSE same-function-body grouping, so co-residency in one transaction is likely but not proven"
			if strings.HasPrefix(group, "txorigin:") {
				precision = "traced to a single unambiguous Begin() call site, so these statements PROVABLY share one transaction"
			}
			findings = append(findings, Finding{
				Severity: Critical, Table: strings.Join(tables, ", "), Role: in.Role,
				Summary: fmt.Sprintf(
					"TRANSACTION STRADDLES THE ROLE PARTITION: transaction %s runs on the %s pool and touches %s, but %s not authorized for %s (%s). A transaction cannot span two pools -- either grant these to %s too (dual-grant) or restructure the transaction. This surfaces as a 42501 at runtime, not as a routing error",
					group, in.Role, strings.Join(tables, "+"),
					strings.Join(elsewhere, "; "), in.Role, precision, in.Role),
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
func incompleteFor(in RoleInput) IncompleteRoleSurface {
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
		if u.Callee != "" && !inModule {
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
	for table := range in.Truth.RequiredTablePrivileges {
		if surface := in.Derived.Tables[table]; surface == nil || surface.Privileges.Empty() {
			out.UndeclaredByEvidence = append(out.UndeclaredByEvidence, table)
		}
	}
	for table := range in.Truth.ColumnScopedTables {
		if surface := in.Derived.Tables[table]; surface == nil || surface.Privileges.Empty() {
			out.UndeclaredByEvidence = append(out.UndeclaredByEvidence, table+" (column-scoped)")
		}
	}
	sort.Strings(out.UndeclaredByEvidence)
	sort.SliceStable(out.WiringHops, func(i, j int) bool {
		if out.WiringHops[i].File != out.WiringHops[j].File {
			return out.WiringHops[i].File < out.WiringHops[j].File
		}
		return out.WiringHops[i].Line < out.WiringHops[j].Line
	})
	return out
}
