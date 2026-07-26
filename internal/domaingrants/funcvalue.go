package domaingrants

import (
	"fmt"
	"go/ast"
	"go/types"
	"sort"
)

// This file closes the analyzer's function-VALUE blind spot: taint that dies
// at a call through a function-typed struct field.
//
// The shape is this repo's dependency-injection idiom. A composition struct
// declares function-typed fields, a package-level var fills them in, and the
// builder is invoked through the field:
//
//	type schedulerRuntimeSources struct {
//	        newFixedLoop   func(*pgxpool.Pool, *health.Registry) (fixedScheduleRuntime, error)
//	        newRepository  func(*pgxpool.Pool) (schedulersync.HandoffStepper, error)
//	}
//	var productionSchedulerRuntimeSources = schedulerRuntimeSources{
//	        newFixedLoop:  buildFixedScheduleLoop,                      // FORM 1: named func
//	        newRepository: func(pool *pgxpool.Pool) (...) { ... },      // FORM 2: func literal
//	}
//	...
//	loop, err := sources.newFixedLoop(coordinatorPool, registry)       // callee unresolvable
//
// At that last line handleCall's callee resolution fails: info.Selections
// yields a *types.Var (a struct field), not a *types.Func, so taint stops and
// nothing the builder reaches is ever analyzed.
//
// Why this mattered enough to fix rather than document: it hid the ENTIRE
// scheduler and fixed-engine query surface from the coordinator derivation --
// 8 of CoordinatorPosture()'s 19 tables (scheduled_jobs,
// scheduled_sync_occurrences, fixed_schedule_occurrences, organizations,
// sync_configurations, remaining_metric_runs, remaining_metric_partitions,
// work_graph_execution_requests). The reconciler's equivalent hops
// (sources.buildRelay, sources.buildSyncMutation) only appeared to work
// because buildSyncMutationPipeline happens to NAME its parameter
// `coordinatorPool`, which re-seeds by naming convention. A gate whose
// completeness rests on a parameter name is not a gate.
//
// Discipline: fail-closed, matching buildSQLParamConstants and buildTxOrigins.
// A field with two distinct targets anywhere in the module resolves to NOTHING
// and is reported as unresolved, rather than picking one and guessing. Only
// non-test files contribute targets, so a test double cannot widen or poison
// the production surface.

// buildPoolParamRoles records, for every *pgxpool.Pool-typed PARAMETER of an
// in-module function, which pool roles are actually passed to it at its call
// sites. It is deliberately ROLE-AGNOSTIC -- computed once from poolRoleSeeds for
// every role, independent of which role the current run is deriving -- because
// its whole purpose is to let one run see what the other run's evidence says.
//
// It exists to close an ATTRIBUTION INVERSION, which is worse than the
// incompleteness this package already documents. A parameter is a seed root by
// SPELLING (`coordinatorPool`, `domainPool`), and spelling can contradict
// reality: `build(domainPool *pgxpool.Pool)` called only with the coordinator
// pool is seeded by the DOMAIN run purely from its name, while the coordinator
// run's barrier discards it. The downstream SQL then looks domain-EXCLUSIVE, and
// the gate confidently directs the privilege to the WRONG role -- a false
// attribution, not merely a missing one, and role attribution is the central
// correctness property of the split.
//
// So: call-site evidence outranks the name. When the observed roles for a
// parameter are non-empty and exclude the role being derived, the name-based seed
// is suppressed for that run (see nameSeedContradicted). When no call site can be
// resolved, the name still applies -- the convention is the only signal left, and
// dropping it would lose real surface.
func (a *analyzer) buildPoolParamRoles() {
	a.poolParamRoles = map[*types.Func]map[int]map[PoolRole]bool{}

	// roleOfExpr answers "which role's pool is this expression, syntactically?"
	// for every role at once.
	roleOfExpr := func(expr ast.Expr, info *types.Info) []PoolRole {
		var out []PoolRole
		for role, seeds := range poolRoleSeeds {
			match := false
			switch e := expr.(type) {
			case *ast.SelectorExpr:
				match = seeds.fields[e.Sel.Name] && isPgxPoolPtr(info.TypeOf(e))
			case *ast.CallExpr:
				if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
					match = seeds.getters[sel.Sel.Name] && isPgxPoolPtr(info.TypeOf(e))
				}
			case *ast.Ident:
				match = seeds.idents[e.Name] && isPgxPoolPtr(info.TypeOf(e))
			}
			if match {
				out = append(out, role)
			}
		}
		return out
	}

	record := func(fn *types.Func, idx int, roles []PoolRole) bool {
		if len(roles) == 0 {
			return false
		}
		if a.poolParamRoles[fn] == nil {
			a.poolParamRoles[fn] = map[int]map[PoolRole]bool{}
		}
		if a.poolParamRoles[fn][idx] == nil {
			a.poolParamRoles[fn][idx] = map[PoolRole]bool{}
		}
		changed := false
		for _, role := range roles {
			if !a.poolParamRoles[fn][idx][role] {
				a.poolParamRoles[fn][idx][role] = true
				changed = true
			}
		}
		return changed
	}

	const maxIterations = 12
	for iter := 0; iter < maxIterations; iter++ {
		changed := false
		for _, ctx := range a.funcDecls {
			var currentFn *types.Func
			if obj, ok := ctx.info.Defs[ctx.decl.Name]; ok {
				currentFn, _ = obj.(*types.Func)
			}
			ast.Inspect(ctx.decl.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee := simpleCalleeFunc(call, ctx.info)
				if callee == nil {
					return true
				}
				sig, ok := callee.Type().(*types.Signature)
				if !ok {
					return true
				}
				for i, arg := range call.Args {
					if i >= sig.Params().Len() || !isPgxPoolPtr(sig.Params().At(i).Type()) {
						continue
					}
					roles := roleOfExpr(arg, ctx.info)
					// Forward the enclosing function's own parameter roles, so a
					// pool threaded through several hops keeps its provenance.
					if id, isIdent := arg.(*ast.Ident); isIdent && currentFn != nil {
						if idx := paramIndex(ctx.funcType(), id.Name); idx >= 0 {
							for role := range a.poolParamRoles[currentFn][idx] {
								roles = append(roles, role)
							}
						}
					}
					if record(callee, i, roles) {
						changed = true
					}
				}
				return true
			})
		}
		if !changed {
			break
		}
	}
}

// NameSeedOverride records one place the naming convention was overruled by
// call-site evidence.
type NameSeedOverride struct {
	File string
	Line int
	// Name is the parameter whose spelling suggested this run's pool.
	Name string
	// Function is the enclosing function.
	Function string
	// ObservedRoles are the roles actually passed at the parameter's call sites.
	ObservedRoles []string
}

func (a *analyzer) recordNameSeedOverride(expr ast.Expr, ctx funcCtx, fn *types.Func) {
	id, ok := expr.(*ast.Ident)
	if !ok || fn == nil {
		return
	}
	idx := paramIndex(ctx.funcType(), id.Name)
	if idx < 0 {
		return
	}
	var observed []string
	for role := range a.poolParamRoles[fn][idx] {
		observed = append(observed, string(role))
	}
	sort.Strings(observed)
	pos := a.fset.Position(expr.Pos())
	override := NameSeedOverride{
		File: a.relFile(pos), Line: pos.Line, Name: id.Name,
		Function: txGroupFallbackName(ctx), ObservedRoles: observed,
	}
	for _, existing := range a.nameSeedOverrides {
		if existing.File == override.File && existing.Line == override.Line {
			return
		}
	}
	a.nameSeedOverrides = append(a.nameSeedOverrides, override)
}

// nameSeedContradicted reports whether expr is an identifier that would be a seed
// for this run PURELY by its name, while the call-site evidence for that
// parameter names only OTHER roles. See buildPoolParamRoles.
func (a *analyzer) nameSeedContradicted(expr ast.Expr, ctx funcCtx, fn *types.Func) bool {
	id, ok := expr.(*ast.Ident)
	if !ok || fn == nil || !a.seeds.idents[id.Name] {
		return false
	}
	idx := paramIndex(ctx.funcType(), id.Name)
	if idx < 0 {
		return false // a local, not a parameter: no call sites to consult
	}
	observed := a.poolParamRoles[fn][idx]
	if len(observed) == 0 {
		return false // no resolvable call site: the name is the only signal left
	}
	return !observed[a.seeds.role]
}

// funcValueScope is one file buildFuncValueTargets scans for function-typed
// field assignments. Scanning whole files (rather than only function bodies)
// is what reaches the package-level `var productionXSources = XSources{...}`
// composite literals where these fields are actually filled in.
type funcValueScope struct {
	node ast.Node
	info *types.Info
}

// funcValueKey identifies one function-typed field: the named struct type that
// declares it, plus the field name.
type funcValueKey struct {
	owner *types.Named
	field string
}

// funcValueTarget is the single resolved implementation behind a
// function-typed field. Exactly one of fn (Form 1, a declared function) or lit
// (Form 2, a function literal) is set.
type funcValueTarget struct {
	fn  *types.Func
	lit *ast.FuncLit
}

func (t funcValueTarget) empty() bool { return t.fn == nil && t.lit == nil }

func (t funcValueTarget) sameAs(other funcValueTarget) bool {
	return t.fn == other.fn && t.lit == other.lit
}

// display names the target for the audit trail.
func (a *analyzer) display(t funcValueTarget) string {
	if t.fn != nil {
		return t.fn.FullName()
	}
	if t.lit != nil {
		pos := a.fset.Position(t.lit.Pos())
		return fmt.Sprintf("func literal at %s:%d", a.relFile(pos), pos.Line)
	}
	return "<none>"
}

// FuncValueResolvedCallSite records every call this pass resolved, so the
// report can show exactly where the (deliberately narrow) unique-target
// shortcut was taken -- the same auditability DevirtualizedCallSite provides
// for the unique-implementer shortcut.
type FuncValueResolvedCallSite struct {
	File   string
	Line   int
	Field  string // "OwnerType.fieldName"
	Target string
}

// FuncValueConflictSite is a tainted call through a function-typed field this
// pass deliberately refused to resolve. Every one is a hole in the surface: the
// arguments are pool-tainted, so real SQL may lie beyond it, and nothing past
// this point was analyzed.
type FuncValueConflictSite struct {
	File   string
	Line   int
	Field  string // "OwnerType.fieldName"
	Reason string
}

// buildFuncValueTargets resolves each function-typed field to its single
// implementation, or to nothing when the module assigns it more than one.
//
// Sources of an assignment, both handled:
//   - a keyed entry in a composite literal of the owning struct type
//     (`schedulerRuntimeSources{newFixedLoop: buildFixedScheduleLoop}`)
//   - a plain assignment to the field (`sources.newFixedLoop = someBuilder`)
//
// A value that is neither a resolvable declared function nor a function
// literal (a further field, a call result, a parameter) marks the field
// CONFLICTED rather than being ignored: "we could not see what goes here" must
// not read the same as "nothing goes here".
func (a *analyzer) buildFuncValueTargets() {
	a.funcValueTargets = map[funcValueKey]funcValueTarget{}
	a.funcValueConflicts = map[funcValueKey]string{}

	// Excluding a field is only half of fail-closed. The exclusion has to be
	// REPORTED, or "two production builders, neither analyzed" is
	// indistinguishable from "no such field" and CI stays green over a surface
	// nothing examined. That is fail-SILENT, and it is what a.funcValueConflicts
	// exists to prevent -- the reason is carried so the report can say which of
	// the two shapes happened.
	record := func(key funcValueKey, target funcValueTarget) {
		if key.owner == nil || key.field == "" {
			return
		}
		if _, conflicted := a.funcValueConflicts[key]; conflicted {
			return
		}
		if target.empty() {
			a.funcValueConflicts[key] = "assigned a value this analyzer cannot resolve to a function body " +
				"(a further field, a call result, or a parameter)"
			delete(a.funcValueTargets, key)
			return
		}
		existing, seen := a.funcValueTargets[key]
		if seen && !existing.sameAs(target) {
			a.funcValueConflicts[key] = fmt.Sprintf(
				"assigned MORE THAN ONE implementation (%s and %s), so neither is analyzed",
				a.display(existing), a.display(target))
			delete(a.funcValueTargets, key)
			return
		}
		a.funcValueTargets[key] = target
	}

	// resolveValue turns the right-hand side of a function-typed assignment
	// into a target. Returns an empty target for anything it cannot see
	// through, which record() treats as a conflict.
	resolveValue := func(value ast.Expr, info *types.Info) funcValueTarget {
		switch v := value.(type) {
		case *ast.FuncLit:
			return funcValueTarget{lit: v}
		case *ast.Ident:
			if fn, ok := info.Uses[v].(*types.Func); ok {
				if _, known := a.funcDecls[fn]; known {
					return funcValueTarget{fn: fn}
				}
			}
		case *ast.SelectorExpr:
			// Package-qualified function reference, e.g.
			// `newLoop: schedulersync.NewLoop`.
			if fn, ok := info.Uses[v.Sel].(*types.Func); ok {
				if _, known := a.funcDecls[fn]; known {
					return funcValueTarget{fn: fn}
				}
			}
		}
		return funcValueTarget{}
	}

	isFuncTyped := func(t types.Type) bool {
		if t == nil {
			return false
		}
		_, ok := t.Underlying().(*types.Signature)
		return ok
	}

	for _, ctx := range a.funcValueScopes {
		ast.Inspect(ctx.node, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CompositeLit:
				owner := namedTypeOf(ctx.info.TypeOf(node))
				if owner == nil {
					return true
				}
				for _, elt := range node.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					name, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					if !isFuncTyped(ctx.info.TypeOf(kv.Value)) {
						continue
					}
					record(funcValueKey{owner: owner, field: name.Name},
						resolveValue(kv.Value, ctx.info))
				}
			case *ast.AssignStmt:
				if len(node.Lhs) != len(node.Rhs) {
					return true
				}
				for i, lhs := range node.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if !isFuncTyped(ctx.info.TypeOf(node.Rhs[i])) {
						continue
					}
					record(funcValueKey{
						owner: namedTypeOf(ctx.info.TypeOf(sel.X)),
						field: sel.Sel.Name,
					}, resolveValue(node.Rhs[i], ctx.info))
				}
			}
			return true
		})
	}
}

// resolveFuncValueField answers "which body does `recv.field(...)` run?" for a
// call whose callee resolution already failed. Returns an empty target when
// the field is unknown or has more than one implementation.
func (a *analyzer) resolveFuncValueField(sel *ast.SelectorExpr, info *types.Info) funcValueTarget {
	owner := namedTypeOf(info.TypeOf(sel.X))
	if owner == nil {
		return funcValueTarget{}
	}
	return a.funcValueTargets[funcValueKey{owner: owner, field: sel.Sel.Name}]
}

// markLitParamTainted is markParamTainted for a function literal. Kept as a
// parallel map rather than folded into taintedParam because a *ast.FuncLit has
// no *types.Func to key on.
func (a *analyzer) markLitParamTainted(lit *ast.FuncLit, idx int) {
	m := a.taintedLitParam[lit]
	if m == nil {
		m = map[int]bool{}
		a.taintedLitParam[lit] = m
	}
	if !m[idx] {
		m[idx] = true
		a.changed = true
	}
}

// propagateIntoFuncValue pushes argument taint into a resolved target's
// parameters and records the resolution for the audit trail. Reports whether
// it handled the call.
func (a *analyzer) propagateIntoFuncValue(
	call *ast.CallExpr,
	sel *ast.SelectorExpr,
	ctx funcCtx,
	isTainted func(ast.Expr) bool,
) bool {
	target := a.resolveFuncValueField(sel, ctx.info)
	if target.empty() {
		// Deliberately excluded, so say so at the CALL SITE. Reporting only the
		// field would leave a reader unable to tell whether any tainted call ever
		// reaches it; reporting only Unresolved loses it entirely, because that
		// record's callee is the bare field name and the incompleteness filter
		// cannot tell such a name from a third-party method.
		if owner := namedTypeOf(ctx.info.TypeOf(sel.X)); owner != nil {
			key := funcValueKey{owner: owner, field: sel.Sel.Name}
			if reason, conflicted := a.funcValueConflicts[key]; conflicted {
				pos := a.fset.Position(call.Pos())
				a.funcValueConflictSites = append(a.funcValueConflictSites, FuncValueConflictSite{
					File:   a.relFile(pos),
					Line:   pos.Line,
					Field:  owner.Obj().Name() + "." + sel.Sel.Name,
					Reason: reason,
				})
			}
		}
		return false
	}
	for i, arg := range call.Args {
		if !isTainted(arg) {
			continue
		}
		switch {
		case target.fn != nil:
			a.markParamTainted(target.fn, i)
		case target.lit != nil:
			a.markLitParamTainted(target.lit, i)
		}
	}
	pos := a.fset.Position(call.Pos())
	ownerName := "?"
	if owner := namedTypeOf(ctx.info.TypeOf(sel.X)); owner != nil {
		ownerName = owner.Obj().Name()
	}
	a.funcValueResolved = append(a.funcValueResolved, FuncValueResolvedCallSite{
		File:   a.relFile(pos),
		Line:   pos.Line,
		Field:  ownerName + "." + sel.Sel.Name,
		Target: a.display(target),
	})
	return true
}
