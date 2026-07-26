package domaingrants

import (
	"fmt"
	"go/ast"
	"go/types"
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
	conflict := map[funcValueKey]bool{}

	record := func(key funcValueKey, target funcValueTarget) {
		if key.owner == nil || key.field == "" || conflict[key] {
			return
		}
		if target.empty() {
			conflict[key] = true
			delete(a.funcValueTargets, key)
			return
		}
		existing, seen := a.funcValueTargets[key]
		if seen && !existing.sameAs(target) {
			conflict[key] = true
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
