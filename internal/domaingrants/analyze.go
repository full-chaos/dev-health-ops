package domaingrants

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Evidence anchors one derived privilege requirement to the source line that
// proved it.
type Evidence struct {
	File      string
	Line      int
	Privilege Privilege
	Statement string // normalized (whitespace-collapsed) SQL text
	// TxGroup identifies the enclosing Go function this statement executed
	// in, as a coarse proxy for "same database transaction". It is
	// FUNCTION-BODY-SCOPED ONLY: it does NOT trace a pgx.Tx passed across
	// function/type boundaries (e.g. NativePostSyncService.Fanout opens one
	// tx and hands it to four different writer types' methods -- those
	// statements get four different TxGroup values here, even though they
	// commit together). See the handoff README's known-limitations list.
	// Two statements sharing a TxGroup ARE reliably in the same transaction
	// (modulo early-return branches that never reach a later statement);
	// two statements in DIFFERENT TxGroups may still share a transaction.
	TxGroup string
}

// TableSurface is everything the analyzer proved about one public-schema
// table: which privileges are required, and the evidence for each.
type TableSurface struct {
	Table      string
	Privileges PrivilegeSet
	Evidence   []Evidence
	// RequiresAnyWriteLock and its evidence: see
	// StatementResult.RequiresAnyWriteLock's doc comment (sql.go). Checked
	// separately from Privileges because Postgres's requirement here is
	// "at least one of INSERT/UPDATE/DELETE", not a single specific
	// privilege.
	RequiresAnyWriteLock bool
	WriteLockEvidence    []Evidence
}

// DynamicSite is a SQL-shaped call (X.Exec/Query/QueryRow/... on a
// domain-pool-tainted receiver) whose SQL argument could not be resolved to
// a compile-time constant string, so the analyzer could not classify it.
// These are reported, never silently dropped -- see the handoff README.
type DynamicSite struct {
	File   string
	Line   int
	Reason string
}

// UnresolvedCallSite is a call that received a domain-pool-tainted argument
// but whose callee could not be resolved to an in-module function body
// (interface dispatch, a function-typed field/parameter, or a
// stdlib/third-party function) -- so taint propagation stops there. Reported
// as a known-limitation instance, not silently dropped.
type UnresolvedCallSite struct {
	File   string
	Line   int
	Callee string
	Reason string
}

// DerivedSurface is the full result of statically deriving the domain-pool
// query surface.
type DerivedSurface struct {
	Tables        map[string]*TableSurface
	Dynamic       []DynamicSite
	Unresolved    []UnresolvedCallSite
	SeedSites     []Evidence // where a *.Domain/.domainPool/.DomainPool() root was found, for auditing tool scope
	Devirtualized []DevirtualizedCallSite
	rootModule    string
}

type funcCtx struct {
	decl *ast.FuncDecl
	info *types.Info
	pkg  *packages.Package
	file string
}

type analyzer struct {
	fset         *token.FileSet
	rootModule   string
	funcDecls    map[*types.Func]funcCtx
	taintedParam map[*types.Func]map[int]bool
	taintedField map[*types.Named]map[string]bool
	changed      bool

	tables     map[string]*TableSurface
	dynamic    []DynamicSite
	unresolved []UnresolvedCallSite
	seedSites  []Evidence
	seedSeen   map[string]bool

	// implementers maps an interface's Named type to every non-test,
	// in-module concrete Named type that implements it. Used only to
	// devirtualize a call through an interface when it has EXACTLY one
	// module-local implementer (see resolveDevirtualized) -- a common,
	// deliberately narrow heuristic, not a sound points-to analysis. See the
	// handoff README's known-limitations list.
	implementers map[*types.Named][]*types.Named
	devirt       []DevirtualizedCallSite
}

// DevirtualizedCallSite records every place the analyzer resolved an
// interface method call to a concrete implementation via the
// unique-implementer heuristic, so the report can show exactly where this
// (unsound in general) shortcut was taken.
type DevirtualizedCallSite struct {
	File      string
	Line      int
	Interface string
	Concrete  string
	Method    string
}

// debugConvergence, when true, prints per-iteration fact counts to help
// diagnose non-convergence. Off by default; flip locally when debugging.
var debugConvergence = false

// isSQLMethod is the set of pgx/pgxpool method names this analyzer treats
// as SQL-execution sinks. Deliberately narrow: Prepare/CopyFrom/etc. are not
// observed anywhere in the domain-pool query surface today; if that changes,
// extend this set (and the handoff README's known-limitations list).
var isSQLMethod = map[string]bool{
	"Exec":      true,
	"Query":     true,
	"QueryRow":  true,
	"QueryFunc": true,
	"SendBatch": true,
}

// Derive loads the Go module rooted at moduleDir and statically derives the
// per-table privilege surface reachable through the Postgres domain
// connection pool, starting from every "*.Domain" / "*.domainPool" /
// "*.DomainPool()" root found anywhere under moduleDir (not limited to a
// fixed list of cmd/ directories -- see the handoff README for why that
// matters: cmd/dev-health-reconciler and cmd/dev-health-scheduler also wire
// the domain pool and are not optional).
func Derive(moduleDir string) (*DerivedSurface, error) {
	cfg := &packages.Config{
		Dir: moduleDir,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedImports | packages.NeedDeps | packages.NeedTypes |
			packages.NeedSyntax | packages.NeedTypesInfo,
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, fmt.Errorf("domaingrants: loading packages: %w", err)
	}
	var loadErrs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs = append(loadErrs, fmt.Sprintf("%s: %s", p.PkgPath, e))
		}
	})
	if len(loadErrs) > 0 {
		return nil, fmt.Errorf("domaingrants: %d package load error(s), e.g.:\n%s",
			len(loadErrs), strings.Join(firstN(loadErrs, 10), "\n"))
	}

	a := &analyzer{
		fset:         nil,
		rootModule:   moduleDir,
		funcDecls:    map[*types.Func]funcCtx{},
		taintedParam: map[*types.Func]map[int]bool{},
		taintedField: map[*types.Named]map[string]bool{},
		tables:       map[string]*TableSurface{},
		seedSeen:     map[string]bool{},
	}

	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if a.fset == nil {
			a.fset = p.Fset
		}
		for i, file := range p.Syntax {
			var filename string
			if i < len(p.CompiledGoFiles) {
				filename = p.CompiledGoFiles[i]
			} else {
				filename = a.fset.Position(file.Pos()).Filename
			}
			if strings.HasSuffix(filename, "_test.go") {
				continue
			}
			for _, decl := range file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				obj, ok := p.TypesInfo.Defs[fd.Name]
				if !ok || obj == nil {
					continue
				}
				fn, ok := obj.(*types.Func)
				if !ok {
					continue
				}
				a.funcDecls[fn] = funcCtx{decl: fd, info: p.TypesInfo, pkg: p, file: filename}
			}
		}
	})

	if len(a.funcDecls) == 0 {
		return nil, fmt.Errorf("domaingrants: discovered zero function declarations under %s; "+
			"go/packages load likely failed silently", moduleDir)
	}
	a.buildImplementers(pkgs)

	const maxIterations = 40
	for iter := 0; iter < maxIterations; iter++ {
		a.changed = false
		// SQL/diagnostic collection is authoritative only once taint facts
		// have stabilized; recomputed fresh each pass and kept only from the
		// final (stable) pass so earlier under-tainted passes never leave
		// stale partial evidence behind.
		a.tables = map[string]*TableSurface{}
		a.dynamic = nil
		a.unresolved = nil
		a.devirt = nil
		a.seedSites = nil
		a.seedSeen = map[string]bool{}
		for fn, ctx := range a.funcDecls {
			a.walkFunc(fn, ctx)
		}
		if debugConvergence {
			totalParams, totalFields := 0, 0
			for _, m := range a.taintedParam {
				totalParams += len(m)
			}
			for _, m := range a.taintedField {
				totalFields += len(m)
			}
			fmt.Printf("[domaingrants] iter=%d changed=%v taintedParams=%d taintedFields=%d tables=%d\n",
				iter, a.changed, totalParams, totalFields, len(a.tables))
		}
		if !a.changed {
			break
		}
		if iter == maxIterations-1 {
			return nil, fmt.Errorf("domaingrants: taint propagation did not converge within %d iterations", maxIterations)
		}
	}

	return &DerivedSurface{
		Tables:        a.tables,
		Dynamic:       a.dynamic,
		Unresolved:    a.unresolved,
		SeedSites:     a.seedSites,
		Devirtualized: a.devirt,
		rootModule:    moduleDir,
	}, nil
}

// buildImplementers scans every non-test, module-loaded package's package
// scope for named interface and concrete types, then records which concrete
// types implement which interfaces (by value or pointer method set). This
// powers resolveDevirtualized: when a call through an interface can't be
// resolved to a body directly (the abstract *types.Func has none), and the
// static interface type has EXACTLY one module-local implementer, taint is
// propagated into that implementer's method as a best-effort fallback. This
// is intentionally narrow -- a second implementer added later (including a
// production feature-flag variant, not just a test mock) silently disables
// the fallback for that interface, which is the safe failure direction: the
// call reverts to being reported as Unresolved rather than mis-attributed.
func (a *analyzer) buildImplementers(pkgs []*packages.Package) {
	var interfaces, concretes []*types.Named
	seen := map[*types.Named]bool{}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if p.Types == nil || p.Types.Scope() == nil {
			return
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok || seen[named] {
				continue
			}
			seen[named] = true
			if _, isIface := named.Underlying().(*types.Interface); isIface {
				interfaces = append(interfaces, named)
			} else if _, isStruct := named.Underlying().(*types.Struct); isStruct {
				concretes = append(concretes, named)
			}
		}
	})
	a.implementers = map[*types.Named][]*types.Named{}
	for _, iface := range interfaces {
		ifaceType, ok := iface.Underlying().(*types.Interface)
		if !ok || ifaceType.NumMethods() == 0 {
			continue // empty interface (e.g. `any`) matches everything; not useful here
		}
		for _, c := range concretes {
			if types.Implements(c, ifaceType) || types.Implements(types.NewPointer(c), ifaceType) {
				a.implementers[iface] = append(a.implementers[iface], c)
			}
		}
	}
}

func methodByName(named *types.Named, name string) *types.Func {
	if named == nil {
		return nil
	}
	for i := 0; i < named.NumMethods(); i++ {
		if m := named.Method(i); m.Name() == name {
			return m
		}
	}
	return nil
}

// resolveDevirtualized attempts the unique-implementer fallback described on
// buildImplementers. recvExpr is the receiver expression at the call site
// (used only for evidence positions), recvType is its static type.
func (a *analyzer) resolveDevirtualized(recvExpr ast.Expr, recvType types.Type, methodName string) *types.Func {
	iface := namedTypeOf(recvType)
	if iface == nil {
		return nil
	}
	if _, isIface := iface.Underlying().(*types.Interface); !isIface {
		return nil
	}
	impls := a.implementers[iface]
	if len(impls) != 1 {
		return nil
	}
	concrete := methodByName(impls[0], methodName)
	if concrete == nil {
		return nil
	}
	if _, ok := a.funcDecls[concrete]; !ok {
		return nil
	}
	pos := a.fset.Position(recvExpr.Pos())
	a.devirt = append(a.devirt, DevirtualizedCallSite{
		File: a.relFile(pos), Line: pos.Line,
		Interface: iface.Obj().Name(), Concrete: impls[0].Obj().Name(), Method: methodName,
	})
	return concrete
}

func firstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (a *analyzer) relFile(pos token.Position) string {
	rel, err := filepath.Rel(a.rootModule, pos.Filename)
	if err != nil {
		return pos.Filename
	}
	return rel
}

func (a *analyzer) markParamTainted(fn *types.Func, idx int) {
	m := a.taintedParam[fn]
	if m == nil {
		m = map[int]bool{}
		a.taintedParam[fn] = m
	}
	if !m[idx] {
		m[idx] = true
		a.changed = true
	}
}

func (a *analyzer) markFieldTainted(named *types.Named, field string) {
	if named == nil || field == "" {
		return
	}
	m := a.taintedField[named]
	if m == nil {
		m = map[string]bool{}
		a.taintedField[named] = m
	}
	if !m[field] {
		m[field] = true
		a.changed = true
	}
}

func namedTypeOf(t types.Type) *types.Named {
	if t == nil {
		return nil
	}
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		return n
	}
	return nil
}

func isPgxPoolPtr(t types.Type) bool {
	p, ok := t.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := p.Elem().(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Name() == "Pool" && strings.HasSuffix(named.Obj().Pkg().Path(), "pgxpool")
}

// exprTainted decides whether expr's value is derived from the domain pool,
// given the current global taint facts (taintedField, and -- via the
// caller's seed detection -- the "*.Domain"/"*.domainPool"/"*.DomainPool()"
// syntactic roots) plus this function's local taint set (locals: params
// already known tainted, and any local variable this same walk has already
// assigned from a tainted expression).
func (a *analyzer) exprTainted(expr ast.Expr, info *types.Info, locals map[string]bool) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case *ast.ParenExpr:
		return a.exprTainted(e.X, info, locals)
	case *ast.StarExpr:
		return a.exprTainted(e.X, info, locals)
	case *ast.Ident:
		return locals[e.Name]
	case *ast.SelectorExpr:
		if (e.Sel.Name == "Domain" || e.Sel.Name == "domainPool") && isPgxPoolPtr(info.TypeOf(e)) {
			return true
		}
		if named := namedTypeOf(info.TypeOf(e.X)); named != nil {
			if fields := a.taintedField[named]; fields != nil && fields[e.Sel.Name] {
				return true
			}
		}
		// Anything selected off an already-tainted base (a method value like
		// pool.Begin, a further field, etc.) is tainted too. This is what
		// lets closure-captured patterns (`begin: pool.Begin` stored as a
		// struct field of function type) propagate without a special case
		// for every such field name.
		return a.exprTainted(e.X, info, locals)
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok &&
			sel.Sel.Name == "DomainPool" && isPgxPoolPtr(info.TypeOf(e)) {
			return true
		}
		// Calling an already-tainted function value (a method on a tainted
		// receiver, or a closure-captured field derived from the pool)
		// produces a tainted result too.
		return a.exprTainted(e.Fun, info, locals)
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			return a.exprTainted(e.X, info, locals)
		}
		return false
	case *ast.CompositeLit:
		// A composite literal built (and typically passed straight through
		// as a call argument, e.g. `New(dailyPostSyncWriter{store:
		// taintedStore, publisher: p})`) is tainted if any element is. This
		// mirrors handleCompositeLit's field-marking, but answers a
		// different question: whether the literal VALUE ITSELF is tainted
		// for the purpose of taint further downstream (e.g. as another
		// call's argument), not which of ITS OWN fields to mark.
		for _, elt := range e.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if a.exprTainted(kv.Value, info, locals) {
					return true
				}
				continue
			}
			if a.exprTainted(elt, info, locals) {
				return true
			}
		}
		return false
	}
	return false
}

func (a *analyzer) recordSeed(expr ast.Expr) {
	pos := a.fset.Position(expr.Pos())
	key := fmt.Sprintf("%s:%d", pos.Filename, pos.Line)
	if a.seedSeen[key] {
		return
	}
	a.seedSeen[key] = true
	a.seedSites = append(a.seedSites, Evidence{File: a.relFile(pos), Line: pos.Line})
}

func (a *analyzer) walkFunc(fn *types.Func, ctx funcCtx) {
	locals := map[string]bool{}
	if ctx.decl.Type.Params != nil {
		idx := 0
		tainted := a.taintedParam[fn]
		for _, field := range ctx.decl.Type.Params.List {
			names := field.Names
			if len(names) == 0 {
				idx++
				continue
			}
			for _, n := range names {
				if tainted[idx] && n.Name != "_" {
					locals[n.Name] = true
				}
				idx++
			}
		}
	}

	// Seed the receiver identifier itself when its type has ANY tainted
	// field. Without this, `func (c *Controller) Pause(...)` calling
	// `c.beginRouteMutation(...)` (another method on the SAME receiver,
	// with no pool-shaped argument at that call site) never gets recognized
	// as tainted -- exprTainted's SelectorExpr case only matches
	// `recv.<specific tainted field>`, not `recv.<some other method that
	// internally touches the tainted field>`. Seeding the bare receiver
	// name closes that gap: any method call on an already-tainted receiver
	// is now itself treated as tainted, which is what lets taint flow
	// through same-type method-calling-method chains (materializer.begin,
	// controller.beginRouteMutation, etc.) -- a common shape in this
	// codebase's DI style. Slightly over-inclusive by design (a method that
	// happens not to touch the tainted field on a tainted receiver is still
	// treated as tainted) -- see the handoff README.
	if ctx.decl.Recv != nil && len(ctx.decl.Recv.List) == 1 && len(ctx.decl.Recv.List[0].Names) == 1 {
		recvName := ctx.decl.Recv.List[0].Names[0].Name
		recvNamed := namedTypeOf(ctx.info.TypeOf(ctx.decl.Recv.List[0].Type))
		if recvName != "_" && recvNamed != nil && len(a.taintedField[recvNamed]) > 0 {
			locals[recvName] = true
		}
	}

	isTainted := func(expr ast.Expr) bool {
		if a.isSeedExpr(expr, ctx.info) {
			a.recordSeed(expr)
			return true
		}
		return a.exprTainted(expr, ctx.info, locals)
	}

	ast.Inspect(ctx.decl.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			a.handleAssign(node, ctx, locals, isTainted)
		case *ast.CompositeLit:
			a.handleCompositeLit(node, ctx, isTainted)
		case *ast.CallExpr:
			a.handleCall(node, ctx, locals, isTainted)
		}
		return true
	})
}

// isSeedExpr is the syntactic domain-pool root: a "*.Domain" or
// "*.domainPool" field of pgxpool.Pool type, or a "*.DomainPool()" call
// returning one. See the handoff README for the wiring sites this matches
// and why (cmd/dev-health-worker's postgresDatabase.pools.Domain,
// cmd/dev-health-workerctl's pools.Domain, cmd/dev-health-reconciler's and
// cmd/dev-health-scheduler's DomainPool() getters, cmd/dev-health-stream-runner's
// storage.domainPool).
func (a *analyzer) isSeedExpr(expr ast.Expr, info *types.Info) bool {
	switch e := expr.(type) {
	case *ast.SelectorExpr:
		if (e.Sel.Name == "Domain" || e.Sel.Name == "domainPool") && isPgxPoolPtr(info.TypeOf(e)) {
			return true
		}
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "DomainPool" && isPgxPoolPtr(info.TypeOf(e)) {
			return true
		}
	case *ast.Ident:
		if e.Name == "domainPool" && isPgxPoolPtr(info.TypeOf(e)) {
			return true
		}
	}
	return false
}

func (a *analyzer) handleAssign(node *ast.AssignStmt, ctx funcCtx, locals map[string]bool, isTainted func(ast.Expr) bool) {
	if len(node.Lhs) == len(node.Rhs) {
		for i, rhs := range node.Rhs {
			tainted := isTainted(rhs)
			switch lhs := node.Lhs[i].(type) {
			case *ast.Ident:
				// Local-only: locals is rebuilt from scratch on every pass
				// (it is a function of the current, monotonic global facts),
				// so discovering it here again is not itself a new global
				// fact and must NOT set a.changed -- only markParamTainted/
				// markFieldTainted (global, monotonic maps) may do that.
				if tainted && lhs.Name != "_" {
					locals[lhs.Name] = true
				}
			case *ast.SelectorExpr:
				if tainted {
					a.markFieldTainted(namedTypeOf(ctx.info.TypeOf(lhs.X)), lhs.Sel.Name)
				}
			}
		}
		return
	}
	// Multi-value assignment, e.g. `store, err := daily.NewPostgresStore(pool)`
	// (taint carried by an argument) or `tx, err := service.pool.Begin(ctx)`
	// (taint carried by the callee/receiver itself, not any argument).
	// isTainted(call) covers both: exprTainted's CallExpr case already
	// checks "is the function value being called tainted" (which recurses
	// into the receiver, e.g. service.pool), and handleCall separately
	// checks argument taint for propagating into a resolved callee's
	// parameters -- this assignment handler only needs to know "is the
	// overall call result tainted", which isTainted(call) answers directly
	// rather than re-implementing a partial (args-only) version of the same
	// check.
	if len(node.Rhs) == 1 {
		if call, ok := node.Rhs[0].(*ast.CallExpr); ok && isTainted(call) {
			if id, ok := node.Lhs[0].(*ast.Ident); ok && id.Name != "_" {
				locals[id.Name] = true
			}
		}
	}
}

func (a *analyzer) handleCompositeLit(node *ast.CompositeLit, ctx funcCtx, isTainted func(ast.Expr) bool) {
	named := namedTypeOf(ctx.info.TypeOf(node))
	if named == nil {
		return
	}
	for _, elt := range node.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		if isTainted(kv.Value) {
			a.markFieldTainted(named, key.Name)
		}
	}
}

func (a *analyzer) handleCall(call *ast.CallExpr, ctx funcCtx, locals map[string]bool, isTainted func(ast.Expr) bool) {
	sel, isSel := call.Fun.(*ast.SelectorExpr)

	// SQL execution sink: X.Exec/Query/QueryRow/QueryFunc/SendBatch(ctx, sql, ...)
	// where X is domain-pool-tainted.
	if isSel && isSQLMethod[sel.Sel.Name] && isTainted(sel.X) {
		a.recordSQLSite(call, ctx)
	}

	// Resolve the callee to an in-module function so taint can propagate
	// into its parameters. Two shapes: plain identifier (`fn(...)`,
	// including package-qualified idents resolved via Uses) and method
	// selector (`recv.Method(...)`).
	var callee *types.Func
	var calleeDisplay string
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		if obj, ok := ctx.info.Uses[fn].(*types.Func); ok {
			callee = obj
			calleeDisplay = fn.Name
		}
	case *ast.SelectorExpr:
		if selinfo, ok := ctx.info.Selections[fn]; ok {
			if f, ok := selinfo.Obj().(*types.Func); ok {
				callee = f
			}
		} else if obj, ok := ctx.info.Uses[fn.Sel].(*types.Func); ok {
			callee = obj // package-qualified function, e.g. daily.NewPostgresStore
		}
		calleeDisplay = fn.Sel.Name
	}

	anyTaintedArg := false
	for _, arg := range call.Args {
		if isTainted(arg) {
			anyTaintedArg = true
			break
		}
	}
	if !anyTaintedArg {
		return
	}
	if callee == nil {
		a.recordUnresolved(call, ctx, calleeDisplay, "callee could not be resolved to a function (dynamic call through a variable/field of function type)")
		return
	}
	if _, ok := a.funcDecls[callee]; !ok {
		// Try the unique-implementer devirtualization fallback before giving
		// up: `recv.Method(...)` where recv is statically an interface with
		// exactly one module-local implementer.
		if isSel {
			if concrete := a.resolveDevirtualized(sel.X, ctx.info.TypeOf(sel.X), sel.Sel.Name); concrete != nil {
				callee = concrete
			}
		}
	}
	if _, ok := a.funcDecls[callee]; !ok {
		reason := "callee has no in-module body (stdlib/third-party function, or build-tag-excluded)"
		if callee.Pkg() != nil && strings.HasPrefix(callee.Pkg().Path(), a.modulePathPrefix()) {
			reason = "callee is declared in this module but has no concrete body reachable here (interface method with zero or multiple module-local implementers -- dynamic dispatch the unique-implementer fallback could not resolve)"
		}
		a.recordUnresolved(call, ctx, callee.FullName(), reason)
		return
	}
	for i, arg := range call.Args {
		if isTainted(arg) {
			a.markParamTainted(callee, i)
		}
	}
}

// modulePathPrefix returns the module import path prefix, derived once from
// any loaded function's package path via the standard
// github.com/full-chaos/dev-health-ops root -- used only to decide whether
// an unresolved callee is "ours" (worth flagging as a dynamic-dispatch
// limitation) or third-party (not worth the noise).
func (a *analyzer) modulePathPrefix() string {
	return "github.com/full-chaos/dev-health-ops"
}

func (a *analyzer) recordUnresolved(call *ast.CallExpr, ctx funcCtx, callee, reason string) {
	pos := a.fset.Position(call.Pos())
	a.unresolved = append(a.unresolved, UnresolvedCallSite{
		File: a.relFile(pos), Line: pos.Line, Callee: callee, Reason: reason,
	})
}

func (a *analyzer) recordSQLSite(call *ast.CallExpr, ctx funcCtx) {
	if len(call.Args) < 2 {
		return
	}
	sqlArg := call.Args[1]
	pos := a.fset.Position(call.Pos())
	txGroup := a.txGroupKey(ctx)

	if tv := ctx.info.Types[sqlArg]; tv.Value != nil && tv.Value.Kind() == constant.String {
		a.recordSQLText(constant.StringVal(tv.Value), pos, txGroup)
		return
	}

	// Fallback for the "loop over a fixed table of named SQL consts" shape
	// (internal/syncreconciler/materializer.go's Step is the motivating
	// case: `for _, step := range steps { tx.Exec(ctx, step.sql, ...) }`
	// where `steps` is a same-function slice-of-struct literal whose `sql`
	// field is always one of a small set of named consts). The SQL argument
	// itself (`step.sql`) is not a compile-time constant, but every value it
	// can possibly take IS -- so this resolves and records all of them
	// rather than giving up. Deliberately narrow: only same-function,
	// same-block slice literals with keyed-or-positional struct elements:
	// see resolveLoopTableSQL's doc comment for exactly what it does and
	// does not handle.
	if values, ok := a.resolveLoopTableSQL(sqlArg, ctx); ok {
		for _, sqlText := range values {
			a.recordSQLText(sqlText, pos, txGroup)
		}
		return
	}

	a.dynamic = append(a.dynamic, DynamicSite{
		File: a.relFile(pos), Line: pos.Line,
		Reason: "SQL argument is not a compile-time constant string expression",
	})
}

// txGroupKey returns the coarse "same enclosing function" transaction-group
// identifier for a SQL call site inside ctx. See Evidence.TxGroup's doc
// comment for exactly what this does and does not prove.
func (a *analyzer) txGroupKey(ctx funcCtx) string {
	name := ctx.decl.Name.Name
	if ctx.decl.Recv != nil && len(ctx.decl.Recv.List) == 1 {
		if named := namedTypeOf(ctx.info.TypeOf(ctx.decl.Recv.List[0].Type)); named != nil {
			name = named.Obj().Name() + "." + name
		}
	}
	return ctx.pkg.PkgPath + "." + name
}

func (a *analyzer) recordSQLText(sqlText string, pos token.Position, txGroup string) {
	stmt := ParseStatement(sqlText)
	normalized := normalizeSQL(sqlText)
	for table, privs := range stmt.Tables {
		surface := a.tables[table]
		if surface == nil {
			surface = &TableSurface{Table: table}
			a.tables[table] = surface
		}
		for p := Privilege(0); p < numPrivileges; p++ {
			if !privs.Has(p) {
				continue
			}
			surface.Privileges.add(p)
			surface.Evidence = append(surface.Evidence, Evidence{
				File: a.relFile(pos), Line: pos.Line, Privilege: p, Statement: normalized, TxGroup: txGroup,
			})
		}
	}
	for table := range stmt.RequiresAnyWriteLock {
		surface := a.tables[table]
		if surface == nil {
			surface = &TableSurface{Table: table}
			a.tables[table] = surface
		}
		surface.RequiresAnyWriteLock = true
		surface.WriteLockEvidence = append(surface.WriteLockEvidence, Evidence{
			File: a.relFile(pos), Line: pos.Line, Statement: normalized,
		})
	}
}

// resolveLoopTableSQL handles `X.field` SQL arguments where X is a range
// variable over a slice-of-struct literal declared with `:=` earlier in the
// SAME function body. It returns every constant string value `field` takes
// across the literal's elements. Deliberately conservative: bails out (ok
// =false) unless EVERY element is a struct composite literal and the field
// resolves to a compile-time constant string in EVERY element -- a partial
// resolution would be worse than none (it would silently under-report the
// unresolved elements as if they didn't exist).
func (a *analyzer) resolveLoopTableSQL(sqlArg ast.Expr, ctx funcCtx) ([]string, bool) {
	sel, ok := sqlArg.(*ast.SelectorExpr)
	if !ok {
		return nil, false
	}
	rangeVar, ok := sel.X.(*ast.Ident)
	if !ok {
		return nil, false
	}
	var lit *ast.CompositeLit
	ast.Inspect(ctx.decl.Body, func(n ast.Node) bool {
		if lit != nil {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		id, ok := assign.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		cl, ok := assign.Rhs[0].(*ast.CompositeLit)
		if !ok {
			return true
		}
		if _, isArr := cl.Type.(*ast.ArrayType); !isArr {
			return true
		}
		// Match by the RANGE variable, not directly: rangeVar in
		// `for _, step := range steps` is `step`, but the literal is
		// assigned to `steps`. Resolve by finding the RangeStmt whose Value
		// is rangeVar and whose X is this identifier.
		found := false
		ast.Inspect(ctx.decl.Body, func(rn ast.Node) bool {
			if found {
				return false
			}
			rs, ok := rn.(*ast.RangeStmt)
			if !ok {
				return true
			}
			valueIdent, ok := rs.Value.(*ast.Ident)
			if !ok || valueIdent.Name != rangeVar.Name {
				return true
			}
			xIdent, ok := rs.X.(*ast.Ident)
			if ok && xIdent.Name == id.Name {
				found = true
			}
			return true
		})
		if found {
			lit = cl
		}
		return true
	})
	if lit == nil {
		return nil, false
	}
	arrType, ok := lit.Type.(*ast.ArrayType)
	if !ok {
		return nil, false
	}
	structType, ok := arrType.Elt.(*ast.StructType)
	if !ok {
		return nil, false
	}
	fieldIdx := -1
	pos := 0
	for _, field := range structType.Fields.List {
		names := field.Names
		if len(names) == 0 {
			pos++
			continue
		}
		for _, fn := range names {
			if fn.Name == sel.Sel.Name {
				fieldIdx = pos
			}
			pos++
		}
	}
	if fieldIdx < 0 {
		return nil, false
	}
	var values []string
	for _, elt := range lit.Elts {
		var fieldExpr ast.Expr
		switch e := elt.(type) {
		case *ast.CompositeLit:
			if fieldIdx >= len(e.Elts) {
				return nil, false
			}
			candidate := e.Elts[fieldIdx]
			if kv, ok := candidate.(*ast.KeyValueExpr); ok {
				fieldExpr = kv.Value
			} else {
				fieldExpr = candidate
			}
		default:
			return nil, false
		}
		tv := ctx.info.Types[fieldExpr]
		if tv.Value == nil || tv.Value.Kind() != constant.String {
			return nil, false
		}
		values = append(values, constant.StringVal(tv.Value))
	}
	if len(values) == 0 {
		return nil, false
	}
	return values, true
}

func normalizeSQL(sql string) string {
	fields := strings.Fields(sql)
	joined := strings.Join(fields, " ")
	const maxLen = 160
	if len(joined) > maxLen {
		joined = joined[:maxLen] + "..."
	}
	return joined
}

// SortedTables returns the derived table names in alphabetical order.
func (d *DerivedSurface) SortedTables() []string {
	names := make([]string, 0, len(d.Tables))
	for t := range d.Tables {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}
