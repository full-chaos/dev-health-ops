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

// UnresolvedTxSite is a SQL call issued through a pgx.Tx-typed PARAMETER
// (so it necessarily originates from some other function's `.Begin(ctx)`)
// whose origin buildTxOrigins could not resolve to a single unambiguous
// Begin() call site -- either because two different call chains reach this
// parameter with two different origins, or because a link in the chain
// couldn't be traced. This is the explicit "co-residency unverified" list:
// the evidence at this site still contributes to its table's privilege
// finding as normal, but its TxGroup fell back to coarse function-scoped
// grouping, so any transaction-consistency finding involving it should be
// treated as incomplete rather than a clean "no conflict" signal.
type UnresolvedTxSite struct {
	File     string
	Line     int
	Function string
}

// DerivedSurface is the full result of statically deriving the domain-pool
// query surface.
type DerivedSurface struct {
	Tables        map[string]*TableSurface
	Dynamic       []DynamicSite
	Unresolved    []UnresolvedCallSite
	SeedSites     []Evidence // where a *.Domain/.domainPool/.DomainPool() root was found, for auditing tool scope
	Devirtualized []DevirtualizedCallSite
	UnresolvedTx  []UnresolvedTxSite
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

	tables       map[string]*TableSurface
	dynamic      []DynamicSite
	unresolved   []UnresolvedCallSite
	seedSites    []Evidence
	seedSeen     map[string]bool
	unresolvedTx []UnresolvedTxSite

	// implementers maps an interface's Named type to every non-test,
	// in-module concrete Named type that implements it. Used only to
	// devirtualize a call through an interface when it has EXACTLY one
	// module-local implementer (see resolveDevirtualized) -- a common,
	// deliberately narrow heuristic, not a sound points-to analysis. See the
	// handoff README's known-limitations list.
	implementers map[*types.Named][]*types.Named
	devirt       []DevirtualizedCallSite

	// sqlParamConstants[fn][paramIndex] holds every distinct constant string
	// value that string parameter is EVER passed at any in-module call site
	// -- but ONLY when every observed call site passed a compile-time
	// constant for that position (one non-constant call site excludes the
	// whole (fn, paramIndex) pair, fail-safe). This is interprocedural
	// constant propagation for SQL text passed through a helper function
	// parameter (e.g. `deleteInChunks(ctx, pool, sqlLiteral, ...)` where the
	// SQL is a parameter, not inlined at the .Exec call) -- a real gap found
	// by re-running against feat/go-default-cutover's
	// internal/jobs/system/retention_postgres.go, which forwards its DELETE
	// statement through two such parameter hops before reaching pool.Exec.
	// Built once, independent of taint state (a pure syntactic property of
	// call sites), before the taint fixed-point loop.
	sqlParamConstants map[*types.Func]map[int][]string

	// txOriginParam[fn][paramIndex] holds the origin ID (a "file:line"
	// string naming the specific `.Begin(ctx)` call site) of a pgx.Tx-typed
	// parameter, when every path that reaches this parameter (across
	// however many function/type hops) traces back to the SAME Begin()
	// call, unambiguously. A parameter reached from two DIFFERENT Begin()
	// call sites, or from an unresolvable source, is excluded (not present
	// in the map) -- fail-closed, matching sqlParamConstants's discipline:
	// a verified list of "unknown" beats a guessed grouping. This is what
	// makes Evidence.TxGroup precise ACROSS function/type boundaries (e.g.
	// NativePostSyncService.Fanout opens one tx and hands it to up to five
	// different writer types' methods in different packages -- without
	// this, each write gets its own function-scoped TxGroup even though
	// they commit together). Built once, independent of taint state, same
	// fixed-point shape as buildSQLParamConstants.
	txOriginParam map[*types.Func]map[int]string
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
	a.buildSQLParamConstants()
	a.buildTxOrigins()

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
		a.unresolvedTx = nil
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
		UnresolvedTx:  dedupUnresolvedTx(a.unresolvedTx),
		rootModule:    moduleDir,
	}, nil
}

// dedupUnresolvedTx collapses duplicate (file, line) entries -- the same
// call site can be visited more than once across the fixed-point loop's
// final iteration in rare cases (e.g. reached via two different resolved
// callers), and only one report per site is useful.
func dedupUnresolvedTx(sites []UnresolvedTxSite) []UnresolvedTxSite {
	seen := map[string]bool{}
	var out []UnresolvedTxSite
	for _, s := range sites {
		key := fmt.Sprintf("%s:%d", s.File, s.Line)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, s)
	}
	return out
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

// simpleCalleeFunc resolves a call expression's callee to a *types.Func
// without devirtualization -- used by buildSQLParamConstants, which only
// needs to handle plain function/method calls (deleteInChunks/deleteOneChunk
// are not interface-dispatched); handleCall has its own, richer resolution
// (including devirtualization) kept separate to avoid entangling the two
// passes.
func simpleCalleeFunc(call *ast.CallExpr, info *types.Info) *types.Func {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		if obj, ok := info.Uses[fn].(*types.Func); ok {
			return obj
		}
	case *ast.SelectorExpr:
		if sel, ok := info.Selections[fn]; ok {
			if f, ok := sel.Obj().(*types.Func); ok {
				return f
			}
		} else if obj, ok := info.Uses[fn.Sel].(*types.Func); ok {
			return obj
		}
	}
	return nil
}

func isStringType(t types.Type) bool {
	b, ok := t.Underlying().(*types.Basic)
	return ok && b.Kind() == types.String
}

// buildSQLParamConstants finds every in-module function with a `string`
// parameter that EVERY call site (found anywhere in the module) passes
// either a compile-time constant, or the CALLING function's own
// already-resolved string parameter, for -- and records the set of values it
// can take. This is a small fixed-point loop (bounded, monotonic, same shape
// as the taint propagation loop but independent of it) because the
// motivating case is TWO hops deep:
// `DeleteBefore` passes a literal to `deleteInChunks(ctx, pool, statement
// string, ...)`, which forwards its OWN `statement` parameter (not a
// literal, from deleteInChunks's local perspective) to `deleteOneChunk`,
// which finally calls `tx.Exec(ctx, statement, ...)`. A single pass would
// resolve deleteInChunks's parameter but then see deleteOneChunk's call site
// (`statement`, a bare identifier, not a literal) and give up. See the
// analyzer struct field's doc comment for the concrete case that motivated
// this.
func (a *analyzer) buildSQLParamConstants() {
	a.sqlParamConstants = map[*types.Func]map[int][]string{}
	const maxIterations = 12
	for iter := 0; iter < maxIterations; iter++ {
		seen := map[*types.Func]map[int]map[string]bool{}
		failed := map[*types.Func]map[int]bool{}

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
				if _, ok := a.funcDecls[callee]; !ok {
					return true // need the callee's own param list; skip stdlib/third-party
				}
				sig, ok := callee.Type().(*types.Signature)
				if !ok {
					return true
				}
				for i, arg := range call.Args {
					if i >= sig.Params().Len() {
						break
					}
					if !isStringType(sig.Params().At(i).Type()) {
						continue
					}
					var values []string
					if tv := ctx.info.Types[arg]; tv.Value != nil && tv.Value.Kind() == constant.String {
						values = []string{constant.StringVal(tv.Value)}
					} else if id, ok := arg.(*ast.Ident); ok && currentFn != nil {
						if pIdx := paramIndex(ctx.decl, id.Name); pIdx >= 0 {
							values = a.sqlParamConstants[currentFn][pIdx] // nil if not yet resolved
						}
					}
					if values == nil {
						if failed[callee] == nil {
							failed[callee] = map[int]bool{}
						}
						failed[callee][i] = true
						continue
					}
					if seen[callee] == nil {
						seen[callee] = map[int]map[string]bool{}
					}
					if seen[callee][i] == nil {
						seen[callee][i] = map[string]bool{}
					}
					for _, v := range values {
						seen[callee][i][v] = true
					}
				}
				return true
			})
		}

		next := map[*types.Func]map[int][]string{}
		total := 0
		for fn, byIdx := range seen {
			for idx, values := range byIdx {
				if failed[fn][idx] {
					continue
				}
				list := make([]string, 0, len(values))
				for v := range values {
					list = append(list, v)
				}
				if next[fn] == nil {
					next[fn] = map[int][]string{}
				}
				next[fn][idx] = list
				total += len(list)
			}
		}
		prevTotal := 0
		for _, byIdx := range a.sqlParamConstants {
			for _, values := range byIdx {
				prevTotal += len(values)
			}
		}
		a.sqlParamConstants = next
		if total == prevTotal {
			break // fixed point: monotonic, so equal totals means no new facts
		}
	}
}

// paramIndex returns the positional index of a parameter named name in
// decl's parameter list, or -1 if not found. Accounts for grouped names
// (`a, b string`).
func paramIndex(decl *ast.FuncDecl, name string) int {
	if decl.Type.Params == nil {
		return -1
	}
	idx := 0
	for _, field := range decl.Type.Params.List {
		if len(field.Names) == 0 {
			idx++
			continue
		}
		for _, n := range field.Names {
			if n.Name == name {
				return idx
			}
			idx++
		}
	}
	return -1
}

// paramNameAt returns the name of the parameter at positional index idx in
// decl's parameter list, or "" if idx is out of range or the parameter is
// unnamed (`_` or bare type with no identifier).
func paramNameAt(decl *ast.FuncDecl, idx int) string {
	if decl.Type.Params == nil {
		return ""
	}
	pos := 0
	for _, field := range decl.Type.Params.List {
		if len(field.Names) == 0 {
			if pos == idx {
				return ""
			}
			pos++
			continue
		}
		for _, n := range field.Names {
			if pos == idx {
				return n.Name
			}
			pos++
		}
	}
	return ""
}

func isPgxTxType(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Name() == "Tx" && strings.HasSuffix(named.Obj().Pkg().Path(), "/pgx/v5")
}

// buildTxOrigins is the cross-function-boundary counterpart to
// buildSQLParamConstants, for pgx.Tx values instead of SQL string literals.
// See txOriginParam's doc comment on the analyzer struct for what this
// proves and why it's fail-closed rather than a best guess. Deliberately a
// SEPARATE pass from the main taint fixed-point loop (not folded into
// exprTainted/handleAssign): it only needs to track "which specific Begin()
// call did this pgx.Tx come from", a narrower and different question than
// "is this value pool-derived at all", and keeping it separate means it
// cannot perturb the existing, already-verified taint/SQL-extraction
// behavior -- a bug here can only degrade TxGroup precision (falling back
// to the coarse function-scoped grouping), never affect which tables/
// privileges are found.
func (a *analyzer) buildTxOrigins() {
	a.txOriginParam = map[*types.Func]map[int]string{}
	const maxIterations = 12
	for iter := 0; iter < maxIterations; iter++ {
		seen := map[*types.Func]map[int]string{}   // resolved, unambiguous origin so far this pass
		conflict := map[*types.Func]map[int]bool{} // saw >1 distinct origin, or an unresolvable source -- exclude

		record := func(fn *types.Func, idx int, origin string) {
			if conflict[fn] != nil && conflict[fn][idx] {
				return
			}
			if seen[fn] == nil {
				seen[fn] = map[int]string{}
			}
			if existing, ok := seen[fn][idx]; ok && existing != origin {
				if conflict[fn] == nil {
					conflict[fn] = map[int]bool{}
				}
				conflict[fn][idx] = true
				delete(seen[fn], idx)
				return
			}
			seen[fn][idx] = origin
		}
		markUnresolved := func(fn *types.Func, idx int) {
			if conflict[fn] == nil {
				conflict[fn] = map[int]bool{}
			}
			conflict[fn][idx] = true
			if seen[fn] != nil {
				delete(seen[fn], idx)
			}
		}

		for _, ctx := range a.funcDecls {
			var currentFn *types.Func
			if obj, ok := ctx.info.Defs[ctx.decl.Name]; ok {
				currentFn, _ = obj.(*types.Func)
			}
			// localOrigin seeds from this function's own already-resolved
			// tx parameters (from the previous iteration), then grows as we
			// walk the body and see `tx, err := X.Begin(ctx)`.
			localOrigin := map[string]string{}
			if currentFn != nil {
				for idx, origin := range a.txOriginParam[currentFn] {
					if name := paramNameAt(ctx.decl, idx); name != "" {
						localOrigin[name] = origin
					}
				}
			}
			ast.Inspect(ctx.decl.Body, func(n ast.Node) bool {
				if assign, ok := n.(*ast.AssignStmt); ok && len(assign.Rhs) == 1 {
					if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
						if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Begin" {
							if id, ok := assign.Lhs[0].(*ast.Ident); ok && id.Name != "_" {
								pos := a.fset.Position(call.Pos())
								localOrigin[id.Name] = fmt.Sprintf("%s:%d", a.relFile(pos), pos.Line)
							}
						}
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee := simpleCalleeFunc(call, ctx.info)
				if callee == nil {
					return true
				}
				if _, ok := a.funcDecls[callee]; !ok {
					return true
				}
				sig, ok := callee.Type().(*types.Signature)
				if !ok {
					return true
				}
				for i, arg := range call.Args {
					if i >= sig.Params().Len() {
						break
					}
					if !isPgxTxType(sig.Params().At(i).Type()) {
						continue
					}
					id, ok := arg.(*ast.Ident)
					if !ok {
						markUnresolved(callee, i)
						continue
					}
					origin, ok := localOrigin[id.Name]
					if !ok {
						markUnresolved(callee, i)
						continue
					}
					record(callee, i, origin)
				}
				return true
			})
		}

		next := map[*types.Func]map[int]string{}
		total := 0
		for fn, byIdx := range seen {
			for idx, origin := range byIdx {
				if conflict[fn][idx] {
					continue
				}
				if next[fn] == nil {
					next[fn] = map[int]string{}
				}
				next[fn][idx] = origin
				total++
			}
		}
		prevTotal := 0
		for _, byIdx := range a.txOriginParam {
			prevTotal += len(byIdx)
		}
		a.txOriginParam = next
		if total == prevTotal {
			break
		}
	}
}

// resolveParamConstantSQL handles a SQL argument that is a bare identifier
// referring to the CURRENT function's own string parameter -- e.g. inside
// `func deleteOneChunk(ctx, pool, statement string, ...) { ... pool.Exec(ctx,
// statement, ...) }`, `statement` isn't a compile-time constant from this
// function's own perspective, but buildSQLParamConstants already proved
// every call site passes one.
func (a *analyzer) resolveParamConstantSQL(sqlArg ast.Expr, ctx funcCtx) ([]string, bool) {
	ident, ok := sqlArg.(*ast.Ident)
	if !ok {
		return nil, false
	}
	fnObj, ok := ctx.info.Defs[ctx.decl.Name]
	if !ok {
		return nil, false
	}
	fn, ok := fnObj.(*types.Func)
	if !ok {
		return nil, false
	}
	idx := paramIndex(ctx.decl, ident.Name)
	if idx < 0 {
		return nil, false
	}
	values, ok := a.sqlParamConstants[fn][idx]
	return values, ok
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
		a.recordSQLSite(call, ctx, sel.X)
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

func (a *analyzer) recordSQLSite(call *ast.CallExpr, ctx funcCtx, recvExpr ast.Expr) {
	if len(call.Args) < 2 {
		return
	}
	sqlArg := call.Args[1]
	pos := a.fset.Position(call.Pos())
	txGroup := a.txGroupKey(ctx, recvExpr)

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

	// Fallback for SQL text forwarded through a helper function's own
	// string parameter (e.g. `deleteInChunks(ctx, pool, sqlLiteral, ...)`,
	// where the literal is constant at THIS call site but `deleteInChunks`
	// itself receives it as a plain `statement string` parameter and passes
	// THAT to .Exec two hops later) -- see buildSQLParamConstants's doc
	// comment; this is what recovers internal/jobs/system/retention_postgres.go's
	// provider_rate_limit_observations DELETE.
	if values, ok := a.resolveParamConstantSQL(sqlArg, ctx); ok {
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

// txGroupKey identifies the transaction a SQL call site's evidence belongs
// to. It tries the precise, cross-function answer first (recvExpr is a
// pgx.Tx-typed PARAMETER of the current function whose origin
// buildTxOrigins resolved unambiguously back to a specific `.Begin(ctx)`
// call site, however many function/type hops away) and falls back to the
// coarse same-function-body grouping when that isn't available (recvExpr
// isn't a parameter -- e.g. it's a tx declared via Begin() right here,
// which the fallback already groups correctly on its own -- or
// buildTxOrigins couldn't resolve it unambiguously). See
// Evidence.TxGroup's doc comment.
func (a *analyzer) txGroupKey(ctx funcCtx, recvExpr ast.Expr) string {
	if id, ok := recvExpr.(*ast.Ident); ok {
		if fnObj, ok := ctx.info.Defs[ctx.decl.Name]; ok {
			if fn, ok := fnObj.(*types.Func); ok {
				if idx := paramIndex(ctx.decl, id.Name); idx >= 0 {
					if origin, ok := a.txOriginParam[fn][idx]; ok {
						return "txorigin:" + origin
					}
					// recvExpr IS a pgx.Tx-typed parameter (it necessarily
					// crossed at least one function boundary to get here),
					// but buildTxOrigins could not resolve a single
					// unambiguous origin for it -- report this explicitly
					// rather than silently falling back to a function-scoped
					// grouping that LOOKS complete but isn't proven to be.
					if isPgxTxType(ctx.info.TypeOf(recvExpr)) {
						pos := a.fset.Position(recvExpr.Pos())
						a.unresolvedTx = append(a.unresolvedTx, UnresolvedTxSite{
							File: a.relFile(pos), Line: pos.Line,
							Function: txGroupFallbackName(ctx),
						})
					}
				}
			}
		}
	}
	return ctx.pkg.PkgPath + "." + txGroupFallbackName(ctx)
}

// txGroupFallbackName returns the "[Receiver.]FuncName" the coarse,
// same-function-body TxGroup fallback uses.
func txGroupFallbackName(ctx funcCtx) string {
	name := ctx.decl.Name.Name
	if ctx.decl.Recv != nil && len(ctx.decl.Recv.List) == 1 {
		if named := namedTypeOf(ctx.info.TypeOf(ctx.decl.Recv.List[0].Type)); named != nil {
			name = named.Obj().Name() + "." + name
		}
	}
	return name
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
