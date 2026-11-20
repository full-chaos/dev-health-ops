package providersync

import (
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// itemRouteAnalysis is the SSA census of item decode failures (CHAOS-14292). Bound: it follows the bare
// providerfoundation.ErrNormalizationInvalid, through assignments, phis, local bindings, forwarding helpers, fmt/errors-style
// wrappers and function-value aliases bound once in a package initializer, from a function that decodes (json.Unmarshal or
// (*json.Decoder).Decode) or takes `raw json.RawMessage` to every return of its callers. The only absorbers are the
// providerfoundation RouteFailure, ResponseFailure and ResponseInvalid constructors. Not pinned: interface-method and reflect
// dispatch, errors.Join or a custom wrapper type that hides the sentinel, a decoder passed as a value to another function,
// callers outside the package, and runtime behaviour (the item_route*_test.go cells pin that).
type itemRouteAnalysis struct {
	fset      *token.FileSet
	funcs     []*ssa.Function
	sum       map[*ssa.Function]*itemRouteSummary
	aliases   map[*ssa.Global]*ssa.Function
	decodes   map[*ssa.Function]bool
	failure   map[*ssa.Function]map[*ssa.BasicBlock]bool
	rawFirst  map[*ssa.Function]bool
	inR       map[*ssa.Function]bool
	errorType *types.Interface
	absorbed  map[string]bool
}

// itemRouteSummary of a function: whether it returns the sentinel with no decode behind it (plain), the sentinel of a decode
// failure (decode), and which parameters it forwards to its error result.
type itemRouteSummary struct {
	plain  bool
	decode bool
	fwd    uint64
}

// itemRouteTaint bits: plain = the sentinel from a function that does not decode; decode = the sentinel of a decode failure;
// routed = a RouteFailure/ResponseFailure built from a decode failure.
type itemRouteTaint struct {
	plain, decode, routed bool
	params                uint64
}

func (a itemRouteTaint) or(b itemRouteTaint) itemRouteTaint {
	return itemRouteTaint{a.plain || b.plain, a.decode || b.decode, a.routed || b.routed, a.params | b.params}
}

func (a itemRouteTaint) fromDecode() bool { return a.decode || a.routed }

// itemRouteFinding is a function of the analysed package that returns the bare sentinel on a decode path. Key is
// "file:Func" for a function that decodes inline, "file:Func>callee" for a call edge to such a function.
type itemRouteFinding struct {
	Key string
	Pos string
}

func loadItemRoutePackage(pattern string) (*packages.Package, error) {
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Dir:  "../..",
	}, pattern)
	if err != nil {
		return nil, err
	}
	if len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
		return nil, &itemRouteLoadError{pattern: pattern, pkgs: pkgs}
	}
	return pkgs[0], nil
}

type itemRouteLoadError struct {
	pattern string
	pkgs    []*packages.Package
}

func (e *itemRouteLoadError) Error() string {
	var parts []string
	for _, pkg := range e.pkgs {
		for _, err := range pkg.Errors {
			parts = append(parts, err.Error())
		}
	}
	return "item route census: load " + e.pattern + ": " + strings.Join(parts, "; ")
}

// absorbed keys ("file:Func") are functions whose bare error the analysis does not follow to their callers.
func newItemRouteAnalysis(pkg *packages.Package, absorbed map[string]bool) *itemRouteAnalysis {
	prog, ssaPkgs := ssautil.Packages([]*packages.Package{pkg}, ssa.InstantiateGenerics)
	ssaPkgs[0].Build()
	a := &itemRouteAnalysis{
		fset:      prog.Fset,
		sum:       map[*ssa.Function]*itemRouteSummary{},
		aliases:   map[*ssa.Global]*ssa.Function{},
		decodes:   map[*ssa.Function]bool{},
		failure:   map[*ssa.Function]map[*ssa.BasicBlock]bool{},
		rawFirst:  map[*ssa.Function]bool{},
		inR:       map[*ssa.Function]bool{},
		errorType: types.Universe.Lookup("error").Type().Underlying().(*types.Interface),
		absorbed:  absorbed,
	}
	visited := map[*ssa.Function]bool{}
	var walk func(fn *ssa.Function)
	walk = func(fn *ssa.Function) {
		if visited[fn] {
			return
		}
		visited[fn] = true
		if fn.Blocks == nil || strings.HasSuffix(a.fset.Position(fn.Pos()).Filename, "_test.go") {
			return
		}
		a.funcs = append(a.funcs, fn)
		for _, anon := range fn.AnonFuncs {
			walk(anon)
		}
	}
	for _, member := range ssaPkgs[0].Members {
		switch m := member.(type) {
		case *ssa.Function:
			walk(m)
		case *ssa.Type:
			for _, set := range []*types.MethodSet{prog.MethodSets.MethodSet(m.Type()), prog.MethodSets.MethodSet(types.NewPointer(m.Type()))} {
				for i := 0; i < set.Len(); i++ {
					if fn := prog.MethodValue(set.At(i)); fn != nil && fn.Pkg == ssaPkgs[0] && fn.Synthetic == "" {
						walk(fn)
					}
				}
			}
		}
	}
	if initFn := ssaPkgs[0].Func("init"); initFn != nil {
		walk(initFn)
	}
	a.collectAliases()
	for _, fn := range a.funcs {
		a.sum[fn] = &itemRouteSummary{}
		a.decodes[fn] = a.callsDecode(fn)
		a.failure[fn] = a.decodeFailureBlocks(fn)
		a.rawFirst[fn] = len(fn.Params) > 0 && fn.Params[0].Name() == "raw" && fn.Params[0].Type().String() == "encoding/json.RawMessage"
	}
	a.solve()
	return a
}

func (a *itemRouteAnalysis) collectAliases() {
	counts := map[*ssa.Global]int{}
	for _, fn := range a.funcs {
		for _, block := range fn.Blocks {
			for _, instr := range block.Instrs {
				store, ok := instr.(*ssa.Store)
				if !ok {
					continue
				}
				global, ok := store.Addr.(*ssa.Global)
				if !ok {
					continue
				}
				counts[global]++
				if target, ok := store.Val.(*ssa.Function); ok {
					a.aliases[global] = target
				}
			}
		}
	}
	for global := range a.aliases {
		if counts[global] != 1 {
			delete(a.aliases, global)
		}
	}
}

func isProviderFoundation(pkg *types.Package) bool {
	return pkg != nil && strings.HasSuffix(pkg.Path(), "/providerfoundation")
}

func (a *itemRouteAnalysis) callsDecode(fn *ssa.Function) bool {
	found := false
	forEachCall(fn, func(call *ssa.Call) { found = found || isDecodeCall(call) })
	return found
}

func isDecodeCall(call *ssa.Call) bool {
	callee := call.Common().StaticCallee()
	if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg.Path() != "encoding/json" {
		return false
	}
	if callee.Name() == "Unmarshal" {
		return true
	}
	recv := callee.Signature.Recv()
	return recv != nil && callee.Name() == "Decode" && strings.HasSuffix(recv.Type().String(), "encoding/json.Decoder")
}

// decodeFailureBlocks: the blocks that the failing edge of a comparison of a decode call's error (`err != nil`,
// `err == nil`, `err != io.EOF`) dominates; when the edge enters a join with another path (`err != nil || bad`) only that block counts, not what follows. A sentinel returned there is the decode failure going out.
func (a *itemRouteAnalysis) decodeFailureBlocks(fn *ssa.Function) map[*ssa.BasicBlock]bool {
	out := map[*ssa.BasicBlock]bool{}
	forEachCall(fn, func(call *ssa.Call) {
		if !isDecodeCall(call) {
			return
		}
		for _, ref := range *call.Referrers() {
			cmp, ok := ref.(*ssa.BinOp)
			if !ok || (cmp.Op != token.NEQ && cmp.Op != token.EQL) {
				continue
			}
			for _, user := range *cmp.Referrers() {
				branch, ok := user.(*ssa.If)
				if !ok || branch.Cond != cmp {
					continue
				}
				failing := branch.Block().Succs[0]
				if cmp.Op == token.EQL {
					failing = branch.Block().Succs[1]
				}
				if len(failing.Preds) != 1 {
					out[failing] = true
					continue
				}
				for _, block := range fn.Blocks {
					if failing.Dominates(block) {
						out[block] = true
					}
				}
			}
		}
	})
	return out
}

func forEachCall(fn *ssa.Function, visit func(*ssa.Call)) {
	for _, block := range fn.Blocks {
		for _, instr := range block.Instrs {
			if call, ok := instr.(*ssa.Call); ok {
				visit(call)
			}
		}
	}
}

// resolve is the callee of a call: a static callee, or a function-value global bound once to a function.
func (a *itemRouteAnalysis) resolve(call *ssa.CallCommon) *ssa.Function {
	if callee := call.StaticCallee(); callee != nil {
		return callee
	}
	if load, ok := call.Value.(*ssa.UnOp); ok {
		if global, ok := load.X.(*ssa.Global); ok {
			return a.aliases[global]
		}
	}
	return nil
}

func isRouteAbsorber(callee *ssa.Function) bool {
	if callee == nil || callee.Pkg == nil || !isProviderFoundation(callee.Pkg.Pkg) || callee.Signature.Recv() != nil {
		return false
	}
	switch callee.Name() {
	case "RouteFailure", "ResponseFailure", "ResponseInvalid":
		return true
	}
	return false
}

func (a *itemRouteAnalysis) isErrorish(t types.Type) bool {
	if t == nil {
		return false
	}
	if tuple, ok := t.(*types.Tuple); ok {
		for i := 0; i < tuple.Len(); i++ {
			if a.isErrorish(tuple.At(i).Type()) {
				return true
			}
		}
		return false
	}
	return types.Implements(t, a.errorType)
}

func (a *itemRouteAnalysis) taint(fn *ssa.Function, v ssa.Value, seen map[ssa.Value]bool) itemRouteTaint {
	if seen[v] {
		return itemRouteTaint{}
	}
	seen[v] = true
	switch v := v.(type) {
	case *ssa.Parameter:
		for i, param := range fn.Params {
			if param == v && i < 64 {
				return itemRouteTaint{params: 1 << uint(i)}
			}
		}
	case *ssa.UnOp:
		if v.Op != token.MUL {
			return itemRouteTaint{}
		}
		switch addr := v.X.(type) {
		case *ssa.Global:
			if addr.Name() == "ErrNormalizationInvalid" && isProviderFoundation(addr.Pkg.Pkg) {
				if a.rawFirst[fn] || a.failure[fn][v.Block()] {
					return itemRouteTaint{decode: true}
				}
				return itemRouteTaint{plain: true}
			}
		case *ssa.Alloc:
			var out itemRouteTaint
			for _, ref := range *addr.Referrers() {
				if store, ok := ref.(*ssa.Store); ok && store.Addr == addr {
					out = out.or(a.taint(fn, store.Val, seen))
				}
			}
			return out
		}
	case *ssa.Phi:
		var out itemRouteTaint
		for _, edge := range v.Edges {
			out = out.or(a.taint(fn, edge, seen))
		}
		return out
	case *ssa.MakeInterface:
		return a.taint(fn, v.X, seen)
	case *ssa.ChangeInterface:
		return a.taint(fn, v.X, seen)
	case *ssa.ChangeType:
		return a.taint(fn, v.X, seen)
	case *ssa.TypeAssert:
		return a.taint(fn, v.X, seen)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok && a.isErrorish(v.Type()) {
			return a.callTaint(fn, call, seen)
		}
	case *ssa.Call:
		if a.isErrorish(v.Type()) {
			return a.callTaint(fn, v, seen)
		}
	}
	return itemRouteTaint{}
}

// callTaint: a summarised callee returns the sentinel itself and forwards the parameters its summary lists; an absorber
// returns a routed error when its cause came from a decode; a callee that returns the sentinel from an argument that came
// from a decode converts the decode failure back; any other callee (a library wrapper, a dynamic call) is taken to forward
// every argument.
func (a *itemRouteAnalysis) callTaint(fn *ssa.Function, call *ssa.Call, seen map[ssa.Value]bool) itemRouteTaint {
	callee := a.resolve(call.Common())
	if callee != nil && a.absorbed[a.file(callee)+":"+a.name(callee)] {
		return itemRouteTaint{}
	}
	args := call.Common().Args
	var argTaint itemRouteTaint
	argTaints := make([]itemRouteTaint, len(args))
	for i, arg := range args {
		argTaints[i] = a.taint(fn, arg, seen)
		argTaint = argTaint.or(argTaints[i])
	}
	if isRouteAbsorber(callee) {
		if argTaint.fromDecode() {
			return itemRouteTaint{routed: true}
		}
		return itemRouteTaint{}
	}
	s := a.sum[callee]
	if callee == nil || s == nil {
		return argTaint
	}
	out := itemRouteTaint{plain: s.plain, decode: s.decode}
	if (s.plain || s.decode) && argTaint.fromDecode() {
		out.decode = true
	}
	for i := range args {
		if i < 64 && s.fwd&(1<<uint(i)) != 0 {
			out = out.or(argTaints[i])
		}
	}
	return out
}

func (a *itemRouteAnalysis) returnTaint(fn *ssa.Function) itemRouteTaint {
	var out itemRouteTaint
	for _, block := range fn.Blocks {
		ret, ok := block.Instrs[len(block.Instrs)-1].(*ssa.Return)
		if !ok {
			continue
		}
		for _, result := range ret.Results {
			if a.isErrorish(result.Type()) {
				out = out.or(a.taint(fn, result, map[ssa.Value]bool{}))
			}
		}
	}
	return out
}

func (a *itemRouteAnalysis) solve() {
	for changed := true; changed; {
		changed = false
		for _, fn := range a.funcs {
			t := a.returnTaint(fn)
			s := a.sum[fn]
			if (t.plain && !s.plain) || (t.decode && !s.decode) || t.params&^s.fwd != 0 {
				s.plain, s.decode, s.fwd = s.plain || t.plain, s.decode || t.decode, s.fwd|t.params
				changed = true
			}
		}
	}
	for _, fn := range a.funcs {
		a.inR[fn] = a.sum[fn].decode
	}
}

func (a *itemRouteAnalysis) rCallees(fn *ssa.Function) []*ssa.Function {
	var out []*ssa.Function
	forEachCall(fn, func(call *ssa.Call) {
		if callee := a.resolve(call.Common()); callee != nil && a.inR[callee] {
			out = append(out, callee)
		}
	})
	return out
}

func (a *itemRouteAnalysis) top(fn *ssa.Function) *ssa.Function {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	return fn
}

func (a *itemRouteAnalysis) name(fn *ssa.Function) string {
	fn = a.top(fn)
	if recv := fn.Signature.Recv(); recv != nil {
		t := recv.Type()
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
		}
		if named, ok := t.(*types.Named); ok {
			return named.Obj().Name() + "." + fn.Name()
		}
	}
	return fn.Name()
}

func (a *itemRouteAnalysis) file(fn *ssa.Function) string {
	return filepath.Base(a.fset.Position(a.top(fn).Pos()).Filename)
}

// decoders: the item decoders (first parameter `raw json.RawMessage`) that return the bare sentinel.
func (a *itemRouteAnalysis) decoders() map[string]bool {
	out := map[string]bool{}
	for _, fn := range a.funcs {
		if a.inR[fn] && a.rawFirst[fn] && fn.Parent() == nil && fn.Signature.Recv() == nil {
			out[fn.Name()] = true
		}
	}
	return out
}

// edges counts the call edges into an item decoder.
func (a *itemRouteAnalysis) edges() int {
	n := 0
	decoders := a.decoders()
	for _, fn := range a.funcs {
		forEachCall(fn, func(call *ssa.Call) {
			if callee := a.resolve(call.Common()); callee != nil && callee.Parent() == nil && decoders[callee.Name()] {
				n++
			}
		})
	}
	return n
}

// findings: every function in R that is not an item decoder by signature. An inline decode (the function decodes itself)
// is keyed "file:Func"; a function that returns what a callee in R returns is keyed "file:Func>callee" per call edge.
func (a *itemRouteAnalysis) findings() []itemRouteFinding {
	seen := map[string]bool{}
	var out []itemRouteFinding
	add := func(key string, pos token.Pos) {
		if !seen[key] {
			seen[key] = true
			out = append(out, itemRouteFinding{Key: key, Pos: a.fset.Position(pos).String()})
		}
	}
	for _, fn := range a.funcs {
		isInit := a.top(fn).Name() == "init"
		if (!a.inR[fn] && !isInit) || a.rawFirst[fn] {
			continue
		}
		file := a.file(fn)
		if a.decodes[fn] {
			add(file+":"+a.name(fn), fn.Pos())
		}
		forEachCall(fn, func(call *ssa.Call) {
			callee := a.resolve(call.Common())
			if callee == nil || !a.inR[callee] {
				return
			}
			if isInit {
				add(filepath.Base(a.fset.Position(call.Pos()).Filename)+":init>"+a.name(callee), call.Pos())
				return
			}
			add(file+":"+a.name(fn)+">"+a.name(callee), call.Pos())
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
