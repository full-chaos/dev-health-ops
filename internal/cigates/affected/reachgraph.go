package affected

import (
	"fmt"
	"go/types"
	"hash/crc32"

	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/types/typeutil"
)

// reachGraph answers, for many programs of one SSA build, which functions each program can run. Rapid Type Analysis
// from every program's roots at once gives a call graph whose dynamic edges (interface invokes, function values) are
// those of the union program: each program's own RTA call graph is a subgraph of it, since RTA only adds edges as
// code is added. A program then walks the graph from its own roots, never entering a function of a package it is not
// built from, and applies RTA's reflection rules to the functions it reaches: a function whose address is taken there
// is reachable (reflect.Value.Call can call it), and so is every exported method of every type converted to an
// interface there and of every type reflection can derive from it. The walk reaches a superset of what the program's
// own RTA reaches: sound, without one RTA per program.
type reachGraph struct {
	id    map[*ssa.Function]int32
	fns   []*ssa.Function
	pkg   []int32   // package index of each function, -1 for a synthetic function of no package (wrappers, thunks)
	succ  [][]int32 // static callees and address-taken functions
	calls [][]int32 // groups of dynamic and invoke callees each function calls
	mat   [][]int32 // types each function converts to an interface (type index)
	pkgID map[*ssa.Package]int32

	// The callees of each group of addDynamicCalls.
	groups [][]int32

	// The types reflection can reach: per type its exported methods and the types derived from it.
	typeMethods [][]int32
	typeKids    [][]int32
}

func newReachGraph(prog *ssa.Program, roots []*ssa.Function) *reachGraph {
	return reachGraphOf(prog, rta.Analyze(roots, false), nil)
}

// reachGraphOf is the reach graph of an RTA result; onSite, when not nil, sees the group of each dynamic and invoke call.
func reachGraphOf(prog *ssa.Program, res *rta.Result, onSite func(ssa.CallInstruction, int32)) *reachGraph {
	g := &reachGraph{id: map[*ssa.Function]int32{}, pkgID: map[*ssa.Package]int32{}}
	add := func(f *ssa.Function) int32 {
		if id, ok := g.id[f]; ok {
			return id
		}
		id := int32(len(g.fns))
		g.id[f] = id
		g.fns = append(g.fns, f)
		p := int32(-1)
		if f.Pkg != nil {
			var ok bool
			if p, ok = g.pkgID[f.Pkg]; !ok {
				p = int32(len(g.pkgID))
				g.pkgID[f.Pkg] = p
			}
		}
		g.pkg = append(g.pkg, p)
		g.succ = append(g.succ, nil)
		g.calls = append(g.calls, nil)
		g.mat = append(g.mat, nil)
		return id
	}
	for f := range res.Reachable {
		add(f)
	}
	g.addDynamicCalls(prog, res, add, onSite)

	// typeOf indexes a type and, once, its exported methods and the types derived from it, as RTA's runtime types
	// (golang.org/x/tools/internal/typesinternal.ForEachElement, BSD-3-Clause, The Go Authors).
	var typeIdx typeutil.Map
	var pending []types.Type
	typeOf := func(T types.Type) int32 {
		if v := typeIdx.At(T); v != nil {
			return v.(int32)
		}
		i := int32(len(g.typeMethods))
		typeIdx.Set(T, i)
		g.typeMethods = append(g.typeMethods, nil)
		g.typeKids = append(g.typeKids, nil)
		pending = append(pending, T)
		return i
	}
	expand := func(T types.Type) {
		i := typeIdx.At(T).(int32)
		kid := func(K types.Type) {
			k := typeOf(K)
			g.typeKids[i] = append(g.typeKids[i], k)
		}
		for sel := range prog.MethodSets.MethodSet(T).Methods() {
			sig := sel.Type().(*types.Signature)
			if sig.TypeParams() != nil {
				continue
			}
			kid(sig.Params())
			kid(sig.Results())
			if types.IsInterface(T) || !sel.Obj().Exported() {
				continue
			}
			if m := prog.MethodValue(sel); m != nil {
				g.typeMethods[i] = append(g.typeMethods[i], add(m))
			}
		}
		switch T := T.(type) {
		case *types.Alias:
			kid(types.Unalias(T))
		case *types.Pointer:
			kid(T.Elem())
		case *types.Slice:
			kid(T.Elem())
		case *types.Chan:
			kid(T.Elem())
		case *types.Map:
			kid(T.Key())
			kid(T.Elem())
		case *types.Signature:
			kid(T.Params())
			kid(T.Results())
		case *types.Named:
			kid(types.NewPointer(T))
			kid(T.Underlying())
		case *types.Array:
			kid(T.Elem())
		case *types.Struct:
			for f := range T.Fields() {
				kid(f.Type())
			}
		case *types.Tuple:
			for v := range T.Variables() {
				kid(v.Type())
			}
		}
	}

	// Static callees, address-taken functions and materialized types of every function in the graph, including the
	// methods the types add.
	var space [32]*ssa.Value
	for i := 0; i < len(g.fns) || len(pending) > 0; {
		if len(pending) > 0 {
			T := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			expand(T)
			continue
		}
		f, id := g.fns[i], int32(i)
		i++
		for _, b := range f.Blocks {
			for _, instr := range b.Instrs {
				rands := instr.Operands(space[:0])
				switch instr := instr.(type) {
				case ssa.CallInstruction:
					if callee := instr.Common().StaticCallee(); callee != nil {
						g.succ[id] = append(g.succ[id], add(callee))
					}
					rands = rands[1:] // the call-position operand is not an address-taken function
				case *ssa.MakeInterface:
					g.mat[id] = append(g.mat[id], typeOf(instr.X.Type()))
				}
				for _, op := range rands {
					if fn, ok := (*op).(*ssa.Function); ok {
						g.succ[id] = append(g.succ[id], add(fn))
					}
				}
			}
		}
	}
	return g
}

// addDynamicCalls adds RTA's call graph edges of the dynamic calls and the invokes of the reachable functions, as
// groups. RTA joins every dynamic call to every address-taken function of an identical signature, and every invoke
// of an interface method to that method of every runtime type that implements the interface, so the callees of a call
// depend only on its signature or its interface method: a function names the groups it calls, and the walk expands a
// group once, instead of one edge per call site and callee (tens of millions for error.Error and io.Writer).
func (g *reachGraph) addDynamicCalls(prog *ssa.Program, res *rta.Result, add func(*ssa.Function) int32, onSite func(ssa.CallInstruction, int32)) {
	type method struct {
		pkg  *types.Package
		name string
		grp  int32
	}
	type iface struct {
		I       *types.Interface
		methods map[string]*method
	}
	var bySig, byIface typeutil.Map
	var ifaces []*iface
	group := func() int32 {
		g.groups = append(g.groups, nil)
		return int32(len(g.groups) - 1)
	}
	sigGroup := func(S types.Type) int32 {
		if v := bySig.At(S); v != nil {
			return v.(int32)
		}
		i := group()
		bySig.Set(S, i)
		return i
	}
	addrTaken := map[*ssa.Function]bool{}
	var space [32]*ssa.Value
	for f := range res.Reachable {
		id := g.id[f]
		calls := map[int32]bool{}
		for _, b := range f.Blocks {
			for _, instr := range b.Instrs {
				rands := instr.Operands(space[:0])
				if call, ok := instr.(ssa.CallInstruction); ok {
					c := call.Common()
					switch _, builtin := c.Value.(*ssa.Builtin); {
					case c.IsInvoke():
						I := c.Value.Type().Underlying().(*types.Interface)
						var in *iface
						if v := byIface.At(I); v != nil {
							in = ifaces[v.(int)]
						} else {
							in = &iface{I: I, methods: map[string]*method{}}
							byIface.Set(I, len(ifaces))
							ifaces = append(ifaces, in)
						}
						m := in.methods[c.Method.Id()]
						if m == nil {
							m = &method{pkg: c.Method.Pkg(), name: c.Method.Name(), grp: group()}
							in.methods[c.Method.Id()] = m
						}
						calls[m.grp] = true
						if onSite != nil {
							onSite(call, m.grp)
						}
					case c.StaticCallee() == nil && !builtin:
						grp := sigGroup(c.Signature())
						calls[grp] = true
						if onSite != nil {
							onSite(call, grp)
						}
					}
					rands = rands[1:]
				}
				for _, op := range rands {
					if fn, ok := (*op).(*ssa.Function); ok && !addrTaken[fn] {
						addrTaken[fn] = true
						i := sigGroup(fn.Signature)
						g.groups[i] = append(g.groups[i], add(fn))
					}
				}
			}
		}
		for c := range calls {
			g.calls[id] = append(g.calls[id], c)
		}
	}

	// The runtime types RTA found are its concrete types (and the interfaces among them); each implements the
	// interfaces it satisfies.
	type concrete struct {
		C      types.Type
		fprint uint64
	}
	var concretes []concrete
	res.RuntimeTypes.Iterate(func(T types.Type, _ any) {
		if !types.IsInterface(T) {
			concretes = append(concretes, concrete{T, fingerprint(prog.MethodSets.MethodSet(T))})
		}
	})
	for _, in := range ifaces {
		fp := fingerprint(prog.MethodSets.MethodSet(in.I))
		for _, c := range concretes {
			if fp&^c.fprint != 0 || !types.Implements(c.C, in.I) {
				continue
			}
			for _, m := range in.methods {
				g.groups[m.grp] = append(g.groups[m.grp], add(prog.LookupMethod(c.C, m.pkg, m.name)))
			}
		}
	}
}

// fingerprint is a bitmask with one bit per method id, so that a type lacking a method of an interface is rejected
// before types.Implements (golang.org/x/tools/go/callgraph/rta, BSD-3-Clause, The Go Authors).
func fingerprint(mset *types.MethodSet) uint64 {
	var space [64]byte
	var mask uint64
	for method := range mset.Methods() {
		method := method.Obj()
		sig := method.Type().(*types.Signature)
		if sig.TypeParams() != nil {
			continue
		}
		sum := crc32.ChecksumIEEE(fmt.Appendf(space[:], "%s/%d/%d", method.Id(), sig.Params().Len(), sig.Results().Len()))
		mask |= 1 << (sum % 64)
	}
	return mask
}

// reach is the functions a program runs: the walk from its roots inside its packages.
func (g *reachGraph) reach(roots []*ssa.Function, pkgs map[*ssa.Package]bool) []bool {
	in := make([]bool, len(g.pkgID))
	for p := range pkgs {
		if i, ok := g.pkgID[p]; ok {
			in[i] = true
		}
	}
	seen := make([]bool, len(g.fns))
	typeSeen := make([]bool, len(g.typeMethods))
	groupSeen := make([]bool, len(g.groups))
	var stack, typeStack []int32
	push := func(id int32) {
		if seen[id] {
			return
		}
		if p := g.pkg[id]; p >= 0 && !in[p] {
			return
		}
		seen[id] = true
		stack = append(stack, id)
	}
	pushType := func(t int32) {
		if !typeSeen[t] {
			typeSeen[t] = true
			typeStack = append(typeStack, t)
		}
	}
	for _, r := range roots {
		id, ok := g.id[r]
		if !ok {
			panic(fmt.Sprintf("reach: root %s is not in the graph", r))
		}
		push(id)
	}
	for len(stack) > 0 || len(typeStack) > 0 {
		if n := len(typeStack); n > 0 {
			t := typeStack[n-1]
			typeStack = typeStack[:n-1]
			for _, m := range g.typeMethods[t] {
				push(m)
			}
			for _, k := range g.typeKids[t] {
				pushType(k)
			}
			continue
		}
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, s := range g.succ[id] {
			push(s)
		}
		for _, c := range g.calls[id] {
			if !groupSeen[c] {
				groupSeen[c] = true
				for _, s := range g.groups[c] {
					push(s)
				}
			}
		}
		for _, t := range g.mat[id] {
			pushType(t)
		}
	}
	return seen
}
