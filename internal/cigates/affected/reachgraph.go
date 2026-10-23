package affected

import (
	"fmt"
	"go/types"

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
	succ  [][]int32 // callees (static, dynamic, invoke) and address-taken functions
	mat   [][]int32 // types each function converts to an interface (type index)
	pkgID map[*ssa.Package]int32

	// The types reflection can reach: per type its exported methods and the types derived from it.
	typeMethods [][]int32
	typeKids    [][]int32
}

func newReachGraph(prog *ssa.Program, roots []*ssa.Function) *reachGraph {
	res := rta.Analyze(roots, true)
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
		g.mat = append(g.mat, nil)
		return id
	}
	for f := range res.Reachable {
		add(f)
	}
	for f, n := range res.CallGraph.Nodes {
		if f == nil {
			continue
		}
		from := add(f)
		for _, e := range n.Out {
			if e.Site == nil {
				continue // reflect.Value.Call's edges to every address-taken function: the walk's own rule instead
			}
			g.succ[from] = append(g.succ[from], add(e.Callee.Func))
		}
	}

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
		for _, t := range g.mat[id] {
			pushType(t)
		}
	}
	return seen
}
