package affected

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const groupsLib = `package lib

type Shape interface {
	Area() int
	name() string
}

type Sq struct{ N int }

func (s Sq) Area() int    { return s.N * s.N }
func (Sq) name() string   { return "sq" }

type Rect struct{ W, H int }

func (r *Rect) Area() int  { return r.W * r.H }
func (*Rect) name() string { return "rect" }

type Embed struct{ Sq }

type Unused struct{}

func (Unused) Area() int    { return 0 }
func (Unused) name() string { return "" }

func Total(ss []Shape) int {
	t := 0
	for _, s := range ss {
		t += s.Area() + len(s.name())
	}
	return t
}

type Op func(int, int) int

func Add(a, b int) int { return a + b }
func Mul(a, b int) int { return a * b }
func Sub(a, b int) int { return a - b }

func Apply(op Op, a, b int) int { return op(a, b) }

type Box[T any] struct{ v T }

func (b Box[T]) Get() T { return b.v }

type Getter[T any] interface{ Get() T }

func Use[T any](g Getter[T]) T { return g.Get() }
`

const groupsMain = `package main

import (
	"fmt"
	"os"

	"m/lib"
)

type stringer struct{}

func (stringer) String() string { return "x" }

func main() {
	ss := []lib.Shape{lib.Sq{N: 2}, &lib.Rect{W: 1, H: 3}, lib.Embed{}}
	f := lib.Add
	if len(os.Args) > 5 {
		f = lib.Mul
	}
	k := len(os.Args)
	closure := func(a, b int) int { return a + b + k }
	area := (&lib.Rect{}).Area
	var s fmt.Stringer = stringer{}
	fmt.Println(lib.Total(ss), lib.Apply(f, 1, 2), lib.Apply(closure, 3, 4), lib.Use[int](lib.Box[int]{}), s.String(), area(), lib.Sub(1, 2))
}
`

// CHAOS-12407: the reach graph keeps RTA's dynamic and invoke edges as groups, not RTA's call graph. On a program with
// interface invokes (exported and unexported methods, a pointer receiver, a promoted method, a generic interface) and
// function-value calls (functions, a closure, a method value), plus fmt and os from the standard library, the group of
// every dynamic and invoke call site holds exactly the callees of that site in rta.Analyze(roots, true).CallGraph.
func TestReachGroupsEqualRTACallGraphEdges(t *testing.T) {
	dir := t.TempDir()
	for name, src := range map[string]string{"go.mod": "module m\n\ngo 1.22\n", "lib/lib.go": groupsLib, "app/main.go": groupsMain} {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &packages.Config{Mode: packages.LoadAllSyntax, Dir: dir, Env: append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off", "GOTOOLCHAIN=local")}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("the fixture module does not type-check")
	}
	prog, spkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	var roots []*ssa.Function
	for _, p := range spkgs {
		if p != nil && p.Pkg.Name() == "main" {
			roots = append(roots, p.Func("main"), p.Func("init"))
		}
	}
	if len(roots) != 2 {
		t.Fatalf("roots %v", roots)
	}

	sites := map[ssa.CallInstruction]int32{}
	g := reachGraphOf(prog, rta.Analyze(roots, false), func(c ssa.CallInstruction, grp int32) { sites[c] = grp })

	want := map[ssa.CallInstruction]map[*ssa.Function]bool{}
	for _, n := range rta.Analyze(roots, true).CallGraph.Nodes {
		for _, e := range n.Out {
			if e.Site == nil || e.Site.Common().StaticCallee() != nil {
				continue
			}
			if want[e.Site] == nil {
				want[e.Site] = map[*ssa.Function]bool{}
			}
			want[e.Site][e.Callee.Func] = true
		}
	}

	names := func(fns map[*ssa.Function]bool) []string {
		var out []string
		for f := range fns {
			out = append(out, f.String())
		}
		slices.Sort(out)
		return out
	}
	invokes, dynamic, multi := 0, 0, 0
	for site := range want {
		if _, ok := sites[site]; !ok {
			t.Errorf("%s (%s): RTA has callees %v, the reach graph records no group for the site", site, prog.Fset.Position(site.Pos()), names(want[site]))
		}
	}
	for site, grp := range sites {
		got := map[*ssa.Function]bool{}
		for _, id := range g.groups[grp] {
			got[g.fns[id]] = true
		}
		var missing, extra []string
		for f := range want[site] {
			if !got[f] {
				missing = append(missing, f.String())
			}
		}
		for f := range got {
			if !want[site][f] {
				extra = append(extra, f.String())
			}
		}
		if len(missing) > 0 || len(extra) > 0 {
			slices.Sort(missing)
			slices.Sort(extra)
			t.Errorf("%s in %s (%s): group callees differ from RTA's call graph: missing %v, extra %v", site, site.Parent(), prog.Fset.Position(site.Pos()), missing, extra)
		}
		if site.Common().IsInvoke() {
			invokes++
		} else {
			dynamic++
		}
		if len(want[site]) > 1 {
			multi++
		}
	}
	if invokes == 0 || dynamic == 0 || multi == 0 {
		t.Fatalf("the fixture must hold invoke and dynamic sites with several callees: %d invoke, %d dynamic, %d with several callees", invokes, dynamic, multi)
	}
	t.Logf("%d invoke and %d dynamic call sites, %d with several callees", invokes, dynamic, multi)
}
