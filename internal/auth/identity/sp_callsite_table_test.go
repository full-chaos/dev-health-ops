package identity

import (
	"strings"
	"testing"
)

const (
	spOtherPath    = "../other.go"
	spDelegatePath = "../identity/social/service.go"
)

const spPreamble = `package p

type Q struct {
	ID              int
	ServiceProvider bool
}
type T struct {
	ServiceProvider bool
	P               Q
}
type S struct{}

func (S) IdentityBySubject(int) (Q, bool, error) { return Q{}, false, nil }

type Store interface {
	IdentityBySubject(int) (Q, bool, error)
}

func signInTo(p Q) error { return nil }
func sink(Q)             {}
func sink3(Q, bool, error) {}
func sinkF(func(int) (Q, bool, error)) {}
func sinkL(func() (Q, bool, error)) {}
`

const spDelegateDecl = "func (s S) lookup() (Q, bool, error) { return s.IdentityBySubject(1) }\n"

type spCell struct {
	name     string
	path     string
	src      string
	wantGaps bool
}

func spCells(name, body string, wantGaps, delegate bool) []spCell {
	wrap := func(call string) string {
		b := strings.ReplaceAll(body, "LOOKUP", call)
		if strings.HasPrefix(b, "func ") {
			return b
		}
		return "func f(s S) {\n" + b + "\n}\n"
	}
	cells := []spCell{{"direct/" + name, spOtherPath, spPreamble + wrap("s.IdentityBySubject(1)"), wantGaps}}
	if delegate {
		cells = append(cells, spCell{"delegate/" + name, spDelegatePath, spPreamble + spDelegateDecl + wrap("s.lookup()"), wantGaps})
	}
	return cells
}

var spChecks = []struct {
	name string
	mk   func(v string) string
}{
	{"selector", func(v string) string { return "_ = " + v + ".ServiceProvider" }},
	{"struct key", func(v string) string { return "_ = T{ServiceProvider: " + v + ".ServiceProvider}" }},
	{"signInTo", func(v string) string { return "_ = signInTo(" + v + ")" }},
}

func spTable() []spCell {
	var cells []spCell
	for _, c := range spChecks {
		cells = append(cells, spCells("unrelated "+c.name, "p, _, _ := LOOKUP\n_ = p.ID\no := Q{}\n"+c.mk("o"), true, true)...)
		cells = append(cells, spCells("nested "+c.name+" credits the outer call", "p, _, _ := LOOKUP\nfunc() { "+c.mk("p")+" }()", true, true)...)
		cells = append(cells, spCells("call in nested func, outer "+c.name+" credits it", "p0 := Q{}\nfunc() {\np, _, _ := LOOKUP\n_ = p.ID\n}()\n"+c.mk("p0"), true, true)...)
	}
	cells = append(cells, spCells("two lookups, first result checked only", "p, _, _ := LOOKUP\nq, _, _ := LOOKUP\n_ = signInTo(p)\n_ = q.ID", true, true)...)

	cells = append(cells,
		spCells("method value assigned then called", "lookup := s.IdentityBySubject\np, _, _ := lookup(1)\n_ = signInTo(p)", true, false)...)
	cells = append(cells,
		spCells("method expression on an interface assigned then called", "lookup := Store.IdentityBySubject\np, _, _ := lookup(s, 1)\n_ = signInTo(p)", true, false)...)
	cells = append(cells,
		spCells("method expression on the concrete type assigned then called", "lookup := S.IdentityBySubject\np, _, _ := lookup(s, 1)\n_ = signInTo(p)", true, false)...)
	cells = append(cells,
		spCells("method expression called directly", "p, _, _ := S.IdentityBySubject(s, 1)\n_ = signInTo(p)", true, false)...)
	cells = append(cells,
		spCells("method value handed to a function", "sinkF(s.IdentityBySubject)", true, false)...)
	cells = append(cells, spCell{"delegate/method value assigned then called", spDelegatePath,
		spPreamble + spDelegateDecl + "func f(s S) {\nlookup := s.lookup\np, _, _ := lookup()\n_ = signInTo(p)\n}\n", true})
	cells = append(cells, spCell{"delegate/method value handed to a function", spDelegatePath,
		spPreamble + spDelegateDecl + "func f(s S) {\nsinkL(s.lookup)\n}\n", true})

	credit := "p, _, _ := LOOKUP\n_ = p.ServiceProvider\n"
	cells = append(cells, spCells("escapes to another func", credit+"sink(p)", true, true)...)
	cells = append(cells, spCells("stored in a struct", credit+"_ = T{P: p}", true, true)...)
	cells = append(cells, spCells("stored in a map", "m := map[int]Q{}\n"+credit+"m[1] = p", true, true)...)
	cells = append(cells, spCells("stored in a slice", credit+"_ = []Q{p}", true, true)...)
	cells = append(cells, spCells("sent on a channel", "ch := make(chan Q, 1)\n"+credit+"ch <- p", true, true)...)
	cells = append(cells, spCells("returned", "func f(s S) Q {\n"+credit+"return p\n}", true, true)...)
	cells = append(cells, spCells("copied to another variable", credit+"q := p\n_ = q.ServiceProvider", true, true)...)
	cells = append(cells, spCells("address taken", credit+"_ = &p", true, true)...)
	cells = append(cells, spCells("held in the blank identifier", credit+"_ = p", true, true)...)
	cells = append(cells, spCells("overwritten after the check", credit+"p = Q{}", true, true)...)
	cells = append(cells, spCells("assigned again by a second lookup", "p, _, _ := LOOKUP\n_ = signInTo(p)\np, _, _ = LOOKUP", true, true)...)
	cells = append(cells, spCells("field of the result written", credit+"p.ServiceProvider = false", true, true)...)
	cells = append(cells, spCells("shadowed by an inner variable", "p, _, _ := LOOKUP\n{\np := Q{}\n_ = p.ServiceProvider\n}\n_ = p.ID", true, true)...)
	cells = append(cells, spCells("captured by a func literal", credit+"func() { _ = p.ID }()", true, true)...)
	cells = append(cells, spCells("discarded with the blank identifier", "_, _, _ = LOOKUP", true, true)...)
	cells = append(cells, spCells("discarded in a short declaration", "_, found, _ := LOOKUP\n_ = found", true, true)...)
	cells = append(cells, spCells("discarded as a statement", "LOOKUP", true, true)...)
	cells = append(cells, spCells("returned directly", "func f(s S) (Q, bool, error) {\nreturn LOOKUP\n}", true, true)...)
	cells = append(cells, spCells("forwarded as call arguments", "sink3(LOOKUP)", true, true)...)
	cells = append(cells, spCells("deferred", "defer LOOKUP", true, true)...)
	cells = append(cells, spCells("started as a goroutine", "go LOOKUP", true, true)...)
	cells = append(cells, spCells("stored in a struct field", "var t T\nt.P, _, _ = LOOKUP\n_ = t.P.ServiceProvider", true, true)...)
	cells = append(cells, spCells("stored in a map entry", "m := map[int]Q{}\nm[1], _, _ = LOOKUP\n_ = m[1].ServiceProvider", true, true)...)
	cells = append(cells, spCells("assigned to an existing variable", "var p Q\np, _, _ = LOOKUP\n_ = p.ServiceProvider", true, true)...)
	cells = append(cells, spCells("assigned inside a func literal to an outer variable", "var p Q\nfunc() {\np, _, _ = LOOKUP\n}()\n_ = p.ServiceProvider", true, true)...)

	for _, c := range spChecks {
		cells = append(cells, spCells("checked by "+c.name, "p, _, _ := LOOKUP\n_ = p.ID\n"+c.mk("p"), false, true)...)
	}
	cells = append(cells, spCells("both of two lookups checked", "p, _, _ := LOOKUP\nq, _, _ := LOOKUP\n_ = signInTo(p)\n_ = q.ServiceProvider", false, true)...)
	cells = append(cells, spCells("lookup in an if initializer", "if p, found, _ := LOOKUP; found {\n_ = p.ServiceProvider\n}", false, true)...)
	cells = append(cells, spCells("struct key built from the result", "p, _, _ := LOOKUP\n_ = T{ServiceProvider: p.ID > 0}", false, true)...)
	cells = append(cells, spCells("lookup and check inside one func literal", "func() {\np, _, _ := LOOKUP\n_ = p.ServiceProvider\n}()", false, true)...)
	cells = append(cells, spCells("returned check result", "func f(s S) error {\np, _, _ := LOOKUP\nreturn signInTo(p)\n}", false, true)...)
	cells = append(cells, spCell{"delegate function itself", spDelegatePath, spPreamble + spDelegateDecl, false})
	cells = append(cells, spCell{"delegate function outside its path", spOtherPath, spPreamble + spDelegateDecl, true})
	return cells
}

func TestCallSiteGuardTable(t *testing.T) {
	cells := spTable()
	var refused, accepted int
	for _, tc := range cells {
		if tc.wantGaps {
			refused++
		} else {
			accepted++
		}
		t.Run(tc.name, func(t *testing.T) {
			gaps := callSiteGaps(tc.path, tc.src)
			for _, g := range gaps {
				if strings.HasPrefix(g, "typecheck:") {
					t.Fatalf("the cell does not compile: %v", gaps)
				}
			}
			if tc.wantGaps && len(gaps) == 0 {
				t.Fatalf("the guard passed a shape it must refuse:\n%s", tc.src)
			}
			if !tc.wantGaps && len(gaps) != 0 {
				t.Fatalf("the guard refused a shape it must pass: %v\n%s", gaps, tc.src)
			}
		})
	}
	t.Logf("table: %d refused cells, %d accepted cells", refused, accepted)
	if refused < 22+14 {
		t.Fatalf("refused cells = %d, want at least the 20 scanner-shape cells, the 2 method-reference cells and one per fail-closed shape", refused)
	}
}
