package routescan

import "testing"

const declFixture = `// Package p is a fixture.
package p

// Issue issues.
func Issue() {}

func (s *Store) Save() {}

func (g Gen[T]) Load() {}

// Limits are the ceilings.
const (
	A = 1
	B, C = 2, 3
)

var V = "v"

type T struct {
	F int
}
`

func TestDeclsNamesEveryTopLevelDeclarationAndItsLines(t *testing.T) {
	decls, err := Decls([]byte(declFixture))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][2]int{
		"package": {1, 2}, "Issue": {4, 5}, "Store.Save": {7, 7}, "Gen.Load": {9, 9},
		"A": {13, 13}, "B,C": {14, 14}, "(A,B,C)": {11, 15}, "V": {17, 17}, "(V)": {17, 17}, "T": {19, 21}, "(T)": {19, 21},
	}
	if len(decls) != len(want) {
		t.Fatalf("got %d declarations, want %d: %+v", len(decls), len(want), decls)
	}
	for _, d := range decls {
		if span, ok := want[d.Name]; !ok || span != [2]int{d.Lo, d.Hi} {
			t.Errorf("%s spans %d-%d, want %v (known %v)", d.Name, d.Lo, d.Hi, span, ok)
		}
	}
}

func TestEnclosingPicksTheSmallestDeclarationAndNamedFindsEach(t *testing.T) {
	decls, err := Decls([]byte(declFixture))
	if err != nil {
		t.Fatal(err)
	}
	for line, want := range map[int]string{1: "package", 4: "Issue", 11: "(A,B,C)", 13: "A", 14: "B,C", 17: "V", 20: "T"} {
		if d, ok := Enclosing(decls, line); !ok || d.Name != want {
			t.Errorf("line %d: %+v %v, want %s", line, d, ok, want)
		}
	}
	if _, ok := Enclosing(decls, 3); ok {
		t.Error("a blank line between declarations is in no declaration")
	}
	if got := Named(append(decls, Decl{"Issue", 30, 31}), "Issue"); len(got) != 2 {
		t.Errorf("Named found %d Issue declarations, want 2", len(got))
	}
	if _, err := Decls([]byte("package p\nfunc (")); err == nil {
		t.Error("a source that does not parse must be an error")
	}
}
