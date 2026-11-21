package latestrow_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const fixtureStub = `package latestrow

const OrderKey = "k"

func ArgMax(c string) string { return c }

func ArgMaxKeepNull(c string) string { return ArgMax(c) }

func ArgMaxKeepNullBy(c, v string) string { return c + v }
`

func writeFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	files["go.mod"] = "module example.com/fix\n\ngo 1.24\n"
	files["latestrow/latestrow.go"] = fixtureStub
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestLatestrowCensusBindsGoTypes(t *testing.T) {
	paths := map[string]bool{"example.com/fix/latestrow": true}
	cases := []struct {
		name      string
		files     map[string]string
		want      map[string]int
		uncounted int
	}{
		{"C1 shadowed name", map[string]string{"a/a.go": `package a

import "example.com/fix/latestrow"

func Real() string { return latestrow.ArgMax("x") }

func Shadow() string {
	latestrow := struct{ ArgMax func(string) string }{ArgMax: func(s string) string { return s }}
	return latestrow.ArgMax("y")
}
`}, map[string]int{"a/a.go:Real:ArgMax": 1}, 0},
		{"C2 dot import", map[string]string{"a/a.go": `package a

import . "example.com/fix/latestrow"

func D() string { return ArgMax("c") }
`}, map[string]int{"a/a.go:D:ArgMax": 1}, 0},
		{"C3 unqualified call in the latestrow package", map[string]string{}, map[string]int{"latestrow/latestrow.go:ArgMaxKeepNull:ArgMax": 1}, 0},
		{"C4 func value stored and called twice", map[string]string{"a/a.go": `package a

import "example.com/fix/latestrow"

func F() string {
	f := latestrow.ArgMax
	return f("a") + f("b")
}
`}, map[string]int{}, 1},
		{"C5 nested module is out of scope", map[string]string{"nested/go.mod": "module example.com/nested\n\ngo 1.24\n\nrequire example.com/fix v0.0.0\n\nreplace example.com/fix => ../\n", "nested/n.go": `package nested

import "example.com/fix/latestrow"

func N() string { return latestrow.ArgMax("n") }
`}, map[string]int{}, 0},
		{"C6 integration-tagged importer", map[string]string{"a/a.go": `//go:build integration

package a

import "example.com/fix/latestrow"

func I() string { return latestrow.ArgMax("i") }
`}, map[string]int{"a/a.go:I:ArgMax": 1}, 0},
		{"C7 test file call is out of scope", map[string]string{"a/a.go": "package a\n", "a/a_test.go": `package a

import (
	"testing"

	"example.com/fix/latestrow"
)

func TestX(t *testing.T) { _ = latestrow.ArgMax("t") }
`}, map[string]int{}, 0},
		{"C8 const use is named", map[string]string{"a/a.go": `package a

import "example.com/fix/latestrow"

func K() string { return "argMax(x, " + latestrow.OrderKey + ")" }
`}, map[string]int{"a/a.go:K:OrderKey": 1}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			found, uncounted, err := latestrowCalls(writeFixture(t, c.files), paths)
			if err != nil {
				t.Fatal(err)
			}
			if c.want["latestrow/latestrow.go:ArgMaxKeepNull:ArgMax"] == 0 {
				delete(found, "latestrow/latestrow.go:ArgMaxKeepNull:ArgMax")
			}
			if !reflect.DeepEqual(found, c.want) || len(uncounted) != c.uncounted {
				t.Fatalf("found = %v, uncounted = %v; want %v, %d uncounted", found, uncounted, c.want, c.uncounted)
			}
		})
	}
}
