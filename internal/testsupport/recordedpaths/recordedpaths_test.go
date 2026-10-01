package recordedpaths

import (
	"reflect"
	"strings"
	"testing"
)

const sample = `{"cases":[{"name":"a","rows":[{"id":1,"flag":true}],"empty":[],"nothing":null}],"label":"x"}`

func TestPathsFoldIndicesAndListScalarsOnly(t *testing.T) {
	got, err := Paths([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".cases[].name", ".cases[].nothing", ".cases[].rows[].flag", ".cases[].rows[].id", ".label"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths %v, want %v (an empty array holds no scalar; null is one)", got, want)
	}
	top, err := Paths([]byte(`[{"a":1},{"a":2,"b":[3]}]`))
	if err != nil || !reflect.DeepEqual(top, []string{"[].a", "[].b[]"}) {
		t.Fatalf("top-level array paths %v, %v", top, err)
	}
}

func declared() ([]string, map[string]string) {
	return []string{".cases[].rows[].flag", ".cases[].rows[].id", ".cases[].nothing"},
		map[string]string{".cases[].name": "a case label", ".label": "a recording label"}
}

func TestAClosedWorldHasNoProblem(t *testing.T) {
	claims, notClaims := declared()
	if problems := Problems([]byte(sample), claims, notClaims); problems != nil {
		t.Fatalf("problems %v", problems)
	}
}

// Every way the world can be open is reported (the plants of the check).
func TestEveryOpeningIsReported(t *testing.T) {
	for name, mutate := range map[string]func(claims []string, notClaims map[string]string) ([]string, map[string]string, string){
		"an undeclared path": func(c []string, n map[string]string) ([]string, map[string]string, string) {
			delete(n, ".label")
			return c, n, "undeclared path (claim it or declare it a not-a-claim with a reason): .label"
		},
		"a stale claim": func(c []string, n map[string]string) ([]string, map[string]string, string) {
			return append(c, ".cases[].gone"), n, "stale claim (the file has no such path): .cases[].gone"
		},
		"a stale not-a-claim": func(c []string, n map[string]string) ([]string, map[string]string, string) {
			n[".gone"] = "was a label"
			return c, n, "stale not-a-claim (the file has no such path): .gone"
		},
		"a path in both lists": func(c []string, n map[string]string) ([]string, map[string]string, string) {
			n[".cases[].rows[].id"] = "also a not-a-claim"
			return c, n, "in both lists: .cases[].rows[].id"
		},
		"a claimed path twice": func(c []string, n map[string]string) ([]string, map[string]string, string) {
			return append(c, ".cases[].rows[].id"), n, "claimed twice: .cases[].rows[].id"
		},
		"a not-a-claim without a reason": func(c []string, n map[string]string) ([]string, map[string]string, string) {
			n[".label"] = "  "
			return c, n, "not-a-claim without a reason: .label"
		},
	} {
		t.Run(name, func(t *testing.T) {
			claims, notClaims := declared()
			claims, notClaims, want := mutate(claims, notClaims)
			problems := Problems([]byte(sample), claims, notClaims)
			if !strings.Contains(strings.Join(problems, "\n"), want) {
				t.Fatalf("problems %v do not hold %q", problems, want)
			}
		})
	}
	if problems := Problems([]byte(`{"a":`), []string{".a"}, nil); len(problems) == 0 {
		t.Fatal("a document that does not decode is not a closed world")
	}
}

type recorder struct {
	testing.TB
	failed string
}

func (r *recorder) Helper()               {}
func (r *recorder) Fatal(args ...any)     { r.failed = "fatal" }
func (r *recorder) Fatalf(string, ...any) { r.failed = "fatalf" }

func TestCheckFailsOnNoClaims(t *testing.T) {
	probe := &recorder{TB: t}
	Check(probe, []byte(sample), nil, map[string]string{".label": "x"})
	if probe.failed == "" {
		t.Fatal("a check with no claims closed nothing and passed")
	}
}
