package callseq

import (
	"fmt"
	"testing"
)

type spy struct{ errs []string }

func (s *spy) Helper()                   {}
func (s *spy) Errorf(f string, a ...any) { s.errs = append(s.errs, fmt.Sprintf(f, a...)) }

func TestSameAcceptsEqualAndRejectsDifferent(t *testing.T) {
	ok := &spy{}
	Same(ok, "x", map[string][]string{"a": {"P"}, "b": {"P"}, "c": {"P"}})
	if len(ok.errs) != 0 {
		t.Fatalf("equal sequences reported %v", ok.errs)
	}
	bad := &spy{}
	Same(bad, "x", map[string][]string{"a": {"P"}, "b": {"P", "B"}})
	if len(bad.errs) == 0 {
		t.Fatal("different sequences were accepted")
	}
}

func TestDeltaWantsExactlyTheExtraCalls(t *testing.T) {
	base := []string{"ByEmail", "Check"}
	cases := []struct {
		name         string
		other, extra []string
		fails        bool
	}{
		{"exact, any order", []string{"ByEmail", "Project", "Check", "Memberships"}, []string{"Memberships", "Project"}, false},
		{"missing extra", []string{"ByEmail", "Check", "Project"}, []string{"Memberships", "Project"}, true},
		{"surplus extra", []string{"ByEmail", "Check", "Project", "Memberships"}, []string{"Project"}, true},
		{"base call lost", []string{"ByEmail"}, nil, true},
		{"none", []string{"ByEmail", "Check"}, nil, false},
	}
	for _, c := range cases {
		s := &spy{}
		Delta(s, c.name, base, c.other, c.extra)
		if (len(s.errs) > 0) != c.fails {
			t.Errorf("%s: errors %v, want failure %v", c.name, s.errs, c.fails)
		}
	}
}

func TestRecorderNamesIsACopy(t *testing.T) {
	var r Recorder
	r.Add("a")
	got := r.Names()
	got[0] = "x"
	if r.Names()[0] != "a" {
		t.Fatal("Names exposes the log")
	}
}
