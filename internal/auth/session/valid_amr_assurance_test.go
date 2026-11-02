package session

import "testing"

func TestAssuranceOf(t *testing.T) {
	for v := range amrValues {
		if got := AssuranceOf([]string{v}); got != "aal1" {
			t.Errorf("%s: %q", v, got)
		}
	}
	for _, c := range [][]string{nil, {}, {"junk"}, {"junk", "pwd"}, {"pwd", "junk"}, {"pwd", "pwd"}, {""}} {
		if got := AssuranceOf(c); got != "" {
			t.Errorf("%v: %q", c, got)
		}
	}
	for _, c := range [][]string{{"mfa"}, {"pwd", "otp"}, {"hwk", "pwd"}} {
		if got := AssuranceOf(c); got != "aal1" {
			t.Errorf("%v: %q, want aal1 (no higher level from amr alone)", c, got)
		}
	}
}
