package pgident

import (
	"strings"
	"testing"
)

func TestCheck(t *testing.T) {
	for name, want := range map[string]Problem{
		"auth":                  OK,
		"identity_plane":        OK,
		"a":                     OK,
		strings.Repeat("a", 63): OK,
		strings.Repeat("a", 64): BadShape,
		"":                      BadShape,
		"Auth":                  BadShape,
		"1auth":                 BadShape,
		"_auth":                 BadShape,
		"a-b":                   BadShape,
		`a"b`:                   BadShape,
		"a b":                   BadShape,
		"a;drop":                BadShape,
		"with":                  ReservedKeyword,
		"user":                  ReservedKeyword,
		"select":                ReservedKeyword,
		"public":                OK,
	} {
		if got := Check(name); got != want {
			t.Errorf("Check(%q) = %d, want %d", name, got, want)
		}
	}
}
