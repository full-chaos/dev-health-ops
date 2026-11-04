package repokey

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey/cells"
)

func TestValidHoldsTheCells(t *testing.T) {
	for _, c := range cells.Table() {
		if got := Valid(c.Key); got != c.Valid {
			t.Errorf("%s: Valid(%q) = %v, want %v", c.Name, c.Key, got, c.Valid)
		}
		if got := Matcher().MatchString(c.Key); got != c.Valid && len(c.Key) <= MaxBytes {
			t.Errorf("%s: Matcher(%q) = %v, want %v", c.Name, c.Key, got, c.Valid)
		}
	}
	if !Valid("*") {
		t.Error(`Valid("*") = false`)
	}
}
