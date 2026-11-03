package grant

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey/cells"
)

func TestValidKeyHoldsTheRepositoryKeyCells(t *testing.T) {
	for _, c := range cells.Table() {
		if got := ValidKey(c.Key); got != c.Valid {
			t.Errorf("%s: ValidKey(%q) = %v, want %v", c.Name, c.Key, got, c.Valid)
		}
	}
	if !ValidKey("*") {
		t.Error(`ValidKey("*") = false, want true`)
	}
}
