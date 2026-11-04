package authorization

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey/cells"
)

func TestNormalizeRequestHoldsTheRepositoryKeyCells(t *testing.T) {
	for _, c := range append(cells.Table(), cells.Cell{Name: "organization-wide", Key: "*", Valid: true}) {
		req := Request{Action: "acr.context.read", ResourceKind: resourceRepository, OrganizationID: "org_example", ResourceKey: c.Key}
		if _, ok := NormalizeRequest(req); ok != c.Valid {
			t.Errorf("%s: NormalizeRequest(%q) ok = %v, want %v", c.Name, c.Key, ok, c.Valid)
		}
	}
}
