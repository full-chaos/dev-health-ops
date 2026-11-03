package provisioning

import (
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/repokey/cells"
)

// The event member is the looser form of the key: groups of 1-100 characters
// from the key alphabet, 2-20 of them. The cells it cannot decide (a dot
// segment, a leading dot, the 255-byte total) are the grant store's.
func TestProvisioningResourceKeyHoldsTheLooserCells(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	notDecided := map[string]bool{
		"256 bytes": true, "dot segment": true, "dot-dot segment": true, "dot-dot last": true,
		"dot last": true, "leading dot group": true, "leading dot owner": true,
	}
	for _, c := range append(cells.Table(), cells.Cell{Name: "organization-wide", Key: "*", Valid: true}) {
		if notDecided[c.Name] {
			continue
		}
		change := map[string]any{"resource_type": "repository", "resource_key": c.Key}
		goErr := CheckEvent("resource_grant", "grt_EXAMPLE0000000000000001", "resource_grant.created", change)
		raw, _ := json.Marshal(change)
		doc := `{"schema_version":"provisioning-event.v1","aggregate_type":"resource_grant","aggregate_id":"grt_EXAMPLE0000000000000001",` +
			`"event_type":"resource_grant.created","idempotency_key":"resource_grant:grt_EXAMPLE0000000000000001:1","created_at":"2026-09-03T10:00:00Z",` +
			`"payload":{"revision":1,"organization_id":"org_EXAMPLE0000000000000001","change":` + string(raw) + `}}`
		_, schemaErr := contracts.ParseProvisioningEvent(root, []byte(doc))
		if (goErr == nil) != c.Valid {
			t.Errorf("%s: CheckEvent = %v, want valid=%v", c.Name, goErr, c.Valid)
		}
		if (schemaErr == nil) != c.Valid {
			t.Errorf("%s: schema = %v, want valid=%v", c.Name, schemaErr, c.Valid)
		}
	}
}
