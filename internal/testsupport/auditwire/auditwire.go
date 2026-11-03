// Package auditwire projects a producer's audit row onto the audit-event.v1
// wire document and validates it against the published schema. Rows keep uuid
// ids; the wire prefixes them (prn_, org_) and re-encodes the request id.
package auditwire

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// Document is the audit-event.v1 document of row. A value that cannot be
// projected (an id that is not a uuid) is left in its row form, so the schema
// refuses it.
func Document(row audit.AuditEvent) (map[string]any, error) {
	attributes := map[string]any{}
	if len(row.Attributes) > 0 {
		raw, err := json.Marshal(row.Attributes)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &attributes); err != nil {
			return nil, err
		}
	}
	doc := map[string]any{
		"schema_version": "audit-event.v1",
		"event_type":     row.EventType,
		"occurred_at":    "2026-11-02T00:00:00Z",
		"outcome":        string(row.Outcome),
		"attributes":     attributes,
	}
	set := func(member string, id *string, aggregate string) {
		if id == nil {
			return
		}
		if wire := provisioning.WireID(aggregate, *id); wire != "" {
			doc[member] = wire
			return
		}
		doc[member] = *id
	}
	set("actor_principal_id", row.ActorPrincipalID, "principal")
	set("subject_principal_id", row.SubjectPrincipalID, "principal")
	set("organization_id", row.OrganizationID, "organization")
	if row.PolicyRevision != nil {
		doc["policy_revision"] = float64(*row.PolicyRevision)
	}
	if row.RequestID != nil {
		if id, err := uuid.Parse(*row.RequestID); err == nil {
			doc["request_id"] = requestid.WireOf(id)
		} else {
			doc["request_id"] = *row.RequestID
		}
	}
	return doc, nil
}

// MustValidate fails t when the row's wire document is refused by audit-event.v1.
func MustValidate(t testing.TB, row audit.AuditEvent) {
	t.Helper()
	doc, err := Document(row)
	if err != nil {
		t.Fatalf("auditwire: projecting %s: %v", row.EventType, err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("auditwire: getwd: %v", err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatalf("auditwire: %v", err)
	}
	if err := contracts.Validate(root, contracts.AuditEventSurface, doc); err != nil {
		t.Errorf("auditwire: the %s row is refused by audit-event.v1: %v", row.EventType, err)
	}
}
