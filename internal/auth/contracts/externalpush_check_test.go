package contracts

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

// TestCheckExternalPushLifecycleAgreesWithTheSchemaCorpus: the runtime check
// accepts every accept golden and refuses every reject golden whose defect lies
// in the members a producer chooses (the change, the organization and the
// source). A reject for an unknown member has no field on the type and is
// outside the check; a reject on the revision, the key or the envelope is the
// writer's, not the producer's.
func TestCheckExternalPushLifecycleAgreesWithTheSchemaCorpus(t *testing.T) {
	c := loadWireCorpus(t, provisioningSurfaces[2])
	for _, a := range c.Accept {
		event, err := ParseExternalPushLifecycleEvent(testRoot(t), readCorpusFile(t, provisioningSurfaces[2], a.File))
		if err != nil {
			t.Fatalf("%s: %v", a.File, err)
		}
		if err := externalpush.Check(event.EventType, event.AggregateID, event.Payload.OrganizationID, event.Payload.SourceID, event.Payload.Change.Wire()); err != nil {
			t.Errorf("accept %s is refused by the runtime check: %v", a.File, err)
		}
	}
	checked := 0
	for _, r := range c.Reject {
		inProducerMembers := strings.HasPrefix(r.ExpectInstanceLocation, "/payload/change") ||
			r.ExpectInstanceLocation == "/payload/source_id" ||
			(r.ExpectInstanceLocation == "/payload" && r.ExpectKeyword == "required")
		if !inProducerMembers || r.ExpectKeyword == "additionalProperties" {
			continue
		}
		var event ExternalPushLifecycleEvent
		if err := json.Unmarshal(readCorpusFile(t, provisioningSurfaces[2], r.File), &event); err != nil {
			t.Fatalf("reject %s does not decode: %v", r.File, err)
		}
		if err := externalpush.Check(event.EventType, event.AggregateID, event.Payload.OrganizationID, event.Payload.SourceID, event.Payload.Change.Wire()); err == nil {
			t.Errorf("reject %s (%s at %s) is accepted by the runtime check", r.File, r.ExpectKeyword, r.ExpectInstanceLocation)
		}
		checked++
	}
	if checked < 8 {
		t.Fatalf("only %d reject goldens were compared, want at least 8", checked)
	}
}

func TestCheckExternalPushLifecycleRefusesWhatTheTypesCannotCarry(t *testing.T) {
	good := ExternalPushLifecycleChange{Status: "active", Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}}
	const org, src, id = "org_EXAMPLE0000000000000001", "src_EXAMPLE0000000000000001", "cred-1"
	if err := externalpush.Check(ExternalPushCreated, id, org, src, good.Wire()); err != nil {
		t.Fatalf("a valid created is refused: %v", err)
	}
	for name, tc := range map[string]struct {
		eventType, id, org, src string
		change                  ExternalPushLifecycleChange
	}{
		"revoked with scopes":       {ExternalPushRevoked, id, org, src, ExternalPushLifecycleChange{Status: "revoked", Scopes: []string{}}},
		"scoped with a status":      {ExternalPushScoped, id, org, src, ExternalPushLifecycleChange{Status: "active", Scopes: []string{}}},
		"scoped without scopes":     {ExternalPushScoped, id, org, src, ExternalPushLifecycleChange{}},
		"created with a parent":     {ExternalPushCreated, id, org, src, ExternalPushLifecycleChange{Status: "active", Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}, RotationParentID: "p"}},
		"organization without org_": {ExternalPushCreated, id, "EXAMPLE", src, good},
		"id with a dot":             {ExternalPushCreated, "a.b", org, src, good},
		"unknown verb":              {"external_push_credential.deleted", id, org, src, good},
	} {
		if err := externalpush.Check(tc.eventType, tc.id, tc.org, tc.src, tc.change.Wire()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
