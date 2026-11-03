package oauthgrant

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// The rows of an approval (client session created, consent approved) and of a
// denial, for a browser and an mcp request, with and without an organization.
func TestDecisionRowsAreValidAuditEventV1(t *testing.T) {
	org := uuid.New()
	var s Store
	for _, class := range []string{ClassBrowser, "mcp"} {
		for _, organization := range []*uuid.UUID{nil, &org} {
			r := row{class: class, audience: "acr-api", organization: organization}
			ctx := requestid.With(context.Background(), uuid.New())
			event := s.event(ctx, uuid.New(), r, uuid.New())
			created := withType(event, EventClientSessionCreate)
			approved := withType(event, EventConsentApproved)
			denied := s.event(ctx, uuid.New(), r, uuid.New())
			denied.EventType, denied.Outcome = EventConsentDenied, audit.OutcomeDenied
			denied.Attributes["reason_code"] = "denied_policy"
			for _, ev := range []audit.AuditEvent{created, approved, denied} {
				auditwire.MustValidate(t, ev)
				if err := audit.ValidateAttributes(ev.Attributes); err != nil {
					t.Fatalf("%s ValidateAttributes: %v", ev.EventType, err)
				}
			}
		}
	}
}
