package authapi

import (
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

const internalReasonKey = "decision_internal_reason"

func (h *authorizeHandlerFixture) makePlatformAdminMember() {
	snap := h.state.snapshots[snapKey{h.principal, h.org}]
	snap.platformRole = "platform_admin"
	snap.membership = &authorization.Membership{OrgID: h.org, Role: "member", Status: authorization.StatusActive}
}

// checkWireReasonRow checks one audit row of an allow: the wire code and the row's decision_reason_code are want, and
// the legacy internal reason key is never written.
func checkWireReasonRow(t *testing.T, e audit.AuditEvent, d wireDecision, want string) {
	t.Helper()
	if !d.Allowed || d.ReasonCode != want {
		t.Fatalf("wire decision %+v, want an allow with %s", d, want)
	}
	if got := attrString(t, e, "decision_reason_code"); got != want {
		t.Errorf("%s: decision_reason_code %q, want %s", e.EventType, got, want)
	}
	if v, present := e.Attributes[internalReasonKey]; present {
		t.Errorf("%s: %s = %v, want the key absent (a legacy attribute no producer writes)", e.EventType, internalReasonKey, v)
	}
	if err := audit.ValidateAttributes(e.Attributes); err != nil {
		t.Errorf("%s: %v", e.EventType, err)
	}
	auditwire.MustValidate(t, e)
}

func TestAuthorizeAuditWritesTheWireReasonAndNoInternalReasonForAPlatformAdminAllow(t *testing.T) {
	t.Run("high-risk role allow writes allowed_by_role", func(t *testing.T) {
		h := newAuthorizeHandlerFixture(t)
		d := h.decideOne(h.org1("settings.write"))
		rows := h.audit.rows()
		if len(rows) != 1 || rows[0].EventType != audit.EventAuthorizeHighRiskDecided {
			t.Fatalf("rows %+v, want one high-risk row", rows)
		}
		checkWireReasonRow(t, rows[0], d, "allowed_by_role")
	})
	t.Run("high-risk platform-admin allow writes allowed_by_platform_admin and no internal reason", func(t *testing.T) {
		h := newAuthorizeHandlerFixture(t)
		h.makePlatformAdminMember()
		d := h.decideOne(h.org1("settings.write"))
		rows := h.audit.rows()
		if len(rows) != 1 || rows[0].EventType != audit.EventAuthorizeHighRiskDecided {
			t.Fatalf("rows %+v, want one high-risk row", rows)
		}
		checkWireReasonRow(t, rows[0], d, "allowed_by_platform_admin")
	})
	t.Run("delegated platform-admin allow writes allowed_by_platform_admin on both rows", func(t *testing.T) {
		h := newAuthorizeHandlerFixture(t)
		h.makePlatformAdminMember()
		h.delegate("settings.write")
		h.state.actors[delegatedActor] = ActorState{Active: true, CanImpersonate: true, Owner: true}
		d := h.decideOne(h.org1("settings.write"))
		rows := h.audit.rows()
		if len(rows) != 2 || len(h.audit.delegatedRows()) != 1 {
			t.Fatalf("rows %+v, want a high-risk row and a delegated row", rows)
		}
		for _, e := range rows {
			checkWireReasonRow(t, e, d, "allowed_by_platform_admin")
		}
	})
}

// decision_internal_reason is a legacy attribute: the audit schema keeps it for stored rows, and the writer allowlist
// refuses it, so no producer can write it again.
func TestAuditWriterAllowlistRefusesTheLegacyInternalReason(t *testing.T) {
	if slices.Contains(audit.AllowedAttributeKeys(), internalReasonKey) {
		t.Fatalf("the writer allowlist still admits %s", internalReasonKey)
	}
	err := audit.ValidateAttributes(map[string]any{internalReasonKey: string(authorization.ReasonAllowedByPlatformAdmin)})
	if err == nil {
		t.Fatalf("ValidateAttributes accepted %s", internalReasonKey)
	}
	if err := audit.ValidateAttributes(map[string]any{"decision_reason_code": string(authorization.ReasonAllowedByPlatformAdmin)}); err != nil {
		t.Fatalf("decision_reason_code allowed_by_platform_admin refused: %v", err)
	}
}
