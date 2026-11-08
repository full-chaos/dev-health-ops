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

// checkInternalReasonRow checks one audit row of an allow: the wire code is allowed_by_role, and the internal reason key
// is absent (want "") or equal to want.
func checkInternalReasonRow(t *testing.T, e audit.AuditEvent, d wireDecision, want string) {
	t.Helper()
	if !d.Allowed || d.ReasonCode != "allowed_by_role" {
		t.Fatalf("wire decision %+v, want an allow with allowed_by_role", d)
	}
	if got := attrString(t, e, "decision_reason_code"); got != "allowed_by_role" {
		t.Errorf("%s: decision_reason_code %q, want allowed_by_role", e.EventType, got)
	}
	v, present := e.Attributes[internalReasonKey]
	switch {
	case want == "" && present:
		t.Errorf("%s: %s = %v, want the key absent", e.EventType, internalReasonKey, v)
	case want != "" && attrString(t, e, internalReasonKey) != want:
		t.Errorf("%s: %s = %v, want %s", e.EventType, internalReasonKey, v, want)
	}
	if err := audit.ValidateAttributes(e.Attributes); err != nil {
		t.Errorf("%s: %v", e.EventType, err)
	}
	auditwire.MustValidate(t, e)
}

func TestAuthorizeAuditRecordsTheInternalReasonOfAPlatformAdminAllow(t *testing.T) {
	t.Run("high-risk role allow has no internal reason", func(t *testing.T) {
		h := newAuthorizeHandlerFixture(t)
		d := h.decideOne(h.org1("settings.write"))
		rows := h.audit.rows()
		if len(rows) != 1 || rows[0].EventType != audit.EventAuthorizeHighRiskDecided {
			t.Fatalf("rows %+v, want one high-risk row", rows)
		}
		checkInternalReasonRow(t, rows[0], d, "")
	})
	t.Run("high-risk platform-admin allow records the internal reason", func(t *testing.T) {
		h := newAuthorizeHandlerFixture(t)
		h.makePlatformAdminMember()
		d := h.decideOne(h.org1("settings.write"))
		rows := h.audit.rows()
		if len(rows) != 1 || rows[0].EventType != audit.EventAuthorizeHighRiskDecided {
			t.Fatalf("rows %+v, want one high-risk row", rows)
		}
		checkInternalReasonRow(t, rows[0], d, "allowed_by_platform_admin")
	})
	t.Run("delegated platform-admin allow records the internal reason on both rows", func(t *testing.T) {
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
			checkInternalReasonRow(t, e, d, "allowed_by_platform_admin")
		}
	})
}

// The audit allowlist's decision_internal_reason set is exactly the internal-only reasons the wire hides.
func TestAuditInternalReasonVocabularyIsTheInternalOnlyReasons(t *testing.T) {
	got := slices.Clone(audit.AttributeVocabulary()[internalReasonKey])
	slices.Sort(got)
	var want []string
	for reason := range internalOnlyReasons {
		want = append(want, string(reason))
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s set %v, want the internalOnlyReasons keys %v", internalReasonKey, got, want)
	}
}
