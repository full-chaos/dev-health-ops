package authapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

func TestPlatformAdminAllowIsInternalAndAnswersAllowedByRole(t *testing.T) {
	code, ok := wireReasonCode(authorization.ReasonAllowedByPlatformAdmin)
	if !ok || code != string(authorization.ReasonAllowedByRole) {
		t.Fatalf("wireReasonCode(allowed_by_platform_admin) = %q, %v; want allowed_by_role", code, ok)
	}
	codes := AuthorizeWireReasonCodes()
	if slices.Contains(codes, string(authorization.ReasonAllowedByPlatformAdmin)) {
		t.Fatalf("the wire set %v names the internal reason", codes)
	}
	if !slices.Equal(codes, slices.Compact(slices.Clone(codes))) {
		t.Fatalf("the wire set %v repeats a code", codes)
	}
}

// A platform admin with the member role is allowed settings.write: the wire and the audit row say allowed_by_role,
// the decision log line keeps the internal reason.
func TestAuthorizePlatformAdminAllowLogsTheInternalReason(t *testing.T) {
	var logs bytes.Buffer
	h := newAuthorizeHandlerFixture(t, func(d *AuthorizeDeps) { d.Logger = slog.New(slog.NewJSONHandler(&logs, nil)) })
	snap := h.state.snapshots[snapKey{h.principal, h.org}]
	snap.platformRole = "platform_admin"
	snap.membership = &authorization.Membership{OrgID: h.org, Role: "member", Status: authorization.StatusActive}

	rec := h.one(h.question("settings.write", "organization", "acme", h.org))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	d := decodeWire[wireDecision](t, rec)
	if !d.Allowed || d.ReasonCode != "allowed_by_role" {
		t.Fatalf("wire decision %+v, want an allow with allowed_by_role", d)
	}
	rows := h.audit.rows()
	if len(rows) != 1 || attrString(t, rows[0], "decision_reason_code") != "allowed_by_role" {
		t.Fatalf("audit rows %+v, want one with allowed_by_role", rows)
	}
	var found bool
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("log line %s: %v", line, err)
		}
		if entry["msg"] != "authorize decision" {
			continue
		}
		found = true
		if entry["decision_id"] != d.DecisionID || entry["reason_code"] != "allowed_by_role" || entry["reason"] != "allowed_by_platform_admin" {
			t.Fatalf("decision log line %v, want reason_code allowed_by_role and reason allowed_by_platform_admin", entry)
		}
	}
	if !found {
		t.Fatalf("no decision log line in %s", logs.String())
	}

	snap.platformRole = ""
	rec = h.one(h.question("settings.write", "organization", "acme", h.org))
	if d := decodeWire[wireDecision](t, rec); rec.Code != http.StatusOK || d.Allowed || d.ReasonCode != "role_lacks_action" {
		t.Fatalf("member without admin.orgs: status %d decision %+v, want deny role_lacks_action", rec.Code, d)
	}
}
