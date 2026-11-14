package ssoprovider

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

func maskedOf(t *testing.T, stored string) string {
	t.Helper()
	raw, err := MaskedConfig(&stored)
	if err != nil {
		t.Fatalf("MaskedConfig(%s): %v", stored, err)
	}
	return string(raw)
}

func TestMaskIsTheLegacyResponseMasking(t *testing.T) {
	for name, c := range map[string]struct{ stored, want string }{
		"secret, cut certificate": {`{"client_secret": "plain-secret", "certificate": "` + strings.Repeat("A", 80) + `", "issuer": "https://idp.example.com"}`,
			`{"client_secret":"********","certificate":"` + strings.Repeat("A", 50) + `...","issuer":"https://idp.example.com"}`},
		"50 characters kept": {`{"certificate": "` + strings.Repeat("B", 50) + `"}`, `{"certificate":"` + strings.Repeat("B", 50) + `"}`},
		"51 characters cut":  {`{"certificate": "` + strings.Repeat("C", 51) + `"}`, `{"certificate":"` + strings.Repeat("C", 50) + `..."}`},
		"null certificate":   {`{"certificate": null}`, `{"certificate":null}`},
		"null config":        {`null`, `{}`},
		"stored order kept":  {`{"scopes": ["openid"], "client_id": "c", "issuer": "i", "claim_mapping": {"z": "1", "a": "2"}}`, `{"scopes":["openid"],"client_id":"c","issuer":"i","claim_mapping":{"z":"1","a":"2"}}`},
		"unicode, not ascii": {`{"issuer": "https://\u00e9.example", "x": "\u2028<&>"}`, "{\"issuer\":\"https://\u00e9.example\",\"x\":\"\u2028<&>\"}"},
	} {
		if got := maskedOf(t, c.stored); got != c.want {
			t.Errorf("%s: %s, want %s", name, got, c.want)
		}
	}
}

func TestEveryAuditRowIsAValidWireRowAndCarriesNoConfig(t *testing.T) {
	org, id, actor := uuid.New(), uuid.New(), uuid.New()
	s := Store{}
	a := Actor{PrincipalID: &actor}
	rows := []audit.AuditEvent{
		s.auditEvent("auth.sso_provider.created", org, id, a, map[string]any{"reason_code": "sso_provider_created", "provider": ProtocolOIDC}),
		s.auditEvent("auth.sso_provider.updated", org, id, a, map[string]any{"reason_code": "sso_provider_updated"}),
		s.auditEvent("auth.sso_provider.activated", org, id, a, map[string]any{"reason_code": "sso_provider_activated", "new_status": statusAttribute(StatusActive)}),
		s.auditEvent("auth.sso_provider.deactivated", org, id, a, map[string]any{"reason_code": "sso_provider_deactivated", "new_status": statusAttribute(StatusInactive)}),
		s.auditEvent("auth.sso_provider.deleted", org, id, a, map[string]any{"reason_code": "sso_provider_deleted"}),
	}
	for _, row := range rows {
		auditwire.MustValidate(t, row)
		for key := range row.Attributes {
			if key == "config" || key == "client_secret" || key == "encrypted_secrets" || key == "certificate" {
				t.Fatalf("%s carries %s", row.EventType, key)
			}
		}
	}
}
