package ssoprovider

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

func TestMaskIsTheLegacyResponseMasking(t *testing.T) {
	long := strings.Repeat("A", 80)
	config := map[string]any{"client_secret": "plain-secret", "certificate": long, "issuer": "https://idp.example.com"}
	mask(config)
	if config["client_secret"] != "********" {
		t.Fatalf("client_secret = %v", config["client_secret"])
	}
	if config["certificate"] != strings.Repeat("A", 50)+"..." {
		t.Fatalf("certificate = %v", config["certificate"])
	}
	if config["issuer"] != "https://idp.example.com" {
		t.Fatalf("issuer changed: %v", config["issuer"])
	}
	short := map[string]any{"certificate": strings.Repeat("B", 50)}
	mask(short)
	if short["certificate"] != strings.Repeat("B", 50) {
		t.Fatalf("a 50-character certificate was cut: %v", short["certificate"])
	}
	edge := map[string]any{"certificate": strings.Repeat("C", 51)}
	mask(edge)
	if edge["certificate"] != strings.Repeat("C", 50)+"..." {
		t.Fatalf("a 51-character certificate: %v, want it cut to 50 and ...", edge["certificate"])
	}
	none := map[string]any{"certificate": nil}
	mask(none)
	if none["certificate"] != nil {
		t.Fatalf("a null certificate changed: %v", none["certificate"])
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
