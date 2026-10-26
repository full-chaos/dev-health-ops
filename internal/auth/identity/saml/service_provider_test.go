package saml_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

func TestSAMLACSRefusesAServiceProviderPrincipalLikeADisabledOne(t *testing.T) {
	id := uuid.New()
	members := &memberSet{ids: map[uuid.UUID]bool{id: true}}
	disabled := postACS(t, &lookupStore{hit: &saml.Principal{ID: id, Disabled: true}}, members, nil)
	if disabled.status != http.StatusForbidden || len(disabled.sessions.got) != 0 {
		t.Fatalf("control: disabled principal ACS = %d, sessions %d", disabled.status, len(disabled.sessions.got))
	}
	service := postACS(t, &lookupStore{hit: &saml.Principal{ID: id, ServiceProvider: true}}, members, nil)
	service.refused(t, disabled.status, disabled.detail, "principal_deactivated")
	if members.reads != 0 {
		t.Fatalf("membership reads = %d, want 0 before the refusal", members.reads)
	}
}
