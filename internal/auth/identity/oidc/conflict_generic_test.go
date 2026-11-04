package oidc

import (
	"net/http"
	"testing"
)

const genericConflictDetail = "This sign-in could not be completed. Contact your administrator if this continues."

func TestConflictRefusalsShareOneGenericDetail(t *testing.T) {
	for name, f := range map[string]*Error{"email collision": conflict(), "identity ambiguous": ambiguousIdentity()} {
		if f.Status != http.StatusConflict || f.Detail != genericConflictDetail {
			t.Errorf("%s: %d %v, want 409 %q", name, f.Status, f.Detail, genericConflictDetail)
		}
	}
	if got := conflict().Reason; got != ReasonEmailCollision {
		t.Errorf("collision reason %q", got)
	}
	if got := ambiguousIdentity().Reason; got != ReasonIdentityConflict {
		t.Errorf("ambiguous reason %q", got)
	}
}
