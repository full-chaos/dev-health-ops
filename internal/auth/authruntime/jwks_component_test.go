package authruntime

import "testing"

// The JWKS document refresher is a lifecycle component: it starts after the
// key states it reads and before the API serves it, and stops in reverse.
func TestConfigureRegistersTheJWKSDocumentBetweenKeyStatesAndTheAPI(t *testing.T) {
	_, components := configuredForShutdown(t)
	states := indexOf(t, components, "auth-key-states")
	document := indexOf(t, components, "auth-jwks-document")
	api := indexOf(t, components, "auth-api-http")
	if document <= states || document >= api {
		t.Fatalf("auth-jwks-document at %d, want after auth-key-states (%d) and before auth-api-http (%d)", document, states, api)
	}
}
