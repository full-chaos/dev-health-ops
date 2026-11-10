package devhealth_test

import (
	"fmt"
	"testing"
)

// CHAOS-13356: auth-service reads go-api's AUTH_REGISTER_LIMIT (internal/auth/authapi/register.go). Only the live e2e
// stack script sets it (ci/live_e2e_auth_stack.sh); prod runs the 3/hour default, so the chart renders no such key
// for the auth-service container. Phase 4 with usersOwnedStage owned is prod's render (values.prod.yaml); the lower
// phases are checked too.
func TestAuthServiceProdRenderHasNoRegisterLimit(t *testing.T) {
	for phase := 0; phase <= 4; phase++ {
		t.Run(fmt.Sprintf("phase %d", phase), func(t *testing.T) {
			env := authServiceContainerEnv(renderPhase(t, phase, mailSets()...))
			if phase == 4 && len(env) == 0 {
				t.Fatal("FAIL: the phase 4 render has no auth-service container env")
			}
			if value, ok := env["AUTH_REGISTER_LIMIT"]; ok {
				t.Fatalf("FAIL: the auth-service container renders AUTH_REGISTER_LIMIT (%s); prod runs the 3/hour default", value)
			}
		})
	}
}
