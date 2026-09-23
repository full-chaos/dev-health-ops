package joboperator

import (
	"context"
	"errors"
	"testing"
)

// TestOperatorAuthorizerAllowsOnlyTheOperatorPrincipal pins the operator
// boundary: `dho workers` acts as OperatorPrincipal for every action, and
// the authorizer refuses any other principal, including the former
// service-credential shape.
func TestOperatorAuthorizerAllowsOnlyTheOperatorPrincipal(t *testing.T) {
	authorizer := OperatorAuthorizer()
	actions := []Action{
		ActionInspect, ActionCancel, ActionRetry, ActionPauseQueue, ActionResumeQueue,
		ActionDrain, ActionUndrain, ActionInspectRoute, ActionApplyRoute, ActionPauseRoute,
		ActionDrainRoute, ActionResumeRoute, ActionInspectJobRoute, ActionRollbackJobRoute,
	}
	for _, action := range actions {
		request := AuthorizationRequest{Principal: OperatorPrincipal, Action: action, ResourceType: "job", ResourceID: "1"}
		if err := authorizer.Authorize(context.Background(), request); err != nil {
			t.Errorf("operator %s refused: %v", action, err)
		}
	}
	for _, other := range []Principal{
		{},
		{Type: "service_credential", ID: "00000000-0000-4000-8000-000000000001"},
		{Type: OperatorPrincipalType, ID: "someone-else"},
		{Type: "user", ID: "dho-workers"},
	} {
		request := AuthorizationRequest{Principal: other, Action: ActionCancel, ResourceType: "job", ResourceID: "1"}
		if err := authorizer.Authorize(context.Background(), request); !errors.Is(err, ErrAuthorization) {
			t.Errorf("principal %+v: err=%v, want ErrAuthorization", other, err)
		}
	}
	if err := validatePrincipal(OperatorPrincipal); err != nil {
		t.Fatalf("OperatorPrincipal fails the service's own principal validation: %v", err)
	}
}
