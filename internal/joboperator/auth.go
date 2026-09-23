package joboperator

import (
	"context"
	"errors"
)

// The operator CLI (`dho workers`) has no bearer token. Whoever can exec
// into a worker pod and holds its database DSNs is the operator: that is
// the authentication boundary, and the cluster's exec audit trail records
// who used it. The CLI acts as one fixed principal, and every mutation
// still requires --reason and --correlation-id and writes an audit row.

// OperatorPrincipalType is the audit principal_type of the operator CLI.
const OperatorPrincipalType = "operator"

// OperatorPrincipal is the one principal `dho workers` acts as.
var OperatorPrincipal = Principal{Type: OperatorPrincipalType, ID: "dho-workers"}

// ErrAuthorization refuses a request from any principal other than the
// operator.
var ErrAuthorization = errors.New("worker operator authorization failed")

// OperatorAuthorizer allows every operator action for OperatorPrincipal and
// nothing for any other principal.
func OperatorAuthorizer() Authorizer { return operatorAuthorizer{} }

type operatorAuthorizer struct{}

func (operatorAuthorizer) Authorize(_ context.Context, request AuthorizationRequest) error {
	if request.Principal != OperatorPrincipal {
		return ErrAuthorization
	}
	return nil
}
