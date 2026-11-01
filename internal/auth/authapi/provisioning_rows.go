package authapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// provisioningPatterns are the routes AUTH_MEMBERSHIP_ROUTES mounts.
var provisioningPatterns = []string{PatternMembershipAdd, PatternMembershipRole, PatternMembershipRemove,
	PatternMembershipTransfer, PatternInviteCreate, PatternInviteAccept, PatternOrgDeprovision, PatternOrgReactivate, PatternInviteRevoke,
	PatternOrgSuspend, PatternOrgOnboard, PatternOrgCreate}

// provisioningRouteOps maps each provisioning operation to its route.
var provisioningRouteOps = map[string]string{
	opMembershipAdd: PatternMembershipAdd, opMembershipRole: PatternMembershipRole, opMembershipRemove: PatternMembershipRemove,
	opMembershipTransfer: PatternMembershipTransfer, opInviteCreate: PatternInviteCreate, opInviteAccept: PatternInviteAccept,
	opOrgDeprovision: PatternOrgDeprovision, opOrgReactivate: PatternOrgReactivate, opInviteRevoke: PatternInviteRevoke,
	opOrgSuspend: PatternOrgSuspend, opOrgState: PatternOrgState, opOrgOnboard: PatternOrgOnboard, opOrgCreate: PatternOrgCreate,
	opServiceAccountCreate: PatternServiceAccountCreate, opServiceAccountDisable: PatternServiceAccountDisable,
	opServiceAccountEnable: PatternServiceAccountEnable, opServiceAccountOwner: PatternServiceAccountOwner,
	opServiceAccountKeyIssue: PatternServiceAccountKeyIssue, opServiceAccountKeyRotate: PatternServiceAccountKeyRotate,
	opServiceAccountKeyRevoke: PatternServiceAccountKeyRevoke, opServiceAccountKeyList: PatternServiceAccountKeyList,
}

// serviceSessionIssuerClass is the endpoint-profile credential class of the
// session issuer credential (svc_authsess_) the routes take.
const serviceSessionIssuerClass = "internal_svc_authsess_token"

var errProvisioningRows = errors.New("authapi: the provisioning routes need their endpoint-profile rows")

// provisioningRowsPresent refuses the provisioning routes unless the
// endpoint-profile document (the one this binary embeds) holds, for each
// route, an auth-service row accepting every credential class the route
// admits (admittedClasses): the switch may turn the routes on only in a build
// that profiles them. The error names the route only.
func provisioningRowsPresent(raw []byte, workload bool) error {
	for _, pattern := range provisioningPatterns {
		if err := rowsAcceptFor(raw, []string{pattern}, admittedClasses(pattern, workload)); err != nil {
			return err
		}
	}
	return nil
}

// admittedClasses are the endpoint-profile credential classes the route on
// pattern admits: the session issuer credential, and the go-api workload
// token when the route's operation is a workload operation and the workload
// path is configured.
func admittedClasses(pattern string, workload bool) []string {
	classes := []string{serviceSessionIssuerClass}
	for op, route := range provisioningRouteOps {
		if route == pattern && workload && workloadOperations[op] {
			classes = append(classes, workloadTokenClass)
		}
	}
	return classes
}

// workloadTokenClass is the endpoint-profile credential class of the go-api
// workload token (JOSE typ workload+jwt).
const workloadTokenClass = "workload_token"

// rowsAcceptFor refuses unless the document holds, for each pattern, an
// auth-service POST row whose accepted classes include every class in classes,
// and classes is not empty: a route mounts only on a row that names every
// credential class its handler admits.
func rowsAcceptFor(raw []byte, patterns, classes []string) error {
	if len(classes) == 0 {
		return fmt.Errorf("%w: no credential class to admit", errProvisioningRows)
	}
	var document struct {
		Rows []struct {
			Service         string   `json:"service"`
			Method          string   `json:"method"`
			Route           string   `json:"route"`
			AcceptedClasses []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return fmt.Errorf("%w: the document does not parse", errProvisioningRows)
	}
	for _, pattern := range patterns {
		found := false
		for _, row := range document.Rows {
			if row.Service == AccessService && row.Method == "POST" && row.Route == pattern &&
				containsAll(row.AcceptedClasses, classes) {
				found = true
				break
			}
		}
		if !found {
			return &ProvisioningRowError{Route: "POST " + pattern, Classes: slices.Clone(classes)}
		}
	}
	return nil
}

// ProvisioningRowError is the rows gate's refusal for one route: the route (method
// and pattern) and the credential classes its endpoint-profile row must list.
// It names no value. auth-service's start log reads DependencyReason and
// DependencyAttrs, so a short document is diagnosed from the log alone.
type ProvisioningRowError struct {
	Route   string
	Classes []string
}

// ProvisioningRowReason is the start-log reason of a ProvisioningRowError.
const ProvisioningRowReason = "auth_provisioning_profile_row_missing"

func (e *ProvisioningRowError) Error() string {
	return fmt.Sprintf("%v: %s has no row accepting %v", errProvisioningRows, e.Route, e.Classes)
}

func (e *ProvisioningRowError) Unwrap() error { return errProvisioningRows }

// DependencyReason is the bounded start-log reason.
func (e *ProvisioningRowError) DependencyReason() string { return ProvisioningRowReason }

// DependencyAttrs names the route and the classes for the start log.
func (e *ProvisioningRowError) DependencyAttrs() []any {
	// required_classes, not credential_classes: the log redacts every key with a
	// protected word (credential, token, ...), and these are class names only.
	return []any{"route", e.Route, "required_classes", slices.Clone(e.Classes)}
}

func containsAll(have, want []string) bool {
	for _, class := range want {
		if !slices.Contains(have, class) {
			return false
		}
	}
	return true
}
