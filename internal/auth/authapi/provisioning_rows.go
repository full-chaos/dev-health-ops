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
	PatternOrgSuspend}

// serviceSessionIssuerClass is the endpoint-profile credential class of the
// session issuer credential (svc_authsess_) the routes take.
const serviceSessionIssuerClass = "internal_svc_authsess_token"

var errProvisioningRows = errors.New("authapi: the provisioning routes need their endpoint-profile rows")

// provisioningRowsPresent refuses the provisioning routes unless the
// endpoint-profile document (the one this binary embeds) holds, for each
// route, an auth-service row accepting the session issuer credential: the
// switch may turn the routes on only in a build that profiles them. The error
// names the route only. It is provisioningRowsAccept without the go-api
// workload binding.
func provisioningRowsPresent(raw []byte) error {
	return provisioningRowsAccept(raw, false)
}

// provisioningRowsAccept is the startup gate of the provisioning routes: for
// each route, the document must hold an auth-service row that lists every
// credential class the route's handler admits (provisioningClasses). With the
// workload binding configured, a route whose operation takes the go-api
// workload token needs workload_token in its row too: a row may not
// under-describe the route it mounts.
func provisioningRowsAccept(raw []byte, workload bool) error {
	for _, pattern := range provisioningPatterns {
		op, ok := provisioningPatternOps[pattern]
		if !ok {
			return fmt.Errorf("%w: POST %s names no operation", errProvisioningRows, pattern)
		}
		if err := rowsAcceptFor(raw, []string{pattern}, provisioningClasses(op, workload)); err != nil {
			return err
		}
	}
	return nil
}

// provisioningPatternOps names each provisioning route's operation.
var provisioningPatternOps = map[string]string{
	PatternMembershipAdd: opMembershipAdd, PatternMembershipRole: opMembershipRole, PatternMembershipRemove: opMembershipRemove,
	PatternMembershipTransfer: opMembershipTransfer, PatternInviteCreate: opInviteCreate, PatternInviteAccept: opInviteAccept,
	PatternOrgDeprovision: opOrgDeprovision, PatternOrgReactivate: opOrgReactivate, PatternInviteRevoke: opInviteRevoke,
	PatternOrgSuspend: opOrgSuspend,
}

// provisioningClasses are the credential classes the handler of op admits
// (caller): the session issuer credential always, and the go-api workload
// token when the binding is configured and op is a workload operation.
func provisioningClasses(op string, workload bool) []string {
	classes := []string{serviceSessionIssuerClass}
	if workload && workloadOperations[op] {
		classes = append(classes, workloadTokenClass)
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
			return fmt.Errorf("%w: POST %s has no row accepting %v", errProvisioningRows, pattern, classes)
		}
	}
	return nil
}

func containsAll(have, want []string) bool {
	for _, class := range want {
		if !slices.Contains(have, class) {
			return false
		}
	}
	return true
}
