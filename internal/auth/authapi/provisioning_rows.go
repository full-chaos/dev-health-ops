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
// names the route only.
func provisioningRowsPresent(raw []byte) error {
	return rowsPresentFor(raw, provisioningPatterns)
}

// rowsPresentFor is provisioningRowsPresent over patterns.
func rowsPresentFor(raw []byte, patterns []string) error {
	return rowsAcceptFor(raw, patterns, []string{serviceSessionIssuerClass})
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
