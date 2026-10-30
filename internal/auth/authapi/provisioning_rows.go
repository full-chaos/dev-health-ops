package authapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// provisioningPatterns are the routes AUTH_MEMBERSHIP_ROUTES mounts.
var provisioningPatterns = []string{PatternMembershipAdd, PatternMembershipRole, PatternMembershipRemove,
	PatternMembershipTransfer, PatternInviteCreate, PatternInviteAccept, PatternOrgDeprovision, PatternInviteRevoke}

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

// darkPatterns are the routes mounted only when the document holds their row
// (the rest of provisioningPatterns refuse the whole switch without one): the
// reactivate route's profile row is added by a follow-up, and until then the
// route is not mounted while the other routes are unaffected.
var darkPatterns = []string{PatternOrgReactivate}

// rowsPresentFor is provisioningRowsPresent over patterns.
func rowsPresentFor(raw []byte, patterns []string) error {
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
				slices.Contains(row.AcceptedClasses, serviceSessionIssuerClass) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: POST %s has no row accepting %s", errProvisioningRows, pattern, serviceSessionIssuerClass)
		}
	}
	return nil
}
