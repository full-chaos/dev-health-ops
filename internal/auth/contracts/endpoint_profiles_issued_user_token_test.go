package contracts

import (
	"strings"
	"testing"
)

// Under the eddsa access token class the change-password route returns a user_access_token; the row names it beside
// the HS256 token, and the class registry lists a validator for it.
func TestTheChangePasswordRowIssuesBothAccessTokenClasses(t *testing.T) {
	var found bool
	for _, row := range authServiceRows(t) {
		if id, _ := row["id"].(string); !strings.HasPrefix(id, "POST /api/v1/auth/change-password ") {
			continue
		}
		found = true
		issued := map[string]bool{}
		credentials, _ := row["issued_credential"].([]any)
		for _, item := range credentials {
			credential, _ := item.(map[string]any)
			class, _ := credential["class_id"].(string)
			issued[class] = true
		}
		for _, class := range []string{"ops_access_token_hs256", "user_access_token", "refresh_credential"} {
			if !issued[class] {
				t.Errorf("the change-password row does not issue %s: %v", class, issued)
			}
		}
	}
	if !found {
		t.Fatal("no change-password row")
	}
}

func TestTheUserAccessTokenClassNoLongerClaimsNoValidator(t *testing.T) {
	for _, class := range loadClassRegistryDocument(t).Classes {
		if class.ClassID != "user_access_token" {
			continue
		}
		for _, gap := range class.Gaps {
			if strings.HasPrefix(gap, "validators: none yet") {
				t.Errorf("the registry still says there is no validator: %q", gap)
			}
		}
		return
	}
	t.Fatal("no user_access_token class")
}
