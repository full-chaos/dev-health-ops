package contracts

import (
	"strings"
	"testing"
)

// The change-password route returns a user_access_token and a refresh credential; the row names both, and the class
// registry lists a validator for the access token.
func TestTheChangePasswordRowIssuesTheUserAccessToken(t *testing.T) {
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
		for _, class := range []string{"user_access_token", "refresh_credential"} {
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
