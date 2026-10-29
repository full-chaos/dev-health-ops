package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Classes []struct {
			ClassID string   `json:"class_id"`
			Gaps    []string `json:"gaps"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, class := range document.Classes {
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
