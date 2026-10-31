package authapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

const changeOrgBody = `{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`

func accessOrg(t *testing.T, body []byte) string {
	t.Helper()
	var answer struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(answer.AccessToken, ".")
	if len(parts) != 3 {
		t.Fatalf("access token has %d parts", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	org, _ := claims["org_id"].(string)
	return org
}

// C6: a membership that exists when the answer is built but not on the
// issued row: the token follows the row.
func TestC6ChangePasswordTokenOrganizationFollowsTheIssuedRow(t *testing.T) {
	c := newCredFixture(t)
	c.fakes.rowNoOrg = true
	r := c.call(http.MethodPost, PatternChangePassword, changeOrgBody, c.bearer(c.grant()))
	if r.Code != http.StatusOK {
		t.Fatalf("change = %d %s", r.Code, r.Body)
	}
	if org := accessOrg(t, r.Body.Bytes()); org != "" {
		t.Fatalf("token org_id = %q, want none: the issued row has no organization", org)
	}
}

// C2 at the answer: the row keeps the organization, the token carries it.
func TestC2ChangePasswordTokenCarriesTheIssuedRowOrganization(t *testing.T) {
	c := newCredFixture(t)
	r := c.call(http.MethodPost, PatternChangePassword, changeOrgBody, c.bearer(c.grant()))
	if r.Code != http.StatusOK {
		t.Fatalf("change = %d %s", r.Code, r.Body)
	}
	if org := accessOrg(t, r.Body.Bytes()); org != c.org.String() {
		t.Fatalf("token org_id = %q, want %q", org, c.org)
	}
}
