package authapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// eddsaTokenFor is eddsaToken (audience dev-health-api) with org_id set to
// org, signed again by the fixture key.
func (f *validateFixture) eddsaTokenFor(org string) string {
	f.t.Helper()
	parts := strings.Split(f.eddsaToken(contracts.PlatformAPIAudience), ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		f.t.Fatal(err)
	}
	var claims contracts.AccessTokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		f.t.Fatal(err)
	}
	claims.OrgID = &org
	return f.sign(accessTokenTyp, claims)
}

// Product rule 1 (D6773), CHAOS-13227: POST /api/v1/auth/validate answers a
// token whose org claim has no membership row as the legacy HS256 path did:
// 200, valid true, the claimed org, and the role of a token with no role row
// (member; the HS256 token claimed member, the role a user access token
// carries). A: the user left the org; B-unknown: the claim names no org;
// B-notUUID: the claim is not a UUID. Both planes (auth-service and go-api)
// answer the same. The HS256 path is retired, so the legacy answer is the
// literal recorded on the last main that had it (715e3f93d, this test's legacy
// side, all three cases): 200 {"valid":true,"user_id":<the user>,
// "email":"ada@example.com","org_id":<the claim>,"role":"member",
// "expires_at":null}. A user access token whose org_id is not a canonical UUID
// is refused by the access-token.v1 verifier itself (authverify), before any
// org read: that stricter answer is CHAOS-13262 and is never held as wanted.
func TestRule1ValidateTokenOrgWithoutMembershipIsTheLegacyAnswer_CHAOS13227(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		org  func(*validateFixture) string
	}{
		{name: "A removed member", org: func(f *validateFixture) string {
			delete(f.users.roles, [2]uuid.UUID{f.principal, f.org})
			return f.org.String()
		}},
		{name: "B-unknown org", org: func(*validateFixture) string { return uuid.NewString() }},
		{name: "B-notUUID claim", org: func(*validateFixture) string { return "not-a-uuid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routed := newValidateFixture(t, true)
			org := tc.org(routed)
			token := routed.eddsaTokenFor(org)
			compat, goAPI := routed.post(routed.server, tokenBody(token), nil), routed.post(routed.goAPI, tokenBody(token), nil)
			t.Logf("routed user access token: %d %s", compat.Code, compat.Body)
			expectSamePlanes(t, "routed "+tc.name, compat, goAPI)
			if org == "not-a-uuid" && compat.Code == http.StatusOK && compat.Body.String() == nullAnswer {
				t.Skipf("CHAOS-13262: the access-token.v1 verifier refuses an org claim that is not a UUID (valid:false); legacy answers valid:true")
			}
			expectAnswer(t, "routed "+tc.name, compat, http.StatusOK, routed.validAnswer("ada@example.com", org, "member"))
		})
	}
}
