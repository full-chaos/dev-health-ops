//go:build integration

package sso_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A GitLab numeric id is unique per instance. A self-hosted user and a gitlab.com user with the same id are two
// identities: the self-hosted sign-in never resolves to the gitlab.com account, even when that account is a member of
// the provider's organization.
func TestOAuthSelfHostedGitLabIDIsNotTheGitLabComAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.userID = "77"
	gitlab.email = "self.hosted.user@allowed.example"
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
		clientSecretEncryptedFallback: "gitlab-secret",
	})
	const socialEmail = "gitlab.com.user@allowed.example"
	seedAccount(t, ctx, st.pool, socialEmail, "gitlab", "77")
	if _, err := st.pool.Exec(ctx, `UPDATE users SET sso_provider_id = NULL WHERE email = $1`, socialEmail); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
SELECT $1, $2, id, 'member', now(), now(), now() FROM users WHERE email = $3`, uuid.New(), orgID, socialEmail); err != nil {
		t.Fatal(err)
	}

	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	status, body := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
	if status != http.StatusOK {
		t.Fatalf("callback: status=%d body=%v", status, body)
	}
	if body["email"] != gitlab.email {
		t.Fatalf("self-hosted sign-in resolved to email %v, want its own %s", body["email"], gitlab.email)
	}
	var accounts, bound int
	if err := st.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE sso_provider_id = $1) FROM users WHERE auth_provider = 'gitlab' AND auth_provider_id = '77'`,
		providerID).Scan(&accounts, &bound); err != nil {
		t.Fatal(err)
	}
	if accounts != 2 || bound != 1 {
		t.Fatalf("gitlab accounts with id 77 = %d (bound to the instance %d), want 2 and 1", accounts, bound)
	}
}
