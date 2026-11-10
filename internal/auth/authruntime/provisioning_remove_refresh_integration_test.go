//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// With AUTH_PROXY_ORG_PROVISIONING on, go-api's member removal is the remove
// call of its provisioning client. After it, a refresh of the removed
// principal's session of that organization is refused on both refresh routes
// and the sessions are revoked, for a superuser as for a member: the answer of
// the legacy go-api path (orgstore.go revokes the org's refresh tokens on
// removal; lockorder TestGoAPIRevokedTokenKeepsTheReuseAnswer). With the switch
// off a superuser keeps refreshing (lockorder
// TestAuthRefreshOfARemovedMemberIsRefusedAndRevoked, CHAOS-13212, D6787).
func TestProvisioningClientRemoveRefusesTheRemovedPrincipalsRefresh(t *testing.T) {
	for _, superuser := range []bool{false, true} {
		name := "member"
		if superuser {
			name = "superuser"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			w := newWorkloadProvisioningEnv(t, ctx)
			e := w.routesEnv
			// The session issue route's audit row names the go-api workload principal.
			if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'go-api-test')`, w.workload.String()); err != nil {
				t.Fatalf("seed the workload principal: %v", err)
			}
			_, org := e.seed(t, ctx)
			// A platform admin acts, as the legacy cell's root does: only a superuser may change a superuser's membership.
			actor := e.account(t, ctx, acctMail(), true, true)
			user := e.account(t, ctx, acctMail(), superuser, true)
			header, _ := w.mint(t, ctx, nil)
			header["Content-Type"] = "application/json"
			e.addVia(t, header, org, user, actor, "member")

			issue := func() string {
				t.Helper()
				minted, _ := w.mint(t, ctx, nil)
				minted["Content-Type"] = "application/json"
				got := e.doWith(t, "/internal/v1/sessions", `{"principal_id":"`+user.String()+`","organization_id":"`+org.String()+`","amr":["pwd"]}`, minted)
				if got.status != http.StatusOK {
					t.Fatalf("fixture: session issue = %d %s", got.status, got.body)
				}
				refresh, _ := object(t, got)["refresh_token"].(string)
				if refresh == "" {
					t.Fatal("fixture: session issue answered no refresh_token")
				}
				return refresh
			}
			compat := func(token string) answer {
				return e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+token+`"}`, compatClient())
			}
			v1 := func(credential string) answer {
				return e.doWith(t, "/v1/token/refresh", `{"schema_version":"refresh-request.v1","refresh_credential":"`+credential+`"}`, compatClient())
			}

			compatToken, v1Credential := issue(), issue()
			before := compat(compatToken)
			if before.status != http.StatusOK {
				t.Fatalf("fixture: refresh before the removal = %d %s", before.status, before.body)
			}
			compatToken, _ = object(t, before)["refresh_token"].(string)

			if err := proxyClient(t, e, header).RemoveMember(ctx, org, user, actor); err != nil {
				t.Fatalf("remove through go-api's client: %v", err)
			}
			if pub, auth := e.memberRows(t, ctx, org, user); pub != "" || auth != "removed" {
				t.Fatalf("after remove: public %q auth %q, want no public row and auth removed", pub, auth)
			}

			if got := compat(compatToken); got.status != http.StatusUnauthorized || !strings.Contains(string(got.body), "Invalid or expired refresh token") {
				t.Fatalf("compat refresh after the removal = %d %s, want 401 Invalid or expired refresh token", got.status, got.body)
			}
			if got := v1(v1Credential); got.status != http.StatusUnauthorized {
				t.Fatalf("v1 refresh after the removal = %d %s, want 401", got.status, got.body)
			}
			var live, credentials int
			if err := e.admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2 AND revoked_at IS NULL),
  (SELECT count(*) FROM auth.refresh_credentials r JOIN auth.sessions s ON s.id = r.session_id
    WHERE s.principal_id = $1 AND s.organization_id = $2 AND r.revoked_at IS NULL AND r.consumed_at IS NULL)`, user, org).
				Scan(&live, &credentials); err != nil {
				t.Fatal(err)
			}
			if live != 0 || credentials != 0 {
				t.Fatalf("after the removal: %d live sessions, %d live refresh credentials of the organization; want 0, 0", live, credentials)
			}
			if n := w.auditRows(t, ctx, "auth.membership.removed", org, &user); n != 1 {
				t.Fatalf("auth.membership.removed audit rows = %d, want 1", n)
			}
		})
	}
}
