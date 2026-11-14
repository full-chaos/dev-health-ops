//go:build integration

package lockorder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// The cells below hold the refresh of an org-scoped session or token to the
// membership it was minted for (D6507): with the membership gone (removed or
// left) a non-superuser's refresh is refused 401 and the session or token
// family revoked, with an audit row, on the auth plane (go-api's own refresh,
// and auth-service's when it owns sessions). A session minted by an invite
// accept or an onboard join refreshes. The superuser cells pin the answer with
// AUTH_PROXY_ORG_PROVISIONING off, which prod has not run since the org
// provisioning flip (deploy values.prod.yaml:1378, orgProvisioning: true): the
// superuser keeps refreshing on the auth plane, WIDER than the legacy go-api
// path, which revokes the org's refresh tokens on removal. That is a rule-1 gap
// of the switch-off path only (CHAOS-13212, D6787). The switch-on answer, the
// legacy one, is pinned by authruntime
// TestProvisioningClientRemoveRefusesTheRemovedPrincipalsRefresh and
// TestProvisioningRemoveEndsTheMembersSessionsOfTheOrganization.

// shape is a failure message's view of an answer: its status, the keys of
// its JSON body and the refusal detail; never a token or other value.
func shape(r response) string {
	keys := make([]string, 0, len(r.body))
	for k := range r.body {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	detail := ""
	if d, ok := r.body["detail"]; ok {
		if encoded, err := json.Marshal(d); err == nil && len(encoded) <= 120 {
			detail = " detail=" + string(encoded)
		}
	}
	return fmt.Sprintf("%d keys=%v%s", r.status, keys, detail)
}

func (e *env) post(ctx context.Context, base, path, bearer string, body any) response {
	encoded, err := json.Marshal(body)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(encoded))
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return response{status: -1, raw: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	out := response{status: resp.StatusCode, raw: string(raw)}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

// join accepts an invite to org for user on base (accept-invite, or the
// onboard join when onboard is set) and returns the refresh token it answered.
func (e *env) join(t *testing.T, ctx context.Context, cell, base string, user, org, inviter uuid.UUID, onboard bool) string {
	t.Helper()
	_, token := e.goInvite(t, ctx, org, inviter, email(user))
	bearer, err := e.token(user)
	if err != nil {
		t.Fatal(err)
	}
	var got response
	if onboard {
		got = e.post(ctx, base, "/api/v1/auth/onboard", bearer, map[string]any{"action": "join_org", "invite_code": token})
	} else {
		got = e.post(ctx, base, "/api/v1/auth/accept-invite", bearer, map[string]any{"token": token})
	}
	refresh, _ := got.body["refresh_token"].(string)
	if got.status != http.StatusOK || refresh == "" {
		t.Fatalf("%s: fixture: join answered %s", cell, shape(got))
	}
	return refresh
}

func (e *env) refreshAt(ctx context.Context, base, token string) response {
	return e.post(ctx, base, "/api/v1/auth/refresh", "", map[string]any{"refresh_token": token})
}

// refreshV1 is auth-service's own refresh, POST /v1/token/refresh.
func (e *env) refreshV1(ctx context.Context, base, credential string) response {
	return e.post(ctx, base, "/v1/token/refresh", "", map[string]any{"schema_version": "refresh-request.v1", "refresh_credential": credential})
}

func (e *env) seedSuperuser(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := e.seedUser(t, ctx)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_superuser = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func wantRefreshRefused(t *testing.T, cell string, got response) {
	t.Helper()
	if got.status != http.StatusUnauthorized || !strings.Contains(got.raw, "Invalid or expired refresh token") {
		t.Fatalf("%s: refresh answer: %s, want 401 Invalid or expired refresh token", cell, shape(got))
	}
}

func wantRefreshOK(t *testing.T, cell string, got response) {
	t.Helper()
	if got.status != http.StatusOK {
		t.Fatalf("%s: refresh answer: %s, want 200", cell, shape(got))
	}
}

// Auth plane: a session issued by an invite accept refreshes while the
// membership exists; once go-api's admin removed the member, the refresh is
// refused, the session revoked (session_revoked) and one revoke audit row
// written. A superuser in the same position keeps refreshing: the switch-off
// answer only (AUTH_PROXY_ORG_PROVISIONING off, not prod), WIDER than legacy
// (CHAOS-13212); see the file comment for the switch-on cells.
func TestAuthRefreshOfARemovedMemberIsRefusedAndRevoked(t *testing.T) {
	for _, superuser := range []bool{false, true} {
		name := "member"
		if superuser {
			name = "superuser"
		}
		t.Run(name, func(t *testing.T) {
			cell := "auth-refresh/removed/" + name
			e, ctx := getEnv(t), cellContext(t)
			e.cleanup(t)
			org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner", nil)
			user := e.seedUser(t, ctx)
			if superuser {
				user = e.seedSuperuser(t, ctx)
			}
			refresh := e.join(t, ctx, cell, e.issuerServer.URL, user, org, owner, false)
			removed := e.call(ctx, e.root, http.MethodDelete, "/api/v1/admin/orgs/"+org.String()+"/members/"+user.String(), nil)
			if removed.status != http.StatusOK {
				t.Fatalf("%s: fixture: remove_member answered %s", cell, shape(removed))
			}

			got := e.refreshAt(ctx, e.authServer.URL, refresh)
			live := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE principal_id = $1 AND revoked_at IS NULL`, user)
			revoked := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE principal_id = $1 AND revoked_reason = 'session_revoked'`, user)
			audits := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.security_audit_events
WHERE event_type = 'auth.session.revoked' AND subject_principal_id = $1`, user)
			if superuser {
				wantRefreshOK(t, cell, got)
				if revoked != "0" || audits != "0" {
					t.Fatalf("%s: a superuser's session was revoked (%s) or audited (%s); the switch-off answer is 200, nothing revoked", cell, revoked, audits)
				}
				return
			}
			wantRefreshRefused(t, cell, got)
			if live != "0" || revoked != "1" || audits != "1" {
				t.Fatalf("%s: sessions live=%s revoked(session_revoked)=%s revoke audit rows=%s, want 0, 1, 1", cell, live, revoked, audits)
			}
		})
	}
}

// Auth plane, v1 refresh: once go-api's admin removed the member, the refresh
// is refused before the credential rotates: no successor row, no presented
// row consumed, and the session revoked (session_revoked). A superuser in the
// same position rotates: the switch-off answer only (AUTH_PROXY_ORG_PROVISIONING
// off, not prod), WIDER than legacy (CHAOS-13212); see the file comment.
func TestAuthV1RefreshOfARemovedMemberRotatesNothing(t *testing.T) {
	for _, superuser := range []bool{false, true} {
		name := "member"
		if superuser {
			name = "superuser"
		}
		t.Run(name, func(t *testing.T) {
			cell := "auth-v1-refresh/removed/" + name
			e, ctx := getEnv(t), cellContext(t)
			e.cleanup(t)
			org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner", nil)
			user := e.seedUser(t, ctx)
			if superuser {
				user = e.seedSuperuser(t, ctx)
			}
			refresh := e.join(t, ctx, cell, e.issuerServer.URL, user, org, owner, false)
			removed := e.call(ctx, e.root, http.MethodDelete, "/api/v1/admin/orgs/"+org.String()+"/members/"+user.String(), nil)
			if removed.status != http.StatusOK {
				t.Fatalf("%s: fixture: remove_member answered %s", cell, shape(removed))
			}
			rows := func() string {
				return e.scalar(t, ctx, `SELECT count(*)::text || '/' || count(*) FILTER (WHERE consumed_at IS NOT NULL OR superseded_by IS NOT NULL)::text
FROM auth.refresh_credentials WHERE principal_id = $1`, user)
			}
			before := rows()

			got := e.refreshV1(ctx, e.authServer.URL, refresh)
			after := rows()
			live := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE principal_id = $1 AND revoked_at IS NULL`, user)
			revoked := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE principal_id = $1 AND revoked_reason = 'session_revoked'`, user)
			if superuser {
				if got.status != http.StatusOK || after == before || revoked != "0" {
					t.Fatalf("%s: refresh answer: %s, credential rows (all/consumed) %s -> %s, revoked %s; want 200, a rotation, nothing revoked",
						cell, shape(got), before, after, revoked)
				}
				return
			}
			if got.status != http.StatusUnauthorized || got.body["reason_code"] != "not_entitled" {
				t.Fatalf("%s: refresh answer: %s reason_code=%v, want 401 not_entitled", cell, shape(got), got.body["reason_code"])
			}
			if after != before {
				t.Fatalf("%s: credential rows (all/consumed) %s -> %s: the refused refresh rotated the credential", cell, before, after)
			}
			if live != "0" || revoked != "1" {
				t.Fatalf("%s: sessions live=%s revoked(session_revoked)=%s, want 0, 1", cell, live, revoked)
			}
		})
	}
}

// No false refusal: a session or token minted right after the membership
// insert (invite accept, onboard join) refreshes, on the auth plane.
func TestRefreshAfterAJoinIsNotRefused(t *testing.T) {
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	for _, plane := range []struct {
		name, joinAt, refreshAt string
	}{
		{"auth", e.issuerServer.URL, e.authServer.URL},
	} {
		for _, onboard := range []bool{false, true} {
			path := "accept-invite"
			if onboard {
				path = "onboard"
			}
			t.Run(plane.name+"/"+path, func(t *testing.T) {
				cell := "refresh-after-join/" + plane.name + "/" + path
				org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
				e.add(t, ctx, org, owner, "owner", nil)
				user := e.seedUser(t, ctx)
				refresh := e.join(t, ctx, cell, plane.joinAt, user, org, owner, onboard)
				wantRefreshOK(t, cell, e.refreshAt(ctx, plane.refreshAt, refresh))
			})
		}
	}
}

// On a one-connection pool (AUTH_DATABASE_MAX_CONNS=1) auth-service's refresh
// of an org-scoped member session ends: the refresh guard's membership read
// runs on the exchange transaction, never on a second connection.
func TestAuthRefreshEndsOnAOneConnectionPool(t *testing.T) {
	const cell = "auth-refresh/one-connection-pool"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	user := e.seedUser(t, ctx)
	e.add(t, ctx, org, user, "member", nil)
	pair, err := e.issueOne.Issue(ctx, httptest.NewRequest(http.MethodPost, "/api/v1/auth/accept-invite", nil),
		sessionissue.Request{PrincipalID: user, OrganizationID: &org, AMR: []string{"pwd"}})
	if err != nil {
		t.Fatalf("%s: fixture: session issue: %v", cell, err)
	}
	// A wait bound, not a timing assert: a refresh that needs a second
	// connection never ends, and the bound turns that into a named failure.
	bounded, cancel := context.WithTimeout(ctx, choreographyBound)
	defer cancel()
	got := e.refreshAt(bounded, e.authServerOne.URL, pair.Refresh)
	if got.status < 0 {
		t.Fatalf("%s: refresh answer: no answer within %s on a one-connection pool (%s)", cell, choreographyBound, got.raw)
	}
	wantRefreshOK(t, cell, got)
	successor, _ := got.body["refresh_token"].(string)
	v1 := e.refreshV1(bounded, e.authServerOne.URL, successor)
	if v1.status < 0 {
		t.Fatalf("%s: v1 refresh answer: no answer within %s on a one-connection pool (%s)", cell, choreographyBound, v1.raw)
	}
	if v1.status != http.StatusOK {
		t.Fatalf("%s: v1 refresh answer: %s, want 200", cell, shape(v1))
	}
}
