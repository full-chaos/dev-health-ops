//go:build integration

package lockorder

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// The cells below hold the refresh of an org-scoped session or token to the
// membership it was minted for (D6507): with the membership gone (removed or
// left) a non-superuser's refresh is refused 401 and the session or token
// family revoked, with an audit row, on both planes (go-api's own refresh,
// and auth-service's when it owns sessions). A superuser keeps refreshing,
// and a session minted by an invite accept or an onboard join refreshes.

// cut keeps a failure message free of whole token bodies.
func cut(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
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
	bearer, err := e.token(user, false)
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
		t.Fatalf("%s: fixture: join answered %d %s", cell, got.status, cut(got.raw))
	}
	return refresh
}

func (e *env) refreshAt(ctx context.Context, base, token string) response {
	return e.post(ctx, base, "/api/v1/auth/refresh", "", map[string]any{"refresh_token": token})
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
		t.Fatalf("%s: refresh answer: %d %s, want 401 Invalid or expired refresh token", cell, got.status, cut(got.raw))
	}
}

func wantRefreshOK(t *testing.T, cell string, got response) {
	t.Helper()
	if got.status != http.StatusOK {
		t.Fatalf("%s: refresh answer: %d %s, want 200", cell, got.status, cut(got.raw))
	}
}

// Auth plane: a session issued by an invite accept refreshes while the
// membership exists; once go-api's admin removed the member, the refresh is
// refused, the session revoked (session_revoked) and one revoke audit row
// written. A superuser in the same position keeps refreshing.
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
				t.Fatalf("%s: fixture: remove_member answered %d %s", cell, removed.status, cut(removed.raw))
			}

			got := e.refreshAt(ctx, e.authServer.URL, refresh)
			live := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE principal_id = $1 AND revoked_at IS NULL`, user)
			revoked := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE principal_id = $1 AND revoked_reason = 'session_revoked'`, user)
			audits := e.scalar(t, ctx, `SELECT count(*)::text FROM auth.security_audit_events
WHERE event_type = 'auth.session.revoked' AND subject_principal_id = $1`, user)
			if superuser {
				wantRefreshOK(t, cell, got)
				if revoked != "0" || audits != "0" {
					t.Fatalf("%s: a superuser's session was revoked (%s) or audited (%s)", cell, revoked, audits)
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

// go-api plane: a token from an invite accept; the membership row is removed
// with a raw DELETE (so no removal path revokes the token first): the refresh
// is refused, the token's family revoked and one login_failed audit row
// written. A superuser in the same position keeps refreshing.
func TestGoAPIRefreshOfARemovedMemberIsRefusedAndRevoked(t *testing.T) {
	for _, superuser := range []bool{false, true} {
		name := "member"
		if superuser {
			name = "superuser"
		}
		t.Run(name, func(t *testing.T) {
			cell := "go-api-refresh/removed/" + name
			e, ctx := getEnv(t), cellContext(t)
			e.cleanup(t)
			org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner", nil)
			user := e.seedUser(t, ctx)
			if superuser {
				user = e.seedSuperuser(t, ctx)
			}
			refresh := e.join(t, ctx, cell, e.server.URL, user, org, owner, false)
			if _, err := e.admin.Exec(ctx, `DELETE FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user); err != nil {
				t.Fatal(err)
			}

			got := e.refreshAt(ctx, e.server.URL, refresh)
			live := e.scalar(t, ctx, `SELECT count(*)::text FROM public.refresh_tokens WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL`, user, org)
			audits := e.scalar(t, ctx, `SELECT count(*)::text FROM public.audit_logs
WHERE user_id = $1 AND org_id = $2 AND action = 'login_failed'`, user, org)
			if superuser {
				wantRefreshOK(t, cell, got)
				if audits != "0" {
					t.Fatalf("%s: %s refusal audit rows for a superuser", cell, audits)
				}
				return
			}
			wantRefreshRefused(t, cell, got)
			if live != "0" || audits != "1" {
				t.Fatalf("%s: live refresh tokens=%s login_failed rows=%s, want 0, 1", cell, live, audits)
			}
		})
	}
}

// F2: go-api's admin removal revokes the member's live refresh tokens for that
// organization in its own transaction, and no other organization's.
func TestGoAPIMemberRemovalRevokesTheOrgsRefreshTokens(t *testing.T) {
	const cell = "go-api-remove/revokes-tokens"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org1, org2, owner := e.seedOrg(t, ctx), e.seedOrg(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org1, owner, "owner", nil)
	e.add(t, ctx, org2, owner, "owner", nil)
	user := e.seedUser(t, ctx)
	refresh1 := e.join(t, ctx, cell, e.server.URL, user, org1, owner, false)
	refresh2 := e.join(t, ctx, cell, e.server.URL, user, org2, owner, false)

	removed := e.call(ctx, e.root, http.MethodDelete, "/api/v1/admin/orgs/"+org1.String()+"/members/"+user.String(), nil)
	if removed.status != http.StatusOK {
		t.Fatalf("%s: remove_member answered %d %s", cell, removed.status, cut(removed.raw))
	}
	live := func(org uuid.UUID) string {
		return e.scalar(t, ctx, `SELECT count(*)::text FROM public.refresh_tokens WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL`, user, org)
	}
	if got := live(org1); got != "0" {
		t.Fatalf("%s: removal answer: %s live refresh tokens for the removed org, want 0", cell, got)
	}
	if got := live(org2); got == "0" {
		t.Fatalf("%s: removal answer: the other org's refresh tokens were revoked too", cell)
	}
	if got := e.refreshAt(ctx, e.server.URL, refresh1); got.status != http.StatusUnauthorized {
		t.Fatalf("%s: refresh of the removed org's token: %d %s, want 401", cell, got.status, cut(got.raw))
	}
	wantRefreshOK(t, cell, e.refreshAt(ctx, e.server.URL, refresh2))
}

// No false refusal: a session or token minted right after the membership
// insert (invite accept, onboard join) refreshes, on both planes.
func TestRefreshAfterAJoinIsNotRefused(t *testing.T) {
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	for _, plane := range []struct {
		name, joinAt, refreshAt string
	}{
		{"go-api", e.server.URL, e.server.URL},
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
