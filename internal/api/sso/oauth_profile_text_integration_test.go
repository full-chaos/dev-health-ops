//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-11678: a lone surrogate in the free text of an OAuth profile, and the callback's join + user update.

func oauthProfileWith(id, email, name, avatar string) string {
	return `{"id": "` + id + `", "email": "` + email + `", "username": "gluser", "name": "` + name + `", "avatar_url": "` + avatar +
		`", "confirmed_at": "2026-01-01T00:00:00Z"}`
}

// oauthCallbackOnce runs one authorize + callback round trip against the provider and returns the callback answer.
func oauthCallbackOnce(t *testing.T, st stack, providerID uuid.UUID) (int, map[string]any) {
	t.Helper()
	client := newCookieClient(t)
	status, authResp := postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/authorize", map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("authorize: %d %v", status, authResp)
	}
	return postJSONWithClient(t, client, st.server, "/api/v1/auth/oauth/"+providerID.String()+"/callback",
		map[string]any{"code": "c", "state": authResp["state"]})
}

func oauthStack(t *testing.T, ctx context.Context, profile string) (stack, uuid.UUID, uuid.UUID, *fakeGitLab) {
	t.Helper()
	st := startStack(t, ctx)
	orgID := seedOrg(t, ctx, st.pool, "enterprise")
	gitlab := startFakeGitLab(t)
	gitlab.rawUser = profile
	providerID := seedProvider(t, ctx, st.pool, orgID, providerOpts{
		protocol: "oauth_gitlab", status: "active", config: gitlabConfig(gitlab.server.URL), autoProvision: true,
	})
	return st, orgID, providerID, gitlab
}

func memberships(t *testing.T, ctx context.Context, st stack, email string) int {
	t.Helper()
	var n int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM memberships m JOIN users u ON u.id = m.user_id WHERE u.email = $1`, email).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func storedText(t *testing.T, ctx context.Context, st stack, email string) (name, avatar string) {
	t.Helper()
	var n, a *string
	if err := st.pool.QueryRow(ctx, `SELECT full_name, avatar_url FROM users WHERE email = $1`, email).Scan(&n, &a); err != nil {
		t.Fatalf("read the user: %v", err)
	}
	if n != nil {
		name = *n
	}
	if a != nil {
		avatar = *a
	}
	return name, avatar
}

// An existing user with no membership, an empty stored name and avatar, a profile name and avatar with a lone
// surrogate: the sign-in succeeds twice, the stored text carries U+FFFD, and the user joined exactly once.
func TestOAuthCallbackExistingUserWithASurrogateInTheProfileText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const email = "surrogate.existing@example.test"
	st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith("42", email, `Ann\ud800`, `https://a.example/\ud800`))
	seedAccount(t, ctx, st.pool, email, "gitlab", "42")
	var report []string
	failed := false
	for attempt := 1; attempt <= 2; attempt++ {
		status, body := oauthCallbackOnce(t, st, providerID)
		joined := memberships(t, ctx, st, email)
		report = append(report, fmt.Sprintf("callback %d: status=%d memberships=%d body=%v", attempt, status, joined, body))
		failed = failed || status != http.StatusOK || joined != 1
	}
	if failed {
		t.Fatalf("want 200 and exactly one membership at both callbacks:\n%s", strings.Join(report, "\n"))
	}
	if name, avatar := storedText(t, ctx, st, email); name != "Ann�" || avatar != "https://a.example/�" {
		t.Fatalf("stored name %q avatar %q, want the surrogates replaced by U+FFFD", name, avatar)
	}
}

// A new user with the same profile text is created with the U+FFFD text (it was a 500).
func TestOAuthCallbackNewUserWithASurrogateInTheProfileText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const email = "surrogate.new@example.test"
	st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith("43", email, `Bob\ud800`, `https://a.example/\ud800`))
	status, body := oauthCallbackOnce(t, st, providerID)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v, want 200", status, body)
	}
	if name, avatar := storedText(t, ctx, st, email); name != "Bob�" || avatar != "https://a.example/�" {
		t.Fatalf("stored name %q avatar %q, want the surrogates replaced by U+FFFD", name, avatar)
	}
	if n := memberships(t, ctx, st, email); n != 1 {
		t.Fatalf("memberships = %d, want 1", n)
	}
}

// A lone surrogate in the email is refused by the provider email rule before the verified-email check: 400
// invalid_provider_email, one "provisioning" denial row, nothing else written.
func TestOAuthCallbackRefusesASurrogateInTheEmail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	st, orgID, providerID, _ := oauthStack(t, ctx, oauthProfileWith("44", `e\ud800@example.test`, "Cy", ""))
	before := snapshotRows(t, ctx, st)
	status, body := oauthCallbackOnce(t, st, providerID)
	detail, _ := body["detail"].(map[string]any)
	if status != http.StatusBadRequest || detail["reason"] != "invalid_provider_email" {
		t.Fatalf("status=%d body=%v, want 400 invalid_provider_email", status, body)
	}
	assertSSOAuditStage(t, ctx, st.pool, orgID, "provisioning")
	assertNoRowsWritten(t, ctx, st, before)
}

// A failing user UPDATE rolls the join back: no membership is left behind and a later sign-in is not stuck.
func TestOAuthCallbackForcedUserUpdateFailureLeavesNoMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const email = "forced.failure@example.test"
	st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith("45", email, "Dee", ""))
	seedAccount(t, ctx, st.pool, email, "gitlab", "45")
	for _, statement := range []string{
		`CREATE FUNCTION fail_user_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'user update refused'; END $$`,
		`CREATE TRIGGER fail_user_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION fail_user_update()`,
	} {
		if _, err := st.pool.Exec(ctx, statement); err != nil {
			t.Fatalf("install failure trigger: %v", err)
		}
	}
	for attempt := 1; attempt <= 2; attempt++ {
		status, body := oauthCallbackOnce(t, st, providerID)
		if status != http.StatusInternalServerError {
			t.Fatalf("callback %d: status=%d body=%v, want 500", attempt, status, body)
		}
		if n := memberships(t, ctx, st, email); n != 0 {
			t.Fatalf("callback %d: memberships = %d after a failed user update, want 0", attempt, n)
		}
	}
	if _, err := st.pool.Exec(ctx, `DROP TRIGGER fail_user_update ON users`); err != nil {
		t.Fatal(err)
	}
	if status, body := oauthCallbackOnce(t, st, providerID); status != http.StatusOK {
		t.Fatalf("after the failure is gone: status=%d body=%v, want 200", status, body)
	}
}

// A provider subject with a lone surrogate is refused with the invalid_provider_id answer (the answer social login
// gives) BEFORE any lookup: PostgreSQL would refuse the bytes in the identity query (a 500). New user or existing,
// nothing is written and the provider row is not marked failed.
func TestOAuthCallbackRefusesASurrogateProviderIDBeforeTheLookup(t *testing.T) {
	for name, seed := range map[string]bool{"new user": false, "existing user": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			const email = "surrogate.subject@example.test"
			st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith(`7\ud800`, email, "Eve", ""))
			if seed {
				seedAccount(t, ctx, st.pool, email, "gitlab", "42")
			}
			before := snapshotRows(t, ctx, st)
			status, body := oauthCallbackOnce(t, st, providerID)
			detail, _ := body["detail"].(map[string]any)
			if status != http.StatusBadRequest || detail["reason"] != "invalid_provider_id" {
				t.Fatalf("status=%d body=%v, want 400 with reason invalid_provider_id", status, body)
			}
			assertNoRowsWritten(t, ctx, st, before)
			assertProviderRowUnchanged(t, ctx, st.pool, providerID, "active")
		})
	}
}

// An EMPTY provider subject is refused 400 invalid_provider_id before any lookup, whether or not another account holds
// the email: the answer does not show whether an account exists.
func TestOAuthCallbackEmptyProviderIDIsRefused(t *testing.T) {
	for name, held := range map[string]bool{"new account": false, "email held by another account": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			const email = "empty.subject@example.test"
			st, _, providerID, _ := oauthStack(t, ctx, oauthProfileWith("", email, "Fay", ""))
			if held {
				seedAccount(t, ctx, st.pool, email, "gitlab", "42")
			}
			before := snapshotRows(t, ctx, st)
			status, body := oauthCallbackOnce(t, st, providerID)
			detail, _ := body["detail"].(map[string]any)
			if status != http.StatusBadRequest || detail["reason"] != "invalid_provider_id" {
				t.Fatalf("status=%d body=%v, want 400 with reason invalid_provider_id", status, body)
			}
			assertNoRowsWritten(t, ctx, st, before)
		})
	}
}
