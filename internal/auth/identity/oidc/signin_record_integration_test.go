//go:build integration

package oidc

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// recordSignIns turns the real record on: its audit row needs a public.users row, which most harness cells do not seed.
func (h *harness) recordSignIns() {
	h.svc.cfg.SignInRecord = ssoaudit.PGSignIn{DB: h.env.runtime, Now: func() time.Time { return h.now }}
}

// lastLoginAtNow reports whether last_login_at and updated_at of the provider row are the harness time. A provider that
// was never signed in through has no last_login_at, so it reports false.
func (h *harness) lastLoginAtNow(t *testing.T, provider uuid.UUID) bool {
	t.Helper()
	var atNow bool
	if err := h.env.admin.QueryRow(context.Background(), `SELECT coalesce(last_login_at = $2 AND updated_at = $2, false)
FROM public.sso_providers WHERE id = $1`, provider, h.now.UTC()).Scan(&atNow); err != nil {
		t.Fatal(err)
	}
	return atNow
}

// successRows are this organization's success rows, the go-api marker rows aside: the database outlives a harness.
func (h *harness) successRows(t *testing.T) []auditRow {
	t.Helper()
	return h.auditRows(t, `org_id = $1 AND action = 'sso_login' AND status = 'success' AND resource_id NOT LIKE 'go-api-%'`, h.orgID)
}

// goAPISuccessRow writes, through go-api's own writer and as finishSSOLogin builds it, go-api's row of a completed
// sign-in, under a marker resource id, and reads it back with the user's id as its resource id.
func (h *harness) goAPISuccessRow(t *testing.T, orgID, user, provider uuid.UUID, protocol string) auditRow {
	t.Helper()
	marker := "go-api-" + uuid.NewString()
	meta := pyjson.NewObject()
	meta.Set("provider_id", provider.String())
	meta.Set("protocol", protocol)
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (audit.PGWriter{Now: func() time.Time { return h.now }}).Write(context.Background(), h.env.admin, audit.Entry{
		OrgID: orgID, UserID: &user, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSession, ResourceID: marker,
		RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	rows := h.auditRows(t, `resource_id = $1`, marker)
	if len(rows) != 1 {
		t.Fatalf("go-api rows %+v", rows)
	}
	rows[0].resourceID = user.String()
	return rows[0]
}

// A failing record on the role of the runtime: the column UPDATE is gone until the test ends.
func (h *harness) revokeProviderWrites(t *testing.T) {
	t.Helper()
	var role string
	if err := h.env.runtime.QueryRow(context.Background(), `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	quoted := `"` + strings.ReplaceAll(role, `"`, `""`) + `"`
	if _, err := h.env.admin.Exec(context.Background(), `REVOKE UPDATE ON public.sso_providers FROM `+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := h.env.admin.Exec(context.Background(), `GRANT UPDATE (`+strings.Join(authschema.SSOProviderWriteColumns(), ", ")+`) ON public.sso_providers TO `+quoted); err != nil {
			t.Errorf("restore the provider write grant: %v", err)
		}
	})
}

// afterIssue runs then once the session exists: the state a concurrent delete leaves between the session and its record.
type afterIssue struct {
	SessionIssuer
	then func()
}

func (a afterIssue) Issue(ctx context.Context, req SessionRequest) (Tokens, error) {
	tokens, err := a.SessionIssuer.Issue(ctx, req)
	a.then()
	return tokens, err
}

type refusingSessions struct{ SessionIssuer }

func (refusingSessions) Issue(context.Context, SessionRequest) (Tokens, error) {
	return Tokens{}, errors.New("the session store is gone")
}

func wantNoTokens(t *testing.T, login Login) {
	t.Helper()
	if login.Tokens != (Tokens{}) {
		t.Fatal("tokens left with a failed sign-in")
	}
}

// A completed OIDC sign-in writes go-api's record: last_login_at and updated_at on its provider row and no other, and
// exactly one audit row, the one go-api's own writer writes for finishSSOLogin.
func TestOIDCSignInWritesGoAPIsSuccessRecord(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	h.recordSignIns()
	other, _ := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	email := uniqueEmail("oidc-record")
	user := h.seedOIDCMember(t, "sub-record", email)
	s := h.start(t)
	login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-record", email)))
	if failure != nil || login.PrincipalID != user || login.Tokens.AccessToken == "" {
		t.Fatalf("sign-in: %v, %+v", failure, login)
	}
	if !h.lastLoginAtNow(t, h.providerID) {
		t.Fatal("last_login_at and updated_at are not the sign-in's time")
	}
	if h.lastLoginAtNow(t, other) {
		t.Fatal("the record moved another provider row")
	}
	want := h.goAPISuccessRow(t, h.orgID, user, h.providerID, "oidc")
	if got := h.successRows(t); len(got) != 1 || got[0] != want {
		t.Fatalf("rows %+v\ngo-api %+v", got, want)
	}
}

// The protocol is the provider row's own value, as go-api writes it, never 12040's "oauth".
func TestOAuthSignInWritesGoAPIsSuccessRecord(t *testing.T) {
	for _, protocol := range []string{"oauth_gitlab", "oauth_github"} {
		t.Run(protocol, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: false})
			h.recordSignIns()
			provider, client := h.seedOAuthProvider(t, h.orgID, protocol, nil)
			other, _ := h.seedOAuthProvider(t, h.orgID, protocol, nil)
			account := fakeAccountWith(uniqueEmail("oauth-record"), true)
			user := h.seedOAuthMember(t, h.orgID, protocol, account)
			login, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
			if failure != nil || login.PrincipalID != user || login.Tokens.AccessToken == "" {
				t.Fatalf("sign-in: %v, %+v", failure, login)
			}
			if !h.lastLoginAtNow(t, provider) {
				t.Fatal("last_login_at and updated_at are not the sign-in's time")
			}
			if h.lastLoginAtNow(t, other) {
				t.Fatal("the record moved another provider row")
			}
			want := h.goAPISuccessRow(t, h.orgID, user, provider, protocol)
			if got := h.successRows(t); len(got) != 1 || got[0] != want {
				t.Fatalf("rows %+v\ngo-api %+v", got, want)
			}
			if !strings.Contains(want.metadata, `"protocol": "`+protocol+`"`) {
				t.Fatalf("metadata %s does not store %s", want.metadata, protocol)
			}
		})
	}
}

// A refused sign-in writes no success row and leaves last_login_at alone.
func TestARefusedSignInWritesNoSuccessRecord(t *testing.T) {
	t.Run("oidc, not a member", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		key := ExternalKey{Issuer: h.issuer, Provider: h.providerID.String(), Subject: "sub-not-member"}
		email := uniqueEmail("not-member")
		h.link(t, key, email)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-not-member", email)))
		wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		if h.lastLoginAtNow(t, h.providerID) || len(h.successRows(t)) != 0 {
			t.Fatal("a refused sign-in was recorded")
		}
	})
	t.Run("oidc, no login cookie", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		email := uniqueEmail("cookie")
		h.seedOIDCMember(t, "sub-no-cookie", email)
		s := h.start(t)
		_, failure := h.finishWith(t, h.providerID, s, h.sign(t, h.claims(s.nonce, "sub-no-cookie", email)), false)
		wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
		if h.lastLoginAtNow(t, h.providerID) || len(h.successRows(t)) != 0 {
			t.Fatal("a refused sign-in was recorded")
		}
	})
	t.Run("oauth, unverified email", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
		account := fakeAccountWith(uniqueEmail("unverified"), false)
		h.seedOAuthMember(t, h.orgID, "oauth_gitlab", account)
		_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
		wantDenied(t, failure, http.StatusForbidden, ReasonEmailUnverified)
		if h.lastLoginAtNow(t, provider) || len(h.successRows(t)) != 0 {
			t.Fatal("a refused sign-in was recorded")
		}
	})
}

// A record that cannot be written is the 500 and no tokens leave; the session stays undelivered, as on go-api's path.
func TestASignInRecordWriteFailureIsThe500(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		email := uniqueEmail("no-write")
		h.seedOIDCMember(t, "sub-no-write", email)
		h.revokeProviderWrites(t)
		s := h.start(t)
		login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-no-write", email)))
		wantDenied(t, failure, http.StatusInternalServerError, ReasonStoreUnavailable)
		wantNoTokens(t, login)
		if h.sessions.count() != 1 || len(h.successRows(t)) != 0 {
			t.Fatalf("sessions %d, rows %+v", h.sessions.count(), h.successRows(t))
		}
	})
	t.Run("oauth", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
		account := fakeAccountWith(uniqueEmail("no-write"), true)
		h.seedOAuthMember(t, h.orgID, "oauth_gitlab", account)
		h.revokeProviderWrites(t)
		login, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
		wantDenied(t, failure, http.StatusInternalServerError, ReasonStoreUnavailable)
		wantNoTokens(t, login)
		if h.sessions.count() != 1 || len(h.successRows(t)) != 0 {
			t.Fatalf("sessions %d, rows %+v", h.sessions.count(), h.successRows(t))
		}
	})
}

// The UPDATE and the audit row are one transaction: an audit row that fails its user foreign key leaves last_login_at
// where it was.
func TestAFailedAuditRowRollsTheLastLoginBack(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	err := ssoaudit.PGSignIn{DB: h.env.runtime, Now: func() time.Time { return h.now }}.Succeeded(context.Background(),
		ssoaudit.Success{OrgID: h.orgID, ProviderID: h.providerID, PrincipalID: uuid.New(), Protocol: "oidc"})
	if err == nil {
		t.Fatal("a user with no public.users row wrote an audit row")
	}
	if h.lastLoginAtNow(t, h.providerID) {
		t.Fatal("last_login_at moved although the audit row failed")
	}
}

// A sign-in whose session could not be issued writes nothing.
func TestASignInWithoutASessionWritesNoRecord(t *testing.T) {
	t.Run("oidc", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		h.svc.cfg.Sessions = refusingSessions{h.sessions}
		email := uniqueEmail("no-session")
		h.seedOIDCMember(t, "sub-no-session", email)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-no-session", email)))
		wantDenied(t, failure, http.StatusInternalServerError, ReasonStoreUnavailable)
		if h.lastLoginAtNow(t, h.providerID) || len(h.successRows(t)) != 0 {
			t.Fatal("a sign-in without a session was recorded")
		}
	})
	t.Run("oauth", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		h.svc.cfg.Sessions = refusingSessions{h.sessions}
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
		account := fakeAccountWith(uniqueEmail("no-session"), true)
		h.seedOAuthMember(t, h.orgID, "oauth_gitlab", account)
		_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
		wantDenied(t, failure, http.StatusInternalServerError, ReasonStoreUnavailable)
		if h.lastLoginAtNow(t, provider) || len(h.successRows(t)) != 0 {
			t.Fatal("a sign-in without a session was recorded")
		}
	})
}

// Rows deleted between the session and its record. public.audit_logs.user_id and org_id are foreign keys;
// provider_id sits in the JSON and the resource id is text, so neither refers to the provider row.
func TestRowsDeletedBetweenTheSessionAndItsRecord(t *testing.T) {
	setup := func(t *testing.T, then func(h *harness, user uuid.UUID)) (*harness, Login, *Error) {
		t.Helper()
		h := newHarness(t, harnessOpts{signup: false})
		h.recordSignIns()
		email := uniqueEmail("window")
		// Unique per subtest: an account outlives its harness, and a deleted provider row leaves it a legacy match.
		subject := "sub-window-" + uuid.NewString()
		user := h.seedOIDCMember(t, subject, email)
		h.svc.cfg.Sessions = afterIssue{SessionIssuer: h.sessions, then: func() { then(h, user) }}
		s := h.start(t)
		login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, subject, email)))
		return h, login, failure
	}
	remove := func(t *testing.T, h *harness, sql string, id uuid.UUID) {
		t.Helper()
		if tag, err := h.env.admin.Exec(context.Background(), sql, id); err != nil || tag.RowsAffected() != 1 {
			t.Errorf("%s: %v, %d rows", sql, err, tag.RowsAffected())
		}
	}
	t.Run("the user: the foreign key fails, 500, no tokens", func(t *testing.T) {
		h, login, failure := setup(t, func(h *harness, user uuid.UUID) { remove(t, h, `DELETE FROM public.users WHERE id = $1`, user) })
		wantDenied(t, failure, http.StatusInternalServerError, ReasonStoreUnavailable)
		wantNoTokens(t, login)
		if h.lastLoginAtNow(t, h.providerID) || len(h.successRows(t)) != 0 {
			t.Fatal("the record stayed although its audit row failed")
		}
	})
	t.Run("the provider: the UPDATE moves no row, the sign-in goes on with its audit row", func(t *testing.T) {
		var provider uuid.UUID
		h, login, failure := setup(t, func(h *harness, _ uuid.UUID) {
			provider = h.providerID
			remove(t, h, `DELETE FROM public.sso_providers WHERE id = $1`, h.providerID)
		})
		if failure != nil || login.Tokens.AccessToken == "" {
			t.Fatalf("sign-in: %v, %+v", failure, login)
		}
		got := h.successRows(t)
		if len(got) != 1 || !strings.Contains(got[0].metadata, provider.String()) {
			t.Fatalf("rows %+v, want one naming %s", got, provider)
		}
		if n := h.count(t, `SELECT count(*) FROM public.sso_providers WHERE id = $1`, provider); n != 0 {
			t.Fatalf("%d provider rows", n)
		}
	})
	t.Run("the organization: the foreign key fails, 500, no tokens", func(t *testing.T) {
		h, login, failure := setup(t, func(h *harness, _ uuid.UUID) {
			remove(t, h, `DELETE FROM public.organizations WHERE id = $1`, h.orgID)
		})
		wantDenied(t, failure, http.StatusInternalServerError, ReasonStoreUnavailable)
		wantNoTokens(t, login)
		if len(h.successRows(t)) != 0 {
			t.Fatal("a row was written for a deleted organization")
		}
	})
}
