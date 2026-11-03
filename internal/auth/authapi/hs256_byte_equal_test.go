package authapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// hs256Answer is one token answer under hs256_compat with the values that differ per run replaced by a name: the
// credentials and tokens (random jti, signature, opaque secret) and the fixture's random identifiers.
type hs256Answer struct {
	status int
	body   string
}

var (
	hs256Credential = regexp.MustCompile(`"(access_token|refresh_token|refresh_credential)":"[^"]*"`)
	hs256Session    = regexp.MustCompile(`ses_[A-Za-z0-9_-]{22}`)
	hs256Request    = regexp.MustCompile(`"request_id":"[^"]*"`)
)

func maskHS256Answer(body string, ids map[string]string) string {
	body = hs256Credential.ReplaceAllString(body, `"$1":"<$1>"`)
	body = hs256Session.ReplaceAllString(body, "<session_id>")
	body = hs256Request.ReplaceAllString(body, `"request_id":"<request_id>"`)
	names := make([]string, 0, len(ids))
	for id := range ids {
		names = append(names, id)
	}
	sort.Strings(names)
	for _, id := range names {
		body = strings.ReplaceAll(body, id, ids[id])
	}
	return body
}

func hs256Issuer(t *testing.T) *Issuer {
	t.Helper()
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}

// hs256Answers renders every token answer auth-service writes, under hs256_compat, masked.
func hs256Answers(t *testing.T) map[string]hs256Answer {
	t.Helper()
	answers := map[string]hs256Answer{}
	record := func(name string, response *httptest.ResponseRecorder, ids map[string]string) {
		answers[name] = hs256Answer{status: response.Code, body: maskHS256Answer(strings.TrimSpace(response.Body.String()), ids)}
	}
	compatIDs := func(f *compatFixture) map[string]string {
		return map[string]string{f.principal.String(): "<principal>", f.org.String(): "<org>", f.orgB.String(): "<org_b>"}
	}

	refresh := newFixtureWith(t, hs256Issuer(t))
	refresh.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: refresh.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: refresh.record.ExpiresAt,
	}}
	record("POST /v1/token/refresh", refresh.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil),
		map[string]string{refresh.principal.String(): "<principal>"})

	issue := newCompatFixture(t)
	record("POST /internal/v1/sessions", issue.do(http.MethodPost, PatternIssueSession, issueBody(issue.principal, issue.org.String(), "pwd"), issuerBearer), compatIDs(issue))

	compatRefresh := newCompatFixture(t)
	compatRefresh.sessions.result = accepted(compatRefresh.record)
	compatRefresh.buildCompat()
	record("POST /api/v1/auth/refresh", compatRefresh.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil), compatIDs(compatRefresh))

	login := newCompatFixture(t)
	record("POST /api/v1/auth/login", login.do(http.MethodPost, PatternCompatLogin, loginBody(compatEmail, compatPassword, ""), nil), compatIDs(login))

	socialLogin, fake := newSocialFixture(t)
	fake.result = social.ExchangeResult{PrincipalID: socialLogin.principal}
	socialLogin.buildCompat()
	record("POST /api/v1/auth/social-login", socialLogin.do(http.MethodPost, PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"tok"}`, nil), compatIDs(socialLogin))

	change := newCredFixtureOn(t, httpapi.NewMemoryCounters(nil), newFixtureWith(t, hs256Issuer(t)))
	bearer, err := hs256Issuer(t).Access(change.grant())
	if err != nil {
		t.Fatal(err)
	}
	record("POST /api/v1/auth/change-password", change.call(http.MethodPost, PatternChangePassword,
		`{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`, map[string]string{"Authorization": "Bearer " + bearer}),
		map[string]string{change.principal.String(): "<principal>", change.org.String(): "<org>"})

	sso := newCompatFixture(t)
	w, r := samlRequest()
	sso.ssoSessions(t).SAML().IssueFederatedSession(w, r, saml.SignIn{PrincipalID: sso.principal, OrganizationID: &sso.org, AMR: []string{"fed"}})
	record("POST /api/v1/auth/saml/{provider_id}/acs", w, compatIDs(sso))
	return answers
}

// The hs256_compat answers of main at 7c11f38a1 (before the eddsa issuer minted a session-route token), rendered by
// this file's hs256Answers on that commit and masked the same way. Under hs256_compat every token answer must stay
// byte for byte what main wrote: same members, same order, same values, and no session_access_token.
var mainHS256Answers = map[string]hs256Answer{
	"POST /api/v1/auth/change-password":        {200, `{"access_token":"<access_token>","refresh_token":"<refresh_token>","token_type":"bearer","expires_in":3600,"needs_onboarding":false,"user":{"id":"<principal>","email":"ada@example.com","username":"ada","full_name":null,"org_id":"<org>","role":"admin","is_superuser":true}}`},
	"POST /api/v1/auth/login":                  {200, `{"access_token":"<access_token>","refresh_token":"<refresh_token>","token_type":"bearer","expires_in":3600,"needs_onboarding":false,"user":{"id":"<principal>","email":"ada@example.com","username":"ada","full_name":null,"org_id":"<org_b>","role":"member","is_superuser":true}}`},
	"POST /api/v1/auth/refresh":                {200, `{"access_token":"<access_token>","refresh_token":"<refresh_token>","token_type":"bearer","expires_in":3600,"user":{"id":"<principal>","email":"ada@example.com","username":null,"full_name":null,"org_id":"<org>","role":"admin","is_superuser":true}}`},
	"POST /api/v1/auth/saml/{provider_id}/acs": {200, `{"access_token":"<access_token>","refresh_token":"<refresh_token>","token_type":"bearer","expires_in":3600,"user_id":"<principal>","email":"ada@example.com","org_id":"<org>","role":"admin"}`},
	"POST /api/v1/auth/social-login":           {200, `{"access_token":"<access_token>","refresh_token":"<refresh_token>","token_type":"bearer","expires_in":3600,"needs_onboarding":false,"user":{"id":"<principal>","email":"ada@example.com","username":"ada","full_name":null,"org_id":"<org_b>","role":"member","is_superuser":true},"is_new_user":false}`},
	"POST /internal/v1/sessions":               {200, `{"access_token":"<access_token>","refresh_token":"<refresh_token>","token_type":"bearer","expires_in":3600}`},
	"POST /v1/token/refresh":                   {200, `{"schema_version":"refresh-response.v1","access_token":"<access_token>","token_type":"bearer","expires_in":3600,"refresh_credential":"<refresh_credential>","refresh_expires_in":604800,"session_id":"<session_id>"}`},
}

func TestHS256CompatAnswersAreByteEqualToMain(t *testing.T) {
	answers := hs256Answers(t)
	if os.Getenv("HS256_BYTE_EQUAL_PRINT") != "" {
		names := make([]string, 0, len(answers))
		for name := range answers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			t.Logf("ANSWER %q %d %s", name, answers[name].status, answers[name].body)
		}
		return
	}
	if len(mainHS256Answers) != 7 || len(answers) != 7 {
		t.Fatalf("%d recorded main answers and %d rendered, want 7 and 7", len(mainHS256Answers), len(answers))
	}
	for name, want := range mainHS256Answers {
		got := answers[name]
		if got != want {
			t.Errorf("%s under hs256_compat:\n got  %d %s\n main %d %s", name, got.status, got.body, want.status, want.body)
		}
		if strings.Contains(got.body, "session_access_token") {
			t.Errorf("%s: an hs256_compat answer carries session_access_token", name)
		}
	}
}
