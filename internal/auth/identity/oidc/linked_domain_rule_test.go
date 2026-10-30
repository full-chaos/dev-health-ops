package oidc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

const domainDeniedDetail = `{"detail":"Email domain is not allowed for this provider"}`

// oauthDomainDeniedBody is go-api's OAuth domain refusal body, byte for byte (internal/api/sso oauthDomainDeniedDetail).
const oauthDomainDeniedBody = `{"detail":{"message":"Email domain is not allowed for this provider","reason":"oauth_domain_not_allowed"}}`

func resolveWithList(t *testing.T, protocol string, linked bool, list []string, p profile) (refusal, *orderIdentities, *resolveSessions) {
	t.Helper()
	ids := &orderIdentities{linked: linked}
	sessions := &resolveSessions{}
	svc := &Service{cfg: Config{Identities: ids, Memberships: resolveMembers{found: true, role: "member"},
		Sessions: sessions, MembershipJIT: &resolveJIT{}}}
	row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true, AllowedDomains: list}
	_, failure := svc.resolve(context.Background(), row, validated{profile: p}, nil)
	return refusalOf(t, failure), ids, sessions
}

// The provider's allowed_domains decide every sign-in, linked or new (go-api oidc.go/oauth.go parity): a linked key
// whose asserted domain is off the list is refused before the lookup, with the answer an unknown key gets. An address
// with no "@" is go-api's 400 on an OIDC row and its domain 403 on an OAuth row (oauth.go has no malformed branch);
// on an OAuth row the domain 403 carries go-api's OAuth detail object.
func TestResolveRefusesALinkedIdentityOffTheDomainList(t *testing.T) {
	malformed := refusal{status: http.StatusBadRequest, reason: ReasonMalformed, detail: `{"detail":"Email address from OIDC claims is malformed"}`}
	for _, protocol := range []string{"oidc", "oauth_gitlab", "oauth_github", "oauth_google"} {
		denied := refusal{status: http.StatusForbidden, reason: ReasonDomainNotAllowed, detail: domainDeniedDetail}
		noAt := malformed
		if protocol != "oidc" {
			denied.detail = oauthDomainDeniedBody
			noAt = denied
		}
		for name, tc := range map[string]struct {
			email string
			want  refusal
		}{
			"off the list": {"u@other.test", denied},
			"empty domain": {"u@", denied},
			"no at":        {"u.example.test", noAt},
		} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				p := profile{Email: tc.email, EmailVerified: true}
				linked, linkedIDs, linkedSessions := resolveWithList(t, protocol, true, []string{"example.test"}, p)
				unknown, unknownIDs, _ := resolveWithList(t, protocol, false, []string{"example.test"}, p)
				if linked != unknown || linked != tc.want {
					t.Fatalf("linked %+v, unknown %+v, want both %+v", linked, unknown, tc.want)
				}
				if linkedIDs.calls+unknownIDs.calls != 0 || len(linkedSessions.issued) != 0 {
					t.Fatalf("%d store calls, %d sessions before the refusal", linkedIDs.calls+unknownIDs.calls, len(linkedSessions.issued))
				}
			})
		}
	}
}

// A linked sign-in with no asserted email is the 400 a new one gets (go-api oidc.go missing-email check).
func TestResolveRefusesALinkedIdentityWithNoEmail(t *testing.T) {
	want := refusal{status: http.StatusBadRequest, reason: ReasonMalformed, detail: `{"detail":"Email address is missing from OIDC claims"}`}
	for _, list := range [][]string{nil, {"example.test"}} {
		got, ids, _ := resolveWithList(t, "oidc", true, list, profile{EmailVerified: true})
		if got != want || ids.calls != 0 {
			t.Fatalf("list %v: %+v with %d store calls, want %+v and none", list, got, ids.calls, want)
		}
	}
}

// Controls: a domain on the list (any ASCII case, entry trimmed), and no list or an empty list, sign a linked key in.
// A linked id-token sign-in with email_verified false still signs in (no verified check on linked OIDC: ruling Q1).
func TestResolveSignsInALinkedIdentityTheListAllows(t *testing.T) {
	for name, tc := range map[string]struct {
		list     []string
		email    string
		verified bool
	}{
		"on the list":         {[]string{" Example.TEST "}, "u@example.test", true},
		"no list":             {nil, "u@anything.test", true},
		"empty list":          {[]string{}, "u@anything.test", true},
		"unverified, on list": {[]string{"example.test"}, "u@example.test", false},
	} {
		got, ids, sessions := resolveWithList(t, "oidc", true, tc.list, profile{Email: tc.email, EmailVerified: tc.verified})
		if got != (refusal{}) || ids.calls == 0 || len(sessions.issued) != 1 {
			t.Fatalf("%s: %+v, %d store calls, %d sessions", name, got, ids.calls, len(sessions.issued))
		}
	}
}

// New account, unverified and off the list: the domain answer now comes first (id-token path; go-api reads no
// email_verified there).
func TestResolveNewAccountOffTheListIsTheDomainAnswer(t *testing.T) {
	got, ids, _ := resolveWithList(t, "oidc", false, []string{"example.test"}, profile{Email: "u@other.test"})
	if got.reason != ReasonDomainNotAllowed || ids.calls != 0 {
		t.Fatalf("%+v with %d store calls", got, ids.calls)
	}
}

// OAuth mode reads the verified flag on every sign-in, linked or new (oauth_flow.go; go-api oauth.go).
func TestOAuthCallbackRefusesALinkedUnverifiedEmail(t *testing.T) {
	got, ids := oauthCallbackRun(t, "a@example.test", false, true)
	if got.status != http.StatusForbidden || got.reason != ReasonEmailUnverified || ids.calls != 0 {
		t.Fatalf("%+v with %d identity store calls", got, ids.calls)
	}
}

// Through the real OAuth callback (stored protocol oauth_gitlab): with a list, an address off the list or with no
// "@" is go-api's OAuth domain 403 for a linked and an unknown key alike, before any lookup (go-api oauth.go has no
// malformed branch).
func TestOAuthCallbackAnswersNoAtWithTheDomainRefusal(t *testing.T) {
	list := `["example.test"]`
	want := refusal{status: http.StatusForbidden, reason: ReasonDomainNotAllowed, detail: oauthDomainDeniedBody}
	for _, email := range []string{"u.example.test", "u@other.test"} {
		linked, linkedIDs := oauthCallbackRunWithList(t, email, true, true, &list)
		unknown, unknownIDs := oauthCallbackRunWithList(t, email, true, false, &list)
		if linked != want || unknown != want || linkedIDs.calls+unknownIDs.calls != 0 {
			t.Fatalf("%s: linked %+v, unknown %+v, %d store calls; want both %+v and none", email, linked, unknown, linkedIDs.calls+unknownIDs.calls, want)
		}
	}
	if got, ids := oauthCallbackRunWithList(t, "u@example.test", true, true, &list); got != (refusal{}) || ids.calls == 0 {
		t.Fatalf("control, domain on the list: %+v with %d store calls", got, ids.calls)
	}
}

// The OAuth domain refusal goes over the wire as go-api's body, byte for byte; an OIDC row keeps its string detail.
func TestOAuthDomainRefusalWritesGoAPIBytes(t *testing.T) {
	for protocol, want := range map[string]string{"oauth_gitlab": oauthDomainDeniedBody, "oidc": domainDeniedDetail} {
		svc := &Service{cfg: Config{Identities: &orderIdentities{linked: true}, Memberships: resolveMembers{found: true, role: "member"},
			Sessions: &resolveSessions{}, MembershipJIT: &resolveJIT{}}}
		row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true, AllowedDomains: []string{"example.test"}}
		_, failure := svc.resolve(context.Background(), row, validated{profile: profile{Email: "u@other.test", EmailVerified: true}}, nil)
		if failure == nil {
			t.Fatalf("%s: no refusal", protocol)
		}
		rec := httptest.NewRecorder()
		svc.writeError(rec, failure)
		if rec.Code != http.StatusForbidden || rec.Body.String() != want || failure.Reason != ReasonDomainNotAllowed {
			t.Fatalf("%s: %d %s (reason %s), want 403 %s", protocol, rec.Code, rec.Body.String(), failure.Reason, want)
		}
	}
}
