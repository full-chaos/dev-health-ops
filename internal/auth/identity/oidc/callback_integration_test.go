//go:build integration

package oidc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base32"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

func uniqueEmail(prefix string) string { return prefix + "-" + uuid.NewString()[:8] + "@example.com" }

func TestNewAccountIsProvisionedAndAuditedWhenSignupIsOn(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	before := h.snapshot(t)
	s := h.start(t)
	email := uniqueEmail("new")
	login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-new", email)))
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	var principal uuid.UUID
	if err := h.env.admin.QueryRow(context.Background(), `SELECT principal_id FROM auth.external_identities
WHERE issuer = $1 AND provider = $2 AND subject = 'sub-new'`, h.issuer, h.providerID.String()).Scan(&principal); err != nil {
		t.Fatalf("identity row: %v", err)
	}
	if principal != login.PrincipalID {
		t.Fatalf("login principal %s, identity principal %s", login.PrincipalID, principal)
	}
	after := h.snapshot(t)
	if after.auditRows != before.auditRows+1 || after.identities != before.identities+1 {
		t.Fatalf("provisioning wrote %+v -> %+v, want one identity and one audit row", before, after)
	}
	if after.publicUsers != before.publicUsers {
		t.Fatal("provisioning wrote public.users")
	}
	if n := h.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND request_id = $2`,
		principal, h.requestID); n != 1 {
		t.Fatalf("audit rows carrying the request id = %d, want 1", n)
	}
	req := h.sessions.requests[len(h.sessions.requests)-1]
	if req.PrincipalID != principal || len(req.AMR) != 1 || req.AMR[0] != "fed" || req.OrgID != nil || req.Role != "member" {
		t.Fatalf("session request %+v", req)
	}
}

func TestNewAccountIsNotAvailableWhileSignupIsOff(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	before := h.snapshot(t)
	s := h.start(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-off", uniqueEmail("off"))))
	wantDenied(t, failure, http.StatusForbidden, ReasonSignupUnavailable)
	wantUnchanged(t, h, before)
}

func TestProviderEmailCollisionIsAConflict(t *testing.T) {
	t.Run("local account, other letter case", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		email := uniqueEmail("holder")
		h.seedPublicUser(t, email, "local", "")
		before := h.snapshot(t)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-other", strings.ToUpper(email[:1])+email[1:])))
		wantDenied(t, failure, http.StatusConflict, ReasonEmailCollision)
		wantUnchanged(t, h, before)
	})
	t.Run("account of another organization", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		email := uniqueEmail("member")
		user := h.seedPublicUser(t, email, "local", "")
		h.seedMembership(t, user, h.seedOrg(t), "owner")
		before := h.snapshot(t)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-foreign", email)))
		wantDenied(t, failure, http.StatusConflict, ReasonEmailCollision)
		wantUnchanged(t, h, before)
	})
	t.Run("account already linked to another subject", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		email := uniqueEmail("linked")
		h.link(t, ExternalKey{Issuer: h.issuer, Provider: h.providerID.String(), Subject: "sub-first"}, email)
		before := h.snapshot(t)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-second", email)))
		wantDenied(t, failure, http.StatusConflict, ReasonEmailCollision)
		wantUnchanged(t, h, before)
	})
}

func TestUnverifiedProviderEmailIsRefused(t *testing.T) {
	for name, value := range map[string]any{"false": false, "string false": "false", "absent": nil} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			before := h.snapshot(t)
			s := h.start(t)
			claims := h.claims(s.nonce, "sub-unverified", uniqueEmail("unverified"))
			if value == nil {
				delete(claims, "email_verified")
			} else {
				claims["email_verified"] = value
			}
			_, failure := h.finish(t, s, h.sign(t, claims))
			wantDenied(t, failure, http.StatusForbidden, ReasonEmailUnverified)
			wantUnchanged(t, h, before)
		})
	}
}

func TestSubjectIsScopedByIssuer(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	other := h.link(t, ExternalKey{Issuer: "https://another-issuer.example", Provider: h.providerID.String(), Subject: "shared-sub"}, uniqueEmail("other"))
	s := h.start(t)
	login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "shared-sub", uniqueEmail("mine"))))
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if login.PrincipalID == other {
		t.Fatal("a subject of another issuer signed in as that issuer's account")
	}
}

func TestSubjectIsScopedByProviderRow(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	first := h.start(t)
	owner, failure := h.finish(t, first, h.sign(t, h.claims(first.nonce, "sub-consented", uniqueEmail("owner"))))
	if failure != nil {
		t.Fatalf("sign-in through the first provider row: %v", failure)
	}
	otherClient := "client-" + uuid.NewString()
	other := h.seedProvider(t, h.seedOrg(t), map[string]any{"client_id": otherClient, "issuer": h.issuer,
		"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
	h.ids.signup = false
	before := h.snapshot(t)
	s := h.startFor(t, other)
	claims := h.claims(s.nonce, "sub-consented", uniqueEmail("asserted"))
	claims["aud"] = otherClient
	login, failure := h.finishWith(t, other, s, h.sign(t, claims), true)
	if failure == nil && login.PrincipalID == owner.PrincipalID {
		t.Fatal("the same issuer and subject through another provider row signed in as the first row's account")
	}
	wantDenied(t, failure, http.StatusForbidden, ReasonSignupUnavailable)
	wantUnchanged(t, h, before)
}

func TestLinkedIdentitySignsInWithoutReadingEmail(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	key := ExternalKey{Issuer: h.issuer, Provider: h.providerID.String(), Subject: "sub-linked"}
	owner := h.link(t, key, uniqueEmail("owner"))
	s := h.start(t)
	claims := h.claims(s.nonce, "sub-linked", uniqueEmail("changed"))
	claims["email_verified"] = false
	login, failure := h.finish(t, s, h.sign(t, claims))
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if login.PrincipalID != owner {
		t.Fatalf("signed in as %s, want %s", login.PrincipalID, owner)
	}
}

func TestLinkedIdentityAnswersWithTheStoredEmail(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	stored := uniqueEmail("stored")
	owner := h.link(t, ExternalKey{Issuer: h.issuer, Provider: h.providerID.String(), Subject: "sub-stored"}, stored)
	s := h.start(t)
	claims := h.claims(s.nonce, "sub-stored", uniqueEmail("asserted"))
	claims["email_verified"] = false
	login, failure := h.finish(t, s, h.sign(t, claims))
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if login.PrincipalID != owner || login.Email != stored {
		t.Fatalf("login principal %s email %q, want %s with the stored email %q", login.PrincipalID, login.Email, owner, stored)
	}
}

func TestExistingSSOAccountSignsInBySubject(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	email := uniqueEmail("legacy")
	user := h.seedPublicUser(t, email, "oidc", "sub-legacy")
	h.seedMembership(t, user, h.orgID, "admin")
	before := h.snapshot(t)
	s := h.start(t)
	login, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-legacy", email)))
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if login.PrincipalID != user || login.Role != "admin" || login.OrgID == nil || *login.OrgID != h.orgID {
		t.Fatalf("login %+v, want user %s admin of %s", login, user, h.orgID)
	}
	after := h.snapshot(t)
	if after.identities != before.identities+1 || after.auditRows <= before.auditRows || after.publicUsers != before.publicUsers {
		t.Fatalf("legacy sign-in wrote %+v -> %+v, want one audited link and no public write", before, after)
	}
	if n := h.count(t, `SELECT count(*) FROM auth.security_audit_events
WHERE subject_principal_id = $1 AND attributes->>'reason_code' = 'identity_projected'`, user); n != 1 {
		t.Fatalf("identity_projected audit rows = %d, want 1", n)
	}
	if n := h.count(t, `SELECT count(*) FROM auth.external_identities
WHERE principal_id = $1 AND issuer = $2 AND provider = $3 AND subject = 'sub-legacy'`, user, h.issuer, h.providerID.String()); n != 1 {
		t.Fatalf("projected identity rows = %d", n)
	}
	s2 := h.start(t)
	if _, failure := h.finish(t, s2, h.sign(t, h.claims(s2.nonce, "sub-legacy", email))); failure != nil {
		t.Fatalf("second sign-in: %v", failure)
	}
	if again := h.snapshot(t); again.auditRows != after.auditRows {
		t.Fatal("a second sign-in wrote another audit row")
	}
}

func TestExistingSSOAccountNeedsProviderOrgMembership(t *testing.T) {
	t.Run("member of another organization only", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		user := h.seedPublicUser(t, uniqueEmail("elsewhere"), "oidc", "sub-elsewhere")
		h.seedMembership(t, user, h.seedOrg(t), "owner")
		before := h.snapshot(t)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-elsewhere", uniqueEmail("asserted"))))
		wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
		wantUnchanged(t, h, before)
	})
	t.Run("two organizations carry the subject", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		inOrg := h.seedPublicUser(t, uniqueEmail("in-org"), "oidc", "sub-two-orgs")
		h.seedMembership(t, inOrg, h.orgID, "member")
		elsewhere := h.seedPublicUser(t, uniqueEmail("other-org"), "oidc", "sub-two-orgs")
		h.seedMembership(t, elsewhere, h.seedOrg(t), "member")
		before := h.snapshot(t)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-two-orgs", uniqueEmail("asserted"))))
		wantDenied(t, failure, http.StatusConflict, ReasonIdentityConflict)
		wantUnchanged(t, h, before)
	})
	t.Run("two accounts carry the subject", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		for i := 0; i < 2; i++ {
			user := h.seedPublicUser(t, uniqueEmail("twin"), "oidc", "sub-twin")
			h.seedMembership(t, user, h.orgID, "member")
		}
		before := h.snapshot(t)
		s := h.start(t)
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-twin", uniqueEmail("asserted"))))
		wantDenied(t, failure, http.StatusConflict, ReasonIdentityConflict)
		wantUnchanged(t, h, before)
	})
}

func TestCallbackRefusesWrongIssuer(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	before := h.snapshot(t)
	s := h.start(t)
	claims := h.claims(s.nonce, "sub-iss", uniqueEmail("iss"))
	claims["iss"] = "https://untrusted.example"
	_, failure := h.finish(t, s, h.sign(t, claims))
	wantDenied(t, failure, http.StatusBadRequest, ReasonIssuer)
	wantUnchanged(t, h, before)
}

func TestCallbackRefusesWrongAudience(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"other client":                   func(c map[string]any) { c["aud"] = "client-other" },
		"several audiences, no azp":      func(c map[string]any) { c["aud"] = []string{c["aud"].(string), "client-other"} },
		"our audience, foreign azp":      func(c map[string]any) { c["azp"] = "client-other" },
		"several audiences, foreign azp": func(c map[string]any) { c["aud"] = []string{c["aud"].(string), "x"}; c["azp"] = "x" },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			before := h.snapshot(t)
			s := h.start(t)
			claims := h.claims(s.nonce, "sub-aud", uniqueEmail("aud"))
			mutate(claims)
			_, failure := h.finish(t, s, h.sign(t, claims))
			wantDenied(t, failure, http.StatusBadRequest, ReasonAudience)
			wantUnchanged(t, h, before)
		})
	}
}

func TestCallbackRefusesExpiredIDToken(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	before := h.snapshot(t)
	s := h.start(t)
	claims := h.claims(s.nonce, "sub-exp", uniqueEmail("exp"))
	claims["exp"] = h.now.Add(-2 * time.Minute).Unix()
	_, failure := h.finish(t, s, h.sign(t, claims))
	wantDenied(t, failure, http.StatusBadRequest, ReasonExpired)
	wantUnchanged(t, h, before)
}

func TestCallbackRefusesIDTokenNotYetValid(t *testing.T) {
	for name, field := range map[string]string{"nbf": "nbf", "iat": "iat"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			before := h.snapshot(t)
			s := h.start(t)
			claims := h.claims(s.nonce, "sub-nbf", uniqueEmail("nbf"))
			claims[field] = h.now.Add(5 * time.Minute).Unix()
			_, failure := h.finish(t, s, h.sign(t, claims))
			wantDenied(t, failure, http.StatusBadRequest, ReasonNotYetValid)
			wantUnchanged(t, h, before)
		})
	}
}

func TestCallbackRefusesNonceFromAnotherFlow(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	before := h.snapshot(t)
	first := h.start(t)
	second := h.start(t)
	_, failure := h.finish(t, second, h.sign(t, h.claims(first.nonce, "sub-nonce", uniqueEmail("nonce"))))
	wantDenied(t, failure, http.StatusBadRequest, ReasonNonceMismatch)
	wantUnchanged(t, h, before)
}

func TestCallbackRefusesReplayedState(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	s := h.start(t)
	email := uniqueEmail("replay")
	if _, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-replay", email))); failure != nil {
		t.Fatalf("first callback: %v", failure)
	}
	calls := h.idp.tokenCalls
	before := h.snapshot(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-replay", email)))
	wantDenied(t, failure, http.StatusBadRequest, ReasonReplayed)
	wantUnchanged(t, h, before)
	if h.idp.tokenCalls != calls {
		t.Fatal("a replayed state reached the token endpoint")
	}
}

func TestCallbackRefusesExpiredState(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	before := h.snapshot(t)
	s := h.start(t)
	h.now = h.now.Add(stateTTL + time.Second)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-late", uniqueEmail("late"))))
	wantDenied(t, failure, http.StatusBadRequest, ReasonExpired)
	wantUnchanged(t, h, before)
}

func TestCallbackRefusesStateForAnotherProvider(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	other := h.seedProvider(t, h.orgID, map[string]any{"client_id": h.clientID, "issuer": h.issuer,
		"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
	before := h.snapshot(t)
	s := h.start(t)
	_, failure := h.finishWith(t, other, s, h.sign(t, h.claims(s.nonce, "sub-cross", uniqueEmail("cross"))), true)
	wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
	wantUnchanged(t, h, before)
}

func TestCallbackRefusesMissingLoginNonceCookie(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	before := h.snapshot(t)
	s := h.start(t)
	idToken := h.sign(t, h.claims(s.nonce, "sub-cookie", uniqueEmail("cookie")))
	_, failure := h.finishWith(t, h.providerID, s, idToken, false)
	wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
	wantUnchanged(t, h, before)
	// The refused attempt did not use up the flow: its own browser still completes it.
	if _, failure := h.finish(t, s, idToken); failure != nil {
		t.Fatalf("flow owner after a cookieless attempt: %v", failure)
	}
}

func TestCallbackRefusesUnsignedIDToken(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	for name, sig := range map[string]string{"empty signature": "", "junk signature": "c2ln"} {
		t.Run(name, func(t *testing.T) {
			before := h.snapshot(t)
			s := h.start(t)
			token := encodeSegments(t, map[string]any{"alg": "none"}, h.claims(s.nonce, "sub-none", uniqueEmail("none"))) + "." + sig
			_, failure := h.finish(t, s, token)
			if failure == nil || failure.Status != http.StatusBadRequest ||
				(failure.Reason != ReasonMalformed && failure.Reason != ReasonBadSignature) {
				t.Fatalf("alg none: %v", failure)
			}
			wantUnchanged(t, h, before)
		})
	}
}

func TestCallbackRefusesSymmetricAlgWithPublicKey(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	der, err := x509.MarshalPKIXPublicKey(&h.idp.key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string][]byte{
		"pem": pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}),
		"der": der, "modulus": h.idp.key.PublicKey.N.Bytes(),
	}
	for name, secret := range keys {
		for _, alg := range []string{"HS256", "HS384", "HS512"} {
			t.Run(name+" "+alg, func(t *testing.T) {
				before := h.snapshot(t)
				s := h.start(t)
				input := encodeSegments(t, map[string]any{"alg": alg, "kid": h.idp.kid}, h.claims(s.nonce, "sub-hs", uniqueEmail("hs")))
				mac := hmac.New(sha256.New, secret)
				_, _ = io.WriteString(mac, input)
				_, failure := h.finish(t, s, input+"."+b64(mac.Sum(nil)))
				wantDenied(t, failure, http.StatusBadRequest, ReasonBadSignature)
				wantUnchanged(t, h, before)
			})
		}
	}
}

func TestCallbackRefusesKeyNotInIssuerJWKS(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	stranger, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for name, kid := range map[string]string{"unknown kid": "k-stranger", "issuer kid": h.idp.kid} {
		t.Run(name, func(t *testing.T) {
			before := h.snapshot(t)
			s := h.start(t)
			token := signRS256(t, stranger, map[string]any{"alg": "RS256", "kid": kid}, h.claims(s.nonce, "sub-key", uniqueEmail("key")))
			_, failure := h.finish(t, s, token)
			wantDenied(t, failure, http.StatusBadRequest, ReasonBadSignature)
			wantUnchanged(t, h, before)
		})
	}
}

func TestAutoProvisionDisabledRefusesNewAccount(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true, noAutoProvision: true})
	before := h.snapshot(t)
	s := h.start(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-noauto", uniqueEmail("noauto"))))
	wantDenied(t, failure, http.StatusForbidden, ReasonProvisioningDisabled)
	wantUnchanged(t, h, before)
}

func TestDisallowedEmailDomainIsRefused(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true, allowedDomains: []string{"allowed.example"}})
	before := h.snapshot(t)
	s := h.start(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-domain", uniqueEmail("domain"))))
	wantDenied(t, failure, http.StatusForbidden, ReasonDomainNotAllowed)
	wantUnchanged(t, h, before)
	if s, ok := failure.Detail.(string); !ok || strings.Contains(s, "example.com") {
		t.Fatalf("detail echoes the domain: %v", failure.Detail)
	}
}

func TestDisabledAccountIsRefused(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	email := uniqueEmail("disabled")
	user := h.seedPublicUser(t, email, "oidc", "sub-disabled")
	h.seedMembership(t, user, h.orgID, "member")
	if _, err := h.env.admin.Exec(context.Background(), `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	s := h.start(t)
	_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-disabled", email)))
	wantDenied(t, failure, http.StatusForbidden, ReasonPrincipalDeactivated)
	if h.sessions.count() != 0 {
		t.Fatal("a disabled account got a session")
	}
}

func TestDiscoveryIssuerMustMatchTheConfiguredIssuer(t *testing.T) {
	t.Run("matching", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true, discovery: true})
		s := h.start(t)
		if _, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-disco", uniqueEmail("disco")))); failure != nil {
			t.Fatalf("callback: %v", failure)
		}
	})
	t.Run("another issuer", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true, discovery: true})
		untrusted := "https://untrusted.example"
		h.idp.docIssuer = &untrusted
		before := h.snapshot(t)
		s := h.start(t)
		claims := h.claims(s.nonce, "sub-disco", uniqueEmail("disco"))
		claims["iss"] = untrusted
		_, failure := h.finish(t, s, h.sign(t, claims))
		wantDenied(t, failure, http.StatusBadRequest, ReasonDependency)
		wantUnchanged(t, h, before)
	})
}

func TestUserinfoSubjectMustMatchTheIDToken(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true, userinfo: true})
	h.idp.userinfo = map[string]any{"sub": "someone-else", "email": uniqueEmail("ui"), "email_verified": true}
	before := h.snapshot(t)
	s := h.start(t)
	claims := h.claims(s.nonce, "sub-ui", "")
	delete(claims, "email")
	delete(claims, "email_verified")
	_, failure := h.finish(t, s, h.sign(t, claims))
	wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
	wantUnchanged(t, h, before)
}

func TestHandlersRoundTrip(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/auth/oidc/{provider_id}/authorize", h.svc.AuthorizeHandler())
	mux.Handle("POST /api/v1/auth/oidc/{provider_id}/callback", h.svc.CallbackHandler())
	server := httptest.NewServer(requestid.Middleware(mux))
	t.Cleanup(server.Close)
	base := server.URL + "/api/v1/auth/oidc/" + h.providerID.String()

	resp, err := http.Post(base+"/authorize", "application/json", strings.NewReader(`{"use_pkce":false}`))
	if err != nil {
		t.Fatal(err)
	}
	var authz map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&authz)
	resp.Body.Close()
	cookies := resp.Cookies()
	if resp.StatusCode != http.StatusOK || len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly {
		t.Fatalf("authorize: %d %v %v", resp.StatusCode, authz, cookies)
	}
	if !strings.Contains(authz["authorization_url"], "code_challenge_method=S256") {
		t.Fatalf("PKCE off on request: %s", authz["authorization_url"])
	}
	parsed, _ := http.NewRequest(http.MethodGet, authz["authorization_url"], nil)
	q := parsed.URL.Query()
	code := "code-" + uuid.NewString()
	h.idp.mu.Lock()
	h.idp.codes[code] = pendingCode{challenge: q.Get("code_challenge"),
		idToken: h.sign(t, h.claims(q.Get("nonce"), "sub-http", uniqueEmail("http")))}
	h.idp.mu.Unlock()

	body, _ := json.Marshal(map[string]string{"code": code, "state": authz["state"], "code_verifier": "caller-chosen"})
	req, _ := http.NewRequest(http.MethodPost, base+"/callback", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: cookies[0].Name, Value: cookies[0].Value})
	clientRequestID := uuid.NewString()
	req.Header.Set("X-Request-ID", clientRequestID)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var login map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&login)
	if resp.StatusCode != http.StatusOK || login["token_type"] != "bearer" || login["access_token"] == "" || login["user_id"] == "" {
		t.Fatalf("callback: %d %v", resp.StatusCode, login)
	}
	cleared := false
	for _, c := range resp.Cookies() {
		if c.Name == cookies[0].Name && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("callback did not clear the login-nonce cookie")
	}
	if n := h.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE request_id = $1`, clientRequestID); n != 0 {
		t.Fatal("an audit row took its request id from the client header")
	}
	wire := resp.Header.Get(requestid.Header)
	raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.TrimPrefix(wire, requestid.WirePrefix))
	if err != nil {
		t.Fatalf("response request id %q: %v", wire, err)
	}
	serverID, err := uuid.FromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if n := h.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE request_id = $1`, serverID.String()); n != 1 {
		t.Fatalf("provisioning audit rows carrying the server request id = %d, want 1", n)
	}

	resp, err = http.Post(base+"/callback", "application/json", strings.NewReader(`{"code":"c","state":"s","code":"d"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("duplicate member body: %d, want 422", resp.StatusCode)
	}
}

func TestNonEntitledOrganizationIsRefused(t *testing.T) {
	flows := func(h *harness) int { return h.count(t, `SELECT count(*) FROM auth.sso_flow_states`) }
	t.Run("authorize", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		org := h.seedOrgTier(t, "community")
		provider := h.seedProvider(t, org, map[string]any{"client_id": h.clientID, "issuer": h.issuer,
			"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
		before, flowRows := h.snapshot(t), flows(h)
		_, err := h.svc.Authorize(context.Background(), provider, "")
		var failure *Error
		if !errors.As(err, &failure) {
			t.Fatalf("Authorize = %v, want a refusal", err)
		}
		wantDenied(t, failure, http.StatusPaymentRequired, ReasonNotEntitled)
		detail, _ := json.Marshal(failure.Detail)
		if !strings.Contains(string(detail), `"error":"feature_not_licensed"`) || !strings.Contains(string(detail), `"feature":"sso_saml"`) {
			t.Fatalf("402 detail %s", detail)
		}
		wantUnchanged(t, h, before)
		if flows(h) != flowRows {
			t.Fatal("a refused authorize recorded a flow")
		}
	})
	t.Run("callback after the entitlement ends", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		s := h.start(t)
		if _, err := h.env.admin.Exec(context.Background(), `UPDATE public.organizations SET tier = 'community' WHERE id = $1`, h.orgID); err != nil {
			t.Fatal(err)
		}
		before := h.snapshot(t)
		calls := h.idp.tokenCalls
		_, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-unlicensed", uniqueEmail("unlicensed"))))
		wantDenied(t, failure, http.StatusPaymentRequired, ReasonNotEntitled)
		wantUnchanged(t, h, before)
		if h.idp.tokenCalls != calls {
			t.Fatal("a non-entitled callback reached the token endpoint")
		}
		if n := h.count(t, `SELECT count(*) FROM auth.sso_flow_states WHERE consumed_at IS NOT NULL AND provider_id = $1`, h.providerID); n != 0 {
			t.Fatal("a non-entitled callback consumed the flow")
		}
	})
	t.Run("an enterprise license row entitles a community organization", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		org := h.seedOrgTier(t, "community")
		if _, err := h.env.admin.Exec(context.Background(), `INSERT INTO public.org_licenses (id, org_id, tier, created_at, updated_at)
VALUES ($1, $2, 'enterprise', now(), now())`, uuid.New(), org); err != nil {
			t.Fatal(err)
		}
		provider := h.seedProvider(t, org, map[string]any{"client_id": h.clientID, "issuer": h.issuer,
			"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
		if _, err := h.svc.Authorize(context.Background(), provider, ""); err != nil {
			t.Fatalf("Authorize for a licensed organization: %v", err)
		}
	})
}

func TestRefusedLogLineCarriesNoURLQuery(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	secret := "q" + uuid.NewString()
	closed := httptest.NewServer(http.NotFoundHandler())
	tokenURL := closed.URL + "/token?client_secret=" + secret
	closed.Close()
	provider := h.seedProvider(t, h.orgID, map[string]any{"client_id": h.clientID, "issuer": h.issuer,
		"token_endpoint": tokenURL, "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
	var logged bytes.Buffer
	cfg := h.svc.cfg
	cfg.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	s := h.startFor(t, provider)
	_, failure := h.finishWith(t, provider, s, h.sign(t, h.claims(s.nonce, "sub-log", uniqueEmail("log"))), true)
	wantDenied(t, failure, http.StatusBadRequest, ReasonDependency)
	if !strings.Contains(logged.String(), "error.class=refused") {
		t.Fatalf("log %q lacks error.class=refused", logged.String())
	}
	if strings.Contains(logged.String(), secret) || strings.Contains(logged.String(), "/token") {
		t.Fatalf("the log line holds the token URL: %q", logged.String())
	}
}

func TestEntitlementIsCheckedBeforeAnyDecode(t *testing.T) {
	ctx := context.Background()
	for name, raw := range map[string]string{"array": `[]`, "scopes not a list": `{"client_id":"c","scopes":"openid"}`,
		"claim mapping not an object": `{"client_id":"c","claim_mapping":[1]}`} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			unlicensed := h.seedRawProvider(t, h.seedOrgTier(t, "community"), raw)
			licensed := h.seedRawProvider(t, h.orgID, raw)
			var failure *Error

			_, err := h.svc.Authorize(ctx, unlicensed, "")
			if !errors.As(err, &failure) || failure.Status != http.StatusPaymentRequired || failure.Reason != ReasonNotEntitled {
				t.Fatalf("authorize, non-entitled org with a malformed stored config = %v, want 402 not_entitled", err)
			}
			_, _, err = h.svc.Callback(ctx, unlicensed, CallbackInput{Code: "c", State: "s"})
			if !errors.As(err, &failure) || failure.Status != http.StatusPaymentRequired {
				t.Fatalf("callback, non-entitled org with a malformed stored config = %v, want 402", err)
			}
			_, err = h.svc.Authorize(ctx, licensed, "")
			if !errors.Is(err, ErrProviderConfig) {
				t.Fatalf("authorize, entitled org with a malformed stored config = %v, want ErrProviderConfig", err)
			}
		})
	}
}

func TestEntitlementIsCheckedBeforeTheRequestBody(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	unlicensed := h.seedProvider(t, h.seedOrgTier(t, "community"), map[string]any{"client_id": h.clientID, "issuer": h.issuer,
		"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/auth/oidc/{provider_id}/authorize", h.svc.AuthorizeHandler())
	mux.Handle("POST /api/v1/auth/oidc/{provider_id}/callback", h.svc.CallbackHandler())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	for _, route := range []string{"authorize", "callback"} {
		resp, err := http.Post(server.URL+"/api/v1/auth/oidc/"+unlicensed.String()+"/"+route, "application/json",
			strings.NewReader(`{"code":1,"state":`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusPaymentRequired {
			t.Fatalf("%s with a malformed body for a non-entitled org = %d, want 402", route, resp.StatusCode)
		}
	}
}

func TestProviderChecksPrecedeConfigDecode(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	for name, row := range map[string][2]string{"another protocol": {"saml", "active"}, "inactive": {"oidc", "inactive"}} {
		t.Run(name, func(t *testing.T) {
			provider := h.seedRawProviderAs(t, h.orgID, `[]`, row[0], row[1])
			for route, call := range map[string]func() error{
				"authorize": func() error { _, err := h.svc.Authorize(context.Background(), provider, ""); return err },
				"callback": func() error {
					_, _, err := h.svc.Callback(context.Background(), provider, CallbackInput{Code: "c", State: "s"})
					return err
				},
			} {
				var failure *Error
				if err := call(); !errors.As(err, &failure) || failure.Status != http.StatusBadRequest {
					t.Fatalf("%s with a malformed stored config = %v, want 400 before the config is decoded", route, err)
				}
			}
		})
	}
}

// outcomeCounts reads auth.oidc.outcomes by its route, outcome and reason labels.
func outcomeCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "auth.oidc.outcomes" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("auth.oidc.outcomes is %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				route, _ := dp.Attributes.Value("route")
				outcome, _ := dp.Attributes.Value("outcome")
				reason, _ := dp.Attributes.Value("reason")
				out[route.AsString()+"/"+outcome.AsString()+"/"+reason.AsString()] += dp.Value
			}
		}
	}
	return out
}

func TestEveryOutcomeIsRecordedOnce(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	reader := sdkmetric.NewManualReader()
	var logged bytes.Buffer
	cfg := h.svc.cfg
	cfg.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	cfg.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	unlicensed := h.seedProvider(t, h.seedOrgTier(t, "community"), map[string]any{"client_id": h.clientID, "issuer": h.issuer,
		"token_endpoint": h.idp.server.URL + "/token", "jwks_uri": h.idp.server.URL + "/jwks"}, true, nil)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/auth/oidc/{provider_id}/authorize", svc.AuthorizeHandler())
	mux.Handle("POST /api/v1/auth/oidc/{provider_id}/callback", svc.CallbackHandler())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	for _, route := range []string{"authorize", "callback"} {
		resp, err := http.Post(server.URL+"/api/v1/auth/oidc/"+unlicensed.String()+"/"+route, "application/json",
			strings.NewReader(`{"code":"c","state":"s"}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusPaymentRequired {
			t.Fatalf("%s for a non-entitled organization = %d, want 402", route, resp.StatusCode)
		}
	}
	s := h.start(t)
	if _, failure := h.finish(t, s, h.sign(t, h.claims(s.nonce, "sub-counted", uniqueEmail("counted")))); failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	counts := outcomeCounts(t, reader)
	for key, want := range map[string]int64{
		"authorize/denied/not_entitled": 1, "callback/denied/not_entitled": 1, "callback/success/accepted": 1,
	} {
		if counts[key] != want {
			t.Errorf("auth.oidc.outcomes %s = %d, want %d (all: %v)", key, counts[key], want, counts)
		}
	}
	if n := strings.Count(logged.String(), "reason=not_entitled"); n != 2 {
		t.Errorf("refusal log lines with reason=not_entitled = %d, want 2:\n%s", n, logged.String())
	}
	wantOneLogLinePerOutcome(t, logged.String(), counts)
}

// wantOneLogLinePerOutcome holds every counted outcome (route/outcome/reason)
// to exactly as many log lines naming it, and no outcome log line to be
// uncounted: success and refusal alike.
func wantOneLogLinePerOutcome(t *testing.T, logged string, counts map[string]int64) {
	t.Helper()
	var total int64
	for key, n := range counts {
		total += n
		parts := strings.SplitN(key, "/", 3)
		needle := "route=" + parts[0] + " outcome=" + parts[1] + " reason=" + parts[2]
		if got := int64(strings.Count(logged, needle)); got != n {
			t.Errorf("log lines %q = %d, want %d (the counted outcomes):\n%s", needle, got, n, logged)
		}
	}
	if lines := int64(strings.Count(logged, " outcome=")); lines != total {
		t.Errorf("outcome log lines = %d, counted outcomes = %d:\n%s", lines, total, logged)
	}
}
