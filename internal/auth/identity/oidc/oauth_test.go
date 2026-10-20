package oidc

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
)

func TestOIDCAndOAuthStatesDoNotOpenAsEachOther(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	provider := uuid.NewString()
	oidcToken, _, err := sealState(testStateSecret, testFlowState(provider), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openOAuthState(testStateSecret, oidcToken, provider, now); !errors.Is(err, errStateInvalid) {
		t.Fatalf("an OIDC state opened as an OAuth state: %v", err)
	}
	oauthToken, sealed, err := sealOAuthState(testStateSecret, oauthFlowState{ProviderID: provider, OrgID: uuid.NewString(),
		RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("c")}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openState(testStateSecret, oauthToken, provider, now); !errors.Is(err, errStateInvalid) {
		t.Fatalf("an OAuth state opened as an OIDC state: %v", err)
	}
	got, err := openOAuthState(testStateSecret, oauthToken, provider, now)
	if err != nil || got.ID != sealed.ID {
		t.Fatalf("OAuth state round trip: %+v %v", got, err)
	}
	if _, err := openOAuthState(testStateSecret, oauthToken, uuid.NewString(), now); !errors.Is(err, errStateInvalid) {
		t.Fatalf("an OAuth state opened for another provider: %v", err)
	}
	if _, err := openOAuthState(testStateSecret, oauthToken, provider, now.Add(stateTTL+time.Second)); !errors.Is(err, errStateExpired) {
		t.Fatalf("expired OAuth state: %v", err)
	}
}

func TestOAuthConfigRefusesInsecureURLs(t *testing.T) {
	for raw, ok := range map[string]bool{
		`{"client_id":"c","base_url":"https://gitlab.example"}`:    true,
		`{"client_id":"c","base_url":"http://localhost:8080"}`:     true,
		`{"client_id":"c","base_url":"http://127.0.0.1"}`:          true,
		`{"client_id":"c","base_url":"http://gitlab.example"}`:     false,
		`{"client_id":"c","base_url":"HTTP://gitlab.example"}`:     false,
		`{"client_id":"c","base_url":"//gitlab.example"}`:          false,
		`{"client_id":"c","redirect_uri":"http://app.example/cb"}`: false,
		`{"client_id":"c","scopes":"read_user"}`:                   false,
		`[]`:                                                       false,
	} {
		if _, err := decodeOAuthConfig(raw); (err == nil) != ok {
			t.Errorf("%s: err=%v, want accepted=%v", raw, err, ok)
		}
	}
}

func TestOAuthTargetPerProvider(t *testing.T) {
	s := &Service{cfg: Config{}}
	for protocol, want := range map[string]struct {
		pkce   bool
		issuer string
	}{
		"oauth_github": {true, "https://github.com"},
		"oauth_gitlab": {true, "https://gitlab.com"},
		"oauth_google": {true, "https://accounts.google.com"},
	} {
		target, failure := s.oauthTarget(Provider{Protocol: protocol})
		if failure != nil || target.pkce != want.pkce || target.issuer != want.issuer {
			t.Errorf("%s: %+v %v, want pkce=%v issuer=%s", protocol, target, failure, want.pkce, want.issuer)
		}
	}
	target, _ := s.oauthTarget(Provider{Protocol: "oauth_gitlab", OAuth: OAuthConfig{BaseURL: "https://git.example/"}})
	if target.issuer != "https://git.example" || target.endpoints.GitLabBase != "https://git.example" ||
		target.token != "https://git.example/oauth/token" {
		t.Fatalf("self-hosted GitLab target %+v", target)
	}
	if _, ok := oauthProviderOf("oidc"); ok {
		t.Fatal("oidc is not an OAuth protocol")
	}
	if p, ok := oauthProviderOf("oauth_google"); !ok || p != social.Google {
		t.Fatal("oauth_google")
	}
}

func TestOAuthGitHubConfigAcceptsOnlyGitHubCom(t *testing.T) {
	for raw, ok := range map[string]bool{
		`{"client_id":"c"}`: true,
		`{"client_id":"c","base_url":"https://github.com"}`:  true,
		`{"client_id":"c","base_url":"https://github.com/"}`: true,
		`{"client_id":"c","base_url":"https://ghe.example"}`: false,
		`{"client_id":"c","base_url":"http://localhost:80"}`: false,
	} {
		cfg, err := decodeOAuthConfig(raw)
		if err == nil {
			err = checkOAuthBase("oauth_github", cfg)
		}
		if (err == nil) != ok {
			t.Errorf("%s: err=%v, want accepted=%v", raw, err, ok)
		}
	}
	if err := checkOAuthBase("oauth_gitlab", OAuthConfig{BaseURL: "https://git.example"}); err != nil {
		t.Fatalf("a self-hosted GitLab base: %v", err)
	}
}
