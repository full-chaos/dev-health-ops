package identity

import (
	"errors"
	"strings"
	"testing"
)

func TestGitLabInstanceIssuer(t *testing.T) {
	for base, want := range map[string]string{
		"":                                   "https://gitlab.com",
		"https://gitlab.com":                 "https://gitlab.com",
		"https://GitLab.com/":                "https://gitlab.com",
		"https://gitlab.com//":               "https://gitlab.com",
		"https://gitlab.com/group":           "https://gitlab.com",
		"https://git.corp.example":           "https://git.corp.example",
		"https://Git.Corp.Example:8443/gl/":  "https://git.corp.example:8443",
		"HTTPS://git.corp.example":           "https://git.corp.example",
		"http://127.0.0.1:8080":              "http://127.0.0.1:8080",
		"https://user:pw@git.corp.example/x": "https://git.corp.example",
	} {
		got, err := GitLabInstanceIssuer(base)
		if err != nil || got != want {
			t.Fatalf("base_url %q = %q, %v; want %q", base, got, err, want)
		}
	}
	for _, bad := range []string{"git.corp.example", "//git.corp.example", "ftp://git.corp.example", "https://", "https:git.corp.example", "https://%zz"} {
		if got, err := GitLabInstanceIssuer(bad); !errors.Is(err, ErrInstanceBaseURL) || got != "" {
			t.Fatalf("base_url %q = %q, %v; want ErrInstanceBaseURL", bad, got, err)
		}
	}
}

func TestOAuthIssuerIsFixedExceptForGitLab(t *testing.T) {
	for p, want := range map[Provider]string{ProviderGitHub: "https://github.com", ProviderGoogle: "https://accounts.google.com"} {
		if got, err := OAuthIssuer(p, "https://git.corp.example"); err != nil || got != want {
			t.Fatalf("%s = %q, %v; want %q", p, got, err, want)
		}
	}
	if got, err := OAuthIssuer(ProviderGitLab, "https://git.corp.example"); err != nil || got != "https://git.corp.example" {
		t.Fatalf("gitlab = %q, %v", got, err)
	}
	if _, err := OAuthIssuer(Provider("oidc"), ""); !errors.Is(err, ErrInvalidExternalKey) {
		t.Fatalf("non-social provider: %v", err)
	}
}

func TestSelfHostedGitLab(t *testing.T) {
	for _, c := range []struct {
		p      Provider
		issuer string
		want   bool
	}{
		{ProviderGitLab, "https://gitlab.com", false},
		{ProviderGitLab, "https://git.corp.example", true},
		{ProviderGitHub, "https://github.com", false},
		{ProviderGitHub, "https://git.corp.example", false},
	} {
		if got := SelfHostedGitLab(c.p, c.issuer); got != c.want {
			t.Fatalf("%s at %s = %v, want %v", c.p, c.issuer, got, c.want)
		}
	}
}

func TestOAuthAccountScopeSQLByIssuer(t *testing.T) {
	social, needs := OAuthAccountScopeSQL("u", "https://gitlab.com", "$3")
	if needs || strings.Contains(social, "$3") || !strings.Contains(social, "NOT EXISTS") {
		t.Fatalf("social scope %q needs=%v", social, needs)
	}
	hosted, needs := OAuthAccountScopeSQL("u", "https://git.corp.example", "$3")
	if !needs || !strings.Contains(hosted, "= $3::text") || strings.HasPrefix(hosted, "NOT") {
		t.Fatalf("instance scope %q needs=%v", hosted, needs)
	}
	for _, sql := range []string{social, hosted} {
		if !strings.Contains(sql, "protocol = 'oauth_gitlab'") || !strings.Contains(sql, "config::jsonb->>'base_url'") {
			t.Fatalf("the scope does not read the provider config: %q", sql)
		}
	}
}
