package identity

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ErrInstanceBaseURL reports a provider base_url that names no instance host.
var ErrInstanceBaseURL = errors.New("identity: provider base_url names no instance host")

// GitLabInstanceIssuer is the issuer of a GitLab OAuth-SSO identity: scheme and lowercase host (port kept) of the
// provider's configured base_url. An empty base_url is gitlab.com. A base_url on the gitlab.com host is the
// gitlab.com issuer itself, so such a row keeps the social key shape. A self-hosted instance is its own issuer. Path,
// query, user info and trailing slashes are not part of it. GitLabInstanceIssuerSQL is its SQL twin.
func GitLabInstanceIssuer(baseURL string) (string, error) {
	social, _ := SocialIssuer(ProviderGitLab)
	if baseURL == "" {
		return social, nil
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", ErrInstanceBaseURL
	}
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "https" && scheme != "http") || u.Host == "" {
		return "", ErrInstanceBaseURL
	}
	return scheme + "://" + strings.ToLower(u.Host), nil
}

// OAuthIssuer is the issuer of an OAuth-SSO key of provider p whose row names baseURL: the fixed social issuer, except
// for GitLab, whose instance host is the issuer.
func OAuthIssuer(p Provider, baseURL string) (string, error) {
	if p == ProviderGitLab {
		return GitLabInstanceIssuer(baseURL)
	}
	issuer, ok := SocialIssuer(p)
	if !ok {
		return "", ErrInvalidExternalKey
	}
	return issuer, nil
}

// GitLabInstanceIssuerSQL is GitLabInstanceIssuer over the text expression base (a base_url, NULL or empty meaning
// gitlab.com); NULL when the value names no http or https host.
func GitLabInstanceIssuerSQL(base string) string {
	social, _ := SocialIssuer(ProviderGitLab)
	const re = `'^(https?)://([^/?#@]*@)?([^/?#]+)'`
	return fmt.Sprintf(`(CASE WHEN coalesce(%[1]s, '') = '' THEN %[2]s::text`+
		` ELSE lower((regexp_match(%[1]s, %[3]s, 'i'))[1]) || '://' || lower((regexp_match(%[1]s, %[3]s, 'i'))[3]) END)`,
		base, sqlLiteral(social), re)
}

// gitlabRowIssuerSQL is the issuer of the public.sso_providers row aliased alias.
func gitlabRowIssuerSQL(alias string) string {
	return GitLabInstanceIssuerSQL(alias + ".config::jsonb->>'base_url'")
}

// NotSelfHostedGitLabSQL holds for the public.users row aliased alias unless it is bound (sso_provider_id) to a
// self-hosted GitLab OAuth provider row: the social-namespace scope. Never NULL.
func NotSelfHostedGitLabSQL(alias string) string {
	social, _ := SocialIssuer(ProviderGitLab)
	return fmt.Sprintf(`NOT EXISTS (SELECT 1 FROM public.sso_providers b_inst WHERE b_inst.id = %[1]s.sso_provider_id`+
		` AND b_inst.protocol = 'oauth_gitlab' AND %[2]s IS DISTINCT FROM %[3]s::text)`,
		alias, gitlabRowIssuerSQL("b_inst"), sqlLiteral(social))
}

// BoundToGitLabInstanceSQL holds for the public.users row aliased alias when it is bound to a GitLab OAuth provider
// row whose instance issuer equals the text parameter issuerParam. The host comes from the provider config, never
// from the users row. Never NULL.
func BoundToGitLabInstanceSQL(alias, issuerParam string) string {
	return fmt.Sprintf(`EXISTS (SELECT 1 FROM public.sso_providers b_inst WHERE b_inst.id = %[1]s.sso_provider_id`+
		` AND b_inst.protocol = 'oauth_gitlab' AND %[2]s = %[3]s::text)`,
		alias, gitlabRowIssuerSQL("b_inst"), issuerParam)
}

// OAuthAccountScopeSQL is the carrier scope of an OAuth key at issuer: a key at a social issuer matches the accounts
// not bound to a self-hosted GitLab instance; a key at any other issuer matches only the accounts bound to a GitLab
// row of that instance, and then issuerParam (a text parameter) must be bound to issuer, which needsIssuer reports.
func OAuthAccountScopeSQL(alias, issuer, issuerParam string) (sql string, needsIssuer bool) {
	if IsSocialIssuer(issuer) {
		return NotSelfHostedGitLabSQL(alias), false
	}
	return BoundToGitLabInstanceSQL(alias, issuerParam), true
}

// IsSocialIssuer reports whether issuer is the fixed issuer of a social provider.
func IsSocialIssuer(issuer string) bool {
	for _, p := range SocialProviders() {
		if social, _ := SocialIssuer(p); social == issuer {
			return true
		}
	}
	return false
}

// SelfHostedGitLab reports whether an OAuth key of p at issuer is a self-hosted GitLab instance's.
func SelfHostedGitLab(p Provider, issuer string) bool {
	return p == ProviderGitLab && !IsSocialIssuer(issuer)
}
