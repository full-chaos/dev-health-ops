package identity

import (
	"errors"
	"regexp"
	"unicode"
	"unicode/utf8"
)

// ErrInvalidExternalKey reports an external identity key that cannot be stored
// or looked up.
var ErrInvalidExternalKey = errors.New("identity: invalid external identity key")

// Provider names who asserted an external identity: one of the social
// providers below, or, for enterprise SSO (OIDC, OAuth, SAML), the id of the
// public.sso_providers row in canonical lower-case UUID form. An SSO subject is
// scoped to one configured provider row, never to a protocol name.
type Provider string

const (
	ProviderGitHub Provider = "github"
	ProviderGitLab Provider = "gitlab"
	ProviderGoogle Provider = "google"
)

// socialIssuers is the fixed issuer of each social provider. A social key must
// carry exactly this issuer.
var socialIssuers = map[Provider]string{
	ProviderGitHub: "https://github.com",
	ProviderGitLab: "https://gitlab.com",
	ProviderGoogle: "https://accounts.google.com",
}

// SocialProviders lists the social providers.
func SocialProviders() []Provider {
	return []Provider{ProviderGitHub, ProviderGitLab, ProviderGoogle}
}

// SocialIssuer returns the fixed issuer of a social provider.
func SocialIssuer(p Provider) (string, bool) {
	issuer, ok := socialIssuers[p]
	return issuer, ok
}

var canonicalUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// IsSocial reports whether p is a social provider.
func (p Provider) IsSocial() bool {
	_, ok := socialIssuers[p]
	return ok
}

// IsSSO reports whether p is an SSO provider row id (canonical UUID form).
func (p Provider) IsSSO() bool { return canonicalUUID.MatchString(string(p)) }

const (
	maxIssuerBytes  = 2048
	maxSubjectBytes = 1024
)

// ExternalKey is what an upstream provider asserts: the issuer that signed the
// assertion, the provider and the provider's subject for the person. The three
// together are unique in auth.external_identities. Issuer and subject are
// compared exactly, byte for byte: no case folding, no trimming, no Unicode
// normalization, because a provider subject is an opaque identifier and two
// spellings are two identities.
type ExternalKey struct {
	Issuer   string
	Provider Provider
	Subject  string
}

// Validate refuses a key whose provider is neither a social provider nor an
// SSO provider row id, a social key whose issuer is not that provider's fixed
// issuer, or an issuer or subject that is empty, too long, not valid UTF-8,
// carries a control character, or begins or ends with white space.
func (k ExternalKey) Validate() error {
	switch {
	case k.Provider.IsSocial():
		if k.Issuer != socialIssuers[k.Provider] {
			return ErrInvalidExternalKey
		}
	case k.Provider.IsSSO():
	default:
		return ErrInvalidExternalKey
	}
	if !validPart(k.Issuer, maxIssuerBytes) || !validPart(k.Subject, maxSubjectBytes) {
		return ErrInvalidExternalKey
	}
	return nil
}

func validPart(v string, maxBytes int) bool {
	if v == "" || len(v) > maxBytes || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || r == utf8.RuneError {
			return false
		}
	}
	first, _ := utf8.DecodeRuneInString(v)
	last, _ := utf8.DecodeLastRuneInString(v)
	return !unicode.IsSpace(first) && !unicode.IsSpace(last)
}
