package identity

import "errors"

// ErrInvalidExternalKey reports an external identity key that cannot be stored
// or looked up.
var ErrInvalidExternalKey = errors.New("identity: invalid external identity key")

// Provider names who asserted an external identity.
type Provider string

const (
	ProviderGitHub Provider = "github"
	ProviderGitLab Provider = "gitlab"
	ProviderGoogle Provider = "google"
)

// SocialProviders lists the social providers.
func SocialProviders() []Provider {
	return []Provider{ProviderGitHub, ProviderGitLab, ProviderGoogle}
}

// SocialIssuer is a stub.
func SocialIssuer(p Provider) (string, bool) { return "", false }

// IsSocial is a stub.
func (p Provider) IsSocial() bool { return false }

// IsSSO is a stub.
func (p Provider) IsSSO() bool { return false }

const (
	maxIssuerBytes  = 2048
	maxSubjectBytes = 1024
)

// ExternalKey is what an upstream provider asserts.
type ExternalKey struct {
	Issuer   string
	Provider Provider
	Subject  string
}

// Validate is a stub that accepts every key.
func (k ExternalKey) Validate() error { return nil }
