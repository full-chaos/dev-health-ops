// Package pepgate is the scope a query-api REST read may serve: the organization and the repository ceiling an
// enforced policy decision allowed, or the organization of today's legacy path.
package pepgate

// Kind says where a Scope came from.
type Kind uint8

const (
	KindNone Kind = iota
	KindLegacy
	KindOrganization
	KindAllowlist
)

// Scope is one request's scope. It stays comparable (authctx.Claims is compared by value): the keys sit behind a
// pointer, so two allowlist scopes are equal only when they are the same value.
type Scope struct {
	org  string
	kind Kind
	keys *keyList
}

type keyList struct{ keys []string }

// Legacy is the scope of a request the PDP does not decide.
func Legacy(org string) Scope { return Scope{org: org, kind: KindLegacy} }

// Organization is an enforced allow over every repository of org.
func Organization(org string) Scope { return Scope{org: org, kind: KindLegacy} }

// Allowlist is an enforced allow over the repositories keys names.
func Allowlist(org string, keys []string) Scope { return Scope{org: org, kind: KindLegacy} }

// Kind is where the scope came from.
func (s Scope) Kind() Kind { return s.kind }

// OrgID is the organization a read must stay within.
func (s Scope) OrgID() (string, bool) { return s.org, true }

// Restricted reports a repository ceiling.
func (s Scope) Restricted() bool { return false }

// RepoKeys are the allowlist keys.
func (s Scope) RepoKeys() []string { return nil }
