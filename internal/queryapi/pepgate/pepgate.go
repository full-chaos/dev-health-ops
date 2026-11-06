// Package pepgate is the scope a query-api REST read may serve: the organization and the repository ceiling an
// enforced policy decision allowed, or the organization of today's legacy path. The zero value is no scope: it names
// no organization and is restricted, so nothing can read it as unrestricted.
package pepgate

// Kind says where a Scope came from.
type Kind uint8

const (
	// KindNone is the zero value and every refusal: no organization, nothing to serve.
	KindNone Kind = iota
	// KindLegacy is a request the PDP does not decide (no cell on, an exempt credential class): today's path.
	KindLegacy
	// KindOrganization is an enforced allow over every repository of the tenant.
	KindOrganization
	// KindAllowlist is an enforced allow over the listed repositories only; an empty list allows none.
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

// Legacy is the scope of a request the PDP does not decide. An empty org is no scope.
func Legacy(org string) Scope { return build(org, KindLegacy, nil) }

// Organization is an enforced allow over every repository of org. An empty org is no scope.
func Organization(org string) Scope { return build(org, KindOrganization, nil) }

// Allowlist is an enforced allow over the repositories keys names; nil and empty allow none. The scope keeps a copy.
// An empty org is no scope.
func Allowlist(org string, keys []string) Scope {
	return build(org, KindAllowlist, &keyList{keys: append([]string{}, keys...)})
}

func build(org string, kind Kind, keys *keyList) Scope {
	if org == "" {
		return Scope{}
	}
	return Scope{org: org, kind: kind, keys: keys}
}

// Kind is where the scope came from.
func (s Scope) Kind() Kind { return s.kind }

// OrgID is the organization a read must stay within; false for no scope, and then a read must not run.
func (s Scope) OrgID() (string, bool) {
	if s.kind == KindNone {
		return "", false
	}
	return s.org, true
}

// Restricted reports a repository ceiling: an allowlist, or no scope at all. A restricted read serves only the
// repositories RepoKeys names.
func (s Scope) Restricted() bool { return s.kind == KindNone || s.kind == KindAllowlist }

// RepoKeys is a copy of the allowlist keys: non-nil (empty allows none) for an allowlist scope, nil otherwise.
func (s Scope) RepoKeys() []string {
	if s.kind != KindAllowlist || s.keys == nil {
		return nil
	}
	return append([]string{}, s.keys.keys...)
}
