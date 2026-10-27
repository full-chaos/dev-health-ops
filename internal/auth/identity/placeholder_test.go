package identity

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

func TestServicePlaceholderPredicate(t *testing.T) {
	s := func(v string) *string { return &v }
	prove, admin, other := uuid.MustParse(edgetokenmint.ProvePrincipalID), uuid.MustParse(edgetokenmint.AdminProofPrincipalID), uuid.New()
	for name, c := range map[string]struct {
		id                  uuid.UUID
		provider, hash, pid *string
		want                bool
	}{
		"proof principal, service, no credential":       {prove, s("service"), nil, nil, true},
		"admin proof principal, service, no credential": {admin, s("service"), nil, nil, true},
		"proof principal, empty strings":                {prove, s("service"), s(""), s(""), true},
		"another id, same shape":                        {other, s("service"), nil, nil, false},
		"the nil id, same shape":                        {uuid.Nil, s("service"), nil, nil, false},
		"proof principal with a password hash":          {prove, s("service"), s("$2b$12$x"), nil, false},
		"proof principal with a provider id":            {prove, s("service"), nil, s("sub-1"), false},
		"proof principal, saml":                         {prove, s("saml"), nil, nil, false},
		"proof principal, local":                        {prove, s("local"), nil, nil, false},
		"proof principal, no provider":                  {prove, nil, nil, nil, false},
		"proof principal, another case of the literal":  {prove, s("Service"), nil, nil, false},
	} {
		if got := IsServicePlaceholder(c.id, c.provider, c.hash, c.pid); got != c.want {
			t.Errorf("%s: IsServicePlaceholder = %v, want %v", name, got, c.want)
		}
	}
}

func TestServicePlaceholderSQLNamesTheSameLiteralAsTheGoPredicate(t *testing.T) {
	want := "(u.id IN ('" + edgetokenmint.ProvePrincipalID + "'::uuid, '" + edgetokenmint.AdminProofPrincipalID + "'::uuid)" +
		" AND coalesce(u.auth_provider, '') = '" + edgetokenmint.ServiceAuthProvider + "'" +
		" AND coalesce(u.password_hash, '') = '' AND coalesce(u.auth_provider_id, '') = '')"
	if got := ServicePlaceholderSQL("u"); got != want {
		t.Fatalf("ServicePlaceholderSQL = %q, want %q", got, want)
	}
}

func TestServiceProviderMatchesEveryRowOfTheProviderOnly(t *testing.T) {
	s := func(v string) *string { return &v }
	for provider, want := range map[*string]bool{s("service"): true, s("Service"): false, s("service "): false, s("local"): false, s(""): false, nil: false} {
		if got := IsServiceProvider(provider); got != want {
			name := "<nil>"
			if provider != nil {
				name = *provider
			}
			t.Errorf("IsServiceProvider(%q) = %v, want %v", name, got, want)
		}
	}
	if got, want := ServiceProviderSQL("p"), "coalesce(p.auth_provider, '') = '"+edgetokenmint.ServiceAuthProvider+"'"; got != want {
		t.Fatalf("ServiceProviderSQL = %q, want %q", got, want)
	}
}
