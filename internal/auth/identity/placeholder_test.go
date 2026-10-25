package identity

import "testing"

func TestServicePlaceholderPredicate(t *testing.T) {
	s := func(v string) *string { return &v }
	for name, c := range map[string]struct {
		provider, hash, id *string
		want               bool
	}{
		"service, no credential":           {s("service"), nil, nil, true},
		"service, empty strings":           {s("service"), s(""), s(""), true},
		"service with a password hash":     {s("service"), s("$2b$12$x"), nil, false},
		"service with a provider id":       {s("service"), nil, s("sub-1"), false},
		"saml without credential":          {s("saml"), nil, nil, false},
		"oidc without credential":          {s("oidc"), nil, nil, false},
		"github without credential":        {s("github"), nil, nil, false},
		"local without credential":         {s("local"), nil, nil, false},
		"no provider":                      {nil, nil, nil, false},
		"another case of the same literal": {s("Service"), nil, nil, false},
	} {
		if got := IsServicePlaceholder(c.provider, c.hash, c.id); got != c.want {
			t.Errorf("%s: IsServicePlaceholder = %v, want %v", name, got, c.want)
		}
	}
}

func TestServicePlaceholderSQLNamesTheSameLiteralAsTheGoPredicate(t *testing.T) {
	want := "(u.auth_provider = 'service' AND coalesce(u.password_hash, '') = '' AND coalesce(u.auth_provider_id, '') = '')"
	if got := ServicePlaceholderSQL("u"); got != want {
		t.Fatalf("ServicePlaceholderSQL = %q, want %q", got, want)
	}
}
