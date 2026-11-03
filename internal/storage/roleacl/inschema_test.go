package roleacl

import "testing"

func TestGrantInSchema(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		grant Grant
		want  bool
	}{
		{Grant{Class: "relation", Object: "auth.sessions"}, true},
		{Grant{Class: "column", Object: "auth.sessions.user_id"}, true},
		{Grant{Class: "function", Object: "auth.fn(text)"}, true},
		{Grant{Class: "type", Object: "auth.kind"}, true},
		{Grant{Class: "schema", Object: "auth"}, true},
		{Grant{Class: "default privileges", Object: "r for devhealth_auth_migrator in auth"}, true},
		{Grant{Class: "relation", Object: "authx.sessions"}, false},
		{Grant{Class: "relation", Object: "public.users"}, false},
		{Grant{Class: "schema", Object: "authority"}, false},
		{Grant{Class: "default privileges", Object: "r for x in authx"}, false},
		{Grant{Class: "database", Object: "auth"}, false},
	} {
		if got := c.grant.InSchema("auth"); got != c.want {
			t.Errorf("%+v InSchema(auth) = %v, want %v", c.grant, got, c.want)
		}
	}
}
