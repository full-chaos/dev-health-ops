package clickhouse

import (
	"slices"
	"testing"
)

// The driver settles the login and password by its own DSN parser, which takes
// forms net/url refuses. Every value is made up.
func TestResolvedCredentialsAreTheOnesTheDriverSettlesOn(t *testing.T) {
	for name, c := range map[string]struct {
		dsn  string
		want []string
	}{
		"userinfo":                    {"clickhouse://zz9-login:zz9-pass@127.0.0.1:9000/d", []string{"zz9-login", "zz9-pass"}},
		"IPv6 literal in a host list": {"clickhouse://zz9-login:zz9-pass@127.0.0.1:1,[::ffff:127.0.0.1]:9000/d", []string{"zz9-login", "zz9-pass"}},
		"escaped userinfo":            {"clickhouse://zz9%3Alogin:zz9%40pass@127.0.0.1:9000/d", []string{"zz9:login", "zz9@pass"}},
		"query username and password": {"clickhouse://127.0.0.1:9000/d?username=zz9-login&password=zz9-pass", []string{"zz9-login", "zz9-pass"}},
		"login only":                  {"clickhouse://zz9-login@127.0.0.1:9000/d", []string{"zz9-login"}},
		"no credentials":              {"clickhouse://127.0.0.1:9000/d", nil},
		"a DSN the driver refuses":    {"clickhouse://zz9-login:zz9-pass@/d", nil},
	} {
		if got := ResolvedCredentials(c.dsn); !slices.Equal(got, c.want) {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}
