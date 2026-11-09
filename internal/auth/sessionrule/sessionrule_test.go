package sessionrule

import (
	"strings"
	"testing"
	"time"
)

const schema = `"auth"`

func rule(t *testing.T) Rule {
	t.Helper()
	r, err := New(schema, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNewRequiresASchemaAndARefreshLifetime(t *testing.T) {
	if _, err := New("", time.Hour); err == nil {
		t.Error("an empty schema was accepted")
	}
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, err := New(schema, ttl); err == nil {
			t.Errorf("refresh lifetime %v was accepted: there is no default here", ttl)
		}
	}
}

const wantRevoked = `COALESCE(COALESCE(s.revoked_at,
		(SELECT u.password_updated_at FROM "auth".users u
		 WHERE u.principal_id = s.principal_id AND u.password_updated_at > s.created_at)), (SELECT COALESCE(s_wp.revoked_at,
		(SELECT u.password_updated_at FROM "auth".users u
		 WHERE u.principal_id = s_wp.principal_id AND u.password_updated_at > s_wp.created_at)) FROM "auth".sessions s_wp WHERE s_wp.id = s.web_session_id))`

const wantExpiry = `LEAST(LEAST(s.expires_at, COALESCE(
		(SELECT r.expires_at FROM "auth".refresh_credentials r WHERE r.session_id = s.id
		 ORDER BY r.issued_at DESC, r.created_at DESC LIMIT 1),
		COALESCE(s.last_seen_at, s.created_at) + 604800000000 * interval '1 microsecond')), (SELECT LEAST(s_wp.expires_at, COALESCE(
		(SELECT r.expires_at FROM "auth".refresh_credentials r WHERE r.session_id = s_wp.id
		 ORDER BY r.issued_at DESC, r.created_at DESC LIMIT 1),
		COALESCE(s_wp.last_seen_at, s_wp.created_at) + 604800000000 * interval '1 microsecond')) FROM "auth".sessions s_wp WHERE s_wp.id = s.web_session_id))`

const wantCap = `LEAST(s.expires_at, (SELECT LEAST(s_wp.expires_at, COALESCE(
		(SELECT r.expires_at FROM "auth".refresh_credentials r WHERE r.session_id = s_wp.id
		 ORDER BY r.issued_at DESC, r.created_at DESC LIMIT 1),
		COALESCE(s_wp.last_seen_at, s_wp.created_at) + 604800000000 * interval '1 microsecond')) FROM "auth".sessions s_wp WHERE s_wp.id = s.web_session_id))`

// The exact text is pinned: an edit of one clause is a failure here, and in the
// parity cells of decisionstate and the store.
func TestTheFragmentsArePinned(t *testing.T) {
	r := rule(t)
	for name, c := range map[string]struct{ got, want string }{
		"Revoked": {r.Revoked("s"), wantRevoked},
		"Expiry":  {r.Expiry("s"), wantExpiry},
		"Cap":     {r.Cap("s"), wantCap},
		"Live":    {r.Live("s", "now()"), `(` + wantRevoked + ` IS NULL AND ` + wantExpiry + ` > now())`},
	} {
		if c.got != c.want {
			t.Errorf("%s changed:\n got %q\nwant %q", name, c.got, c.want)
		}
	}
}

func TestEveryClauseOfTheRuleIsPresent(t *testing.T) {
	r := rule(t)
	revoked, expiry := r.Revoked("x"), r.Expiry("x")
	for _, c := range []struct{ what, in, has string }{
		{"own revoke", revoked, "x.revoked_at"},
		{"password stamp after created_at", revoked, "u.password_updated_at > x.created_at"},
		{"web parent revoke", revoked, "x_wp.revoked_at"},
		{"web parent stamp against the parent's created_at", revoked, "u.password_updated_at > x_wp.created_at"},
		{"web parent link", revoked, `"auth".sessions x_wp WHERE x_wp.id = x.web_session_id`},
		{"own absolute expiry", expiry, "LEAST(x.expires_at"},
		{"newest credential", expiry, "ORDER BY r.issued_at DESC, r.created_at DESC LIMIT 1"},
		{"no credential: last rotation plus the lifetime", expiry, "COALESCE(x.last_seen_at, x.created_at) + 604800000000"},
		{"web parent expiry", expiry, "LEAST(x_wp.expires_at"},
		{"web parent credential", expiry, "r.session_id = x_wp.id"},
		{"web parent link", expiry, `"auth".sessions x_wp WHERE x_wp.id = x.web_session_id`},
	} {
		if !strings.Contains(c.in, c.has) {
			t.Errorf("the rule lost its %s clause (%q)", c.what, c.has)
		}
	}
}

func TestTheRefreshLifetimeIsInTheExpiry(t *testing.T) {
	r, err := New(schema, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Expiry("s"); !strings.Contains(got, "+ 3600000000 * interval") || strings.Contains(got, "604800000000") {
		t.Errorf("the expiry does not carry the one-hour lifetime: %s", got)
	}
}

func TestTwoRowsOfOneStatementDoNotShareAParentAlias(t *testing.T) {
	r := rule(t)
	live := r.Live("s", "now()") + r.Live("b", "now()")
	for _, want := range []string{"s_wp", "b_wp"} {
		if !strings.Contains(live, want) {
			t.Errorf("no %s alias in %s", want, live)
		}
	}
	if strings.Contains(r.Live("b", "now()"), "s_wp") {
		t.Error("the b fragment reads the s parent")
	}
}

func TestAnAliasThatIsNotAnIdentifierIsRefused(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("an alias with SQL in it was accepted")
		}
	}()
	rule(t).Live("s; DROP TABLE x", "now()")
}

// The revoke fragment without a lifetime is the rule's, byte for byte.
func TestRevocationIsTheRulesRevokeWithoutALifetime(t *testing.T) {
	if _, err := NewRevocation(""); err == nil {
		t.Error("an empty schema was accepted")
	}
	v, err := NewRevocation(schema)
	if err != nil {
		t.Fatal(err)
	}
	if got := v.Revoked("s"); got != wantRevoked {
		t.Errorf("Revocation.Revoked changed:\n got %q\nwant %q", got, wantRevoked)
	}
	if got, want := v.Revoked("x"), rule(t).Revoked("x"); got != want {
		t.Errorf("Revocation.Revoked(x) = %q, Rule.Revoked(x) = %q", got, want)
	}
}
