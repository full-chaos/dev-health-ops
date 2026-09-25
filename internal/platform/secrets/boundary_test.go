package secrets

import (
	"errors"
	"strings"
	"testing"
)

func TestCredentialComponents_URLForm(t *testing.T) {
	dsn := "postgres://user:" + marker + "@host/db"
	got := CredentialComponents(dsn)
	if len(got) != 3 || got[0] != dsn || got[1] != marker || got[2] != "user" {
		t.Fatalf("CredentialComponents(%q) = %v, want [dsn, password, login name]", dsn, got)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestCredentialComponents_LoginName(t *testing.T) {
	for name, tc := range map[string]struct {
		dsn  string
		want []string // must be among the components
		not  []string // must not be
	}{
		"URL without a password":  {"clickhouse://alice-login@host:9000/db", []string{"alice-login"}, nil},
		"percent-encoded login":   {"clickhouse://ali%40ce:pw-x@host/db", []string{"ali@ce", "pw-x"}, nil},
		"empty login, a password": {"postgres://:pw-only@host/db", []string{"pw-only"}, []string{""}},
		"URL without userinfo":    {"clickhouse://host:9000/db", nil, []string{"host"}},
		"keyword bare":            {"host=h user=bob-login password=pw-k dbname=d", []string{"bob-login", "pw-k"}, nil},
		"keyword quoted":          {"host=h user = 'bob l\\'ogin' password=pw-k", []string{"bob l'ogin", "pw-k"}, nil},
		"keyword first":           {"user=first-login host=h", []string{"first-login"}, nil},
		"not another parameter":   {"host=h superuser=nope db_user=nope2 password=pw-k", []string{"pw-k"}, []string{"nope", "nope2"}},
		// The effective login is not always the userinfo or the first keyword:
		// clickhouse-go reads "username=" from the query, pgx lets a query "user"
		// override the userinfo and keeps the LAST duplicate keyword.
		"query username":             {"clickhouse://host:9000/db?username=query-login&password=pw-q", []string{"query-login", "pw-q"}, nil},
		"query user overrides":       {"postgres://info-login:pw@host/db?user=query-login", []string{"info-login", "query-login"}, nil},
		"query key case":             {"clickhouse://host/db?USERNAME=upper-login", []string{"upper-login"}, nil},
		"query repeated":             {"postgres://host/db?user=first-login&user=last-login", []string{"first-login", "last-login"}, nil},
		"query encoded":              {"postgres://host/db?user=us%40er", []string{"us@er"}, nil},
		"keyword duplicate user":     {"host=h user=first-login user=last-login password=pw", []string{"first-login", "last-login", "pw"}, nil},
		"keyword username":           {"host=h username=alt-login", []string{"alt-login"}, nil},
		"keyword duplicate password": {"host=h password=pw-one password=pw-two", []string{"pw-one", "pw-two"}, nil},
	} {
		got := CredentialComponents(tc.dsn)
		for _, want := range tc.want {
			if !contains(got, want) {
				t.Errorf("%s: CredentialComponents(%q) = %q, want %q among them", name, tc.dsn, got, want)
			}
		}
		for _, not := range tc.not {
			if contains(got, not) {
				t.Errorf("%s: CredentialComponents(%q) = %q, must not contain %q", name, tc.dsn, got, not)
			}
		}
	}
}

// TestBoundary_RedactsTheLoginNameTheServerEchoes is the shape of ClickHouse's
// own authentication-failure text: the login name leads it.
func TestBoundary_RedactsTheLoginNameTheServerEchoes(t *testing.T) {
	dsn := "clickhouse://worker-login:" + marker + "@host:9000/db"
	err := errors.New("code: 516, message: worker-login: Authentication failed: password is incorrect, or there is no user with such name")
	for name, redacted := range map[string]error{
		"Boundary":          NewBoundary(dsn).Redact(err),
		"WithRedactedCause": WithRedactedCause(errors.New("clickhouse unavailable"), dsn, err),
	} {
		if strings.Contains(redacted.Error(), "worker-login") {
			t.Errorf("%s left the login name in: %v", name, redacted)
		}
		if !strings.Contains(redacted.Error(), "Authentication failed") {
			t.Errorf("%s lost the failure's own text: %v", name, redacted)
		}
	}
}

func TestCredentialComponents_KeywordForm(t *testing.T) {
	dsn := "host=127.0.0.1 password = " + marker + " connect_timeout=nan"
	got := CredentialComponents(dsn)
	found := false
	for _, v := range got {
		if v == marker {
			found = true
		}
	}
	if !found {
		t.Fatalf("CredentialComponents(%q) = %v, want the password %q among them", dsn, got, marker)
	}
}

func TestCredentialComponents_EmptyDSN(t *testing.T) {
	if got := CredentialComponents(""); got != nil {
		t.Fatalf("CredentialComponents(\"\") = %v, want nil", got)
	}
}

// TestBoundary_RedactsTheOptionsParameterAdversarialCase pins the exact
// crafted DSN shape that gets a password past pgx's own userinfo-only
// redaction: the primary userinfo is unremarkable, but the DSN's
// options= query parameter embeds a second `password=` assignment pgx's
// own driver never redacts. The Boundary redacts it anyway because it
// scrubs dsn's OWN full text, not just what pgx chose to keep visible.
func TestBoundary_RedactsTheOptionsParameterAdversarialCase(t *testing.T) {
	dsn := "postgres:xxxxxx@host:bad/db?options=-c%20password%3D" + marker
	err := errors.New("cannot parse `" + dsn + "`: failed to parse as URL (invalid port \":bad\" after host)")

	redacted := NewBoundary(dsn).Redact(err)

	if strings.Contains(redacted.Error(), marker) {
		t.Fatalf("Boundary.Redact left the marker in: %v", redacted)
	}
}

// TestBoundary_RedactsALaterCallErrorAfterAGoodEarlierPing pins the
// residual class an eager Ping alone does not close: a pool that
// connects successfully, then hits a driver error on a LATER call
// (a query, a Begin) that happens to echo the DSN anyway. The Boundary
// is constructed once, from the resolved DSN, independent of which call
// in the chain actually produced the error.
func TestBoundary_RedactsALaterCallErrorAfterAGoodEarlierPing(t *testing.T) {
	dsn := "postgres://user:" + marker + "@host/db"
	// Simulates a fake driver/pool whose Ping succeeds but whose LATER
	// Query/Begin call returns an error that still carries the DSN --
	// exactly the shape a lazy pgxpool.Pool can produce on a connection
	// that drops between a successful Ping and the next real operation.
	queryErr := errors.New("read go_api_routing_state: failed to connect to `" + dsn + "`: server closed the connection")

	redacted := NewBoundary(dsn).Redact(queryErr)

	if strings.Contains(redacted.Error(), marker) {
		t.Fatalf("Boundary.Redact left the marker in: %v", redacted)
	}
	if strings.Contains(redacted.Error(), dsn) {
		t.Fatalf("Boundary.Redact left the DSN in: %v", redacted)
	}
}

func TestBoundary_NilErrorStaysNil(t *testing.T) {
	if got := NewBoundary("postgres://user:" + marker + "@host/db").Redact(nil); got != nil {
		t.Fatalf("Redact(nil) = %v, want nil", got)
	}
}

func TestBoundary_EmptyDSNIsANoOp(t *testing.T) {
	original := errors.New("dial tcp: connection refused")
	if got := NewBoundary("").Redact(original); got != original {
		t.Fatalf("Redact with an empty DSN should return err unchanged, got %v", got)
	}
}

func TestBoundary_UnrelatedErrorTextIsUnchanged(t *testing.T) {
	original := errors.New("no such file or directory")
	dsn := "postgres://user:" + marker + "@host/db"
	if got := NewBoundary(dsn).Redact(original); got.Error() != original.Error() {
		t.Fatalf("Redact of unrelated text changed it: %v", got)
	}
}
