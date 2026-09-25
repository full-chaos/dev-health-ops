package server

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestWriteGrantsCheckIsOptIn(t *testing.T) {
	t.Parallel()
	for name, value := range map[string]string{"unset": "", "blank": "   \t"} {
		env := func(key string) string {
			if key == "QUERY_API_DATABASE_ROLE" {
				return value
			}
			return ""
		}
		if check := writeGrantsCheck(env, nil); check != nil {
			t.Errorf("%s: a deployment that names no role must not be checked", name)
		}
	}
	named := func(key string) string {
		if key == "QUERY_API_DATABASE_ROLE" {
			return " dev_health_query_api "
		}
		return ""
	}
	check := writeGrantsCheck(named, nil)
	if check == nil {
		t.Fatal("a deployment that names a role must be checked")
	}
	// With no pool the check cannot pass: naming a role and having no way to
	// verify it is not ready.
	if err := check(context.Background()); err == nil {
		t.Fatal("the check passed with no Postgres pool")
	}
}

func TestWriteGrantsReadinessClassesAFailureAndPassesOtherwise(t *testing.T) {
	t.Parallel()
	if err := writeGrantsReadiness(context.Background(), nil); err != nil {
		t.Fatalf("no check configured: %v", err)
	}
	if err := writeGrantsReadiness(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("a passing check: %v", err)
	}
	cause := errors.New("saved_reports: missing [INSERT] for role \"r\" at dial tcp 10.0.0.1:5432")
	err := writeGrantsReadiness(context.Background(), func(context.Context) error { return cause })
	var dependency *readyzDependencyError
	if !errors.As(err, &dependency) || dependency.Class != readyzClassWriteGrants || !errors.Is(err, cause) {
		t.Fatalf("a failing check must be a %q dependency error wrapping its cause: %v", readyzClassWriteGrants, err)
	}
	if got := readyzDependencyClass(err); got != readyzClassWriteGrants {
		t.Fatalf("/readyz would report class %q, want %q", got, readyzClassWriteGrants)
	}
}

// buildQueryRoute needs a live ClickHouse to run, so the wiring of the check
// into /readyz is pinned at the call site: the readiness func it hands back
// must be built WITH the write-grants check, or naming QUERY_API_DATABASE_ROLE
// would gate nothing.
func TestBuildQueryRouteWiresTheWriteGrantsCheckIntoReadiness(t *testing.T) {
	t.Parallel()
	src, err := os.ReadFile("query_route.go")
	if err != nil {
		t.Fatal(err)
	}
	const call = "readinessCheck(chClient, pgPool, verifier, writeGrantsCheck(getenv, pgPool))"
	if strings.Count(string(src), call) != 1 {
		t.Fatalf("query_route.go must build its readiness check exactly once, with %q", call)
	}
}
