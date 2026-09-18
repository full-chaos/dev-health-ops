package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// postgresURIAdversarialDSN embeds the marker a SECOND time inside the
// options= query parameter, past pgx's own userinfo-only redaction --
// the shape an executed reproduction used to reach ReadRoutingState's
// connect failure with the marker still visible in the raw driver error.
const postgresURIAdversarialDSN = "postgres:xxxxxx@host:bad/db?options=-c%20password%3D" + postgresURIMarker

// postgresURIMarker is a recognisable, obviously-fake credential-shaped value:
// present in test output only if a redaction guard fails to hold.
const postgresURIMarker = "MARKER-PW-7f3a9c"

func TestRegisterFlags_DSNUsageNeverPrintsTheEnvValue(t *testing.T) {
	t.Setenv(postgresURIEnvVar, "postgres://user:"+postgresURIMarker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var out bytes.Buffer
	set.SetOutput(&out)
	registerFlags(set)

	set.Usage()

	if strings.Contains(out.String(), postgresURIMarker) {
		t.Fatalf("usage output leaked the %s value: %s", postgresURIEnvVar, out.String())
	}
	if !strings.Contains(out.String(), postgresURIEnvVar) {
		t.Fatalf("usage output should name %s: %s", postgresURIEnvVar, out.String())
	}
}

func TestRegisterFlags_DSNParseErrorNeverPrintsTheEnvValue(t *testing.T) {
	t.Setenv(postgresURIEnvVar, "postgres://user:"+postgresURIMarker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var out bytes.Buffer
	set.SetOutput(&out)
	registerFlags(set)

	err := set.Parse([]string{"-not-a-real-flag"})
	if err == nil {
		t.Fatal("want an error for an undefined flag")
	}
	if strings.Contains(out.String(), postgresURIMarker) {
		t.Fatalf("parse-error output leaked the %s value: %s", postgresURIEnvVar, out.String())
	}
	if !strings.Contains(out.String(), postgresURIEnvVar) {
		t.Fatalf("parse-error output should name %s: %s", postgresURIEnvVar, out.String())
	}
}

func TestRegisterFlags_DSNFallsBackToTheEnvValue(t *testing.T) {
	marker := "postgres://user:" + postgresURIMarker + "@host/db"
	t.Setenv(postgresURIEnvVar, marker)

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerFlags(set)
	if err := set.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *f.dsn != "" {
		t.Fatalf("dsn before resolve = %q, want empty (never the env value as a default)", *f.dsn)
	}

	secrets.ResolveFlag(set, f.dsn, "dsn", postgresURIEnvVar)

	if *f.dsn != marker {
		t.Fatalf("dsn after resolve = %q, want the env value %q", *f.dsn, marker)
	}
}

// TestRegisterFlags_DSNExplicitEmptyWinsOverTheEnvValue: an explicit
// -dsn= (empty) must stay empty, never silently pick up POSTGRES_URI.
func TestRegisterFlags_DSNExplicitEmptyWinsOverTheEnvValue(t *testing.T) {
	t.Setenv(postgresURIEnvVar, "postgres://user:"+postgresURIMarker+"@host/db")

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerFlags(set)
	if err := set.Parse([]string{"-dsn="}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	secrets.ResolveFlag(set, f.dsn, "dsn", postgresURIEnvVar)

	if *f.dsn != "" {
		t.Fatalf("dsn = %q, want empty (an explicit -dsn= must not fall back to %s)", *f.dsn, postgresURIEnvVar)
	}
}

// TestRunRender_BoundaryRedactsAConnectErrorCarryingTheMarker pins the
// executed reproduction that motivated this class: -dsn set to a DSN
// crafted so pgx's parse failure echoes it back with the marker still
// inside (past pgx's own userinfo-only redaction), composed the same way
// main() itself composes it -- secrets.NewBoundary(the resolved
// DSN).Redact(the returned error) -- against the REAL runRender function.
func TestRunRender_BoundaryRedactsAConnectErrorCarryingTheMarker(t *testing.T) {
	root := repoRoot(t)

	err := runRender(root, postgresURIAdversarialDSN, "", "none", nil)
	if err == nil {
		t.Fatal("want a connect error against the adversarial DSN")
	}

	redacted := secrets.NewBoundary(postgresURIAdversarialDSN).Redact(err)

	if strings.Contains(redacted.Error(), postgresURIMarker) {
		t.Fatalf("the boundary left the marker in: %v", redacted)
	}
}
