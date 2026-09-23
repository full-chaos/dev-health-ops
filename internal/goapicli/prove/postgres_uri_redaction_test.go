package prove

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// postgresURIMarker is a recognisable, obviously-fake credential-shaped value:
// present in test output only if a redaction guard fails to hold.
const postgresURIMarker = "MARKER-PW-7f3a9c"

// captureFlagOutput points parseFlags' own flag set's usage/error output at
// a buffer instead of stderr, for the duration of one test. Restored on
// cleanup.
func captureFlagOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	original := flagOutput
	var out bytes.Buffer
	flagOutput = &out
	t.Cleanup(func() { flagOutput = original })
	return &out
}

func TestParseFlags_PostgresURIUsageNeverPrintsTheEnvValue(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgres://user:"+postgresURIMarker+"@host/db")
	out := captureFlagOutput(t)

	_, _ = parseFlags([]string{"-h"})

	if strings.Contains(out.String(), postgresURIMarker) {
		t.Fatalf("usage output leaked the POSTGRES_URI value: %s", out.String())
	}
	if !strings.Contains(out.String(), "POSTGRES_URI") {
		t.Fatalf("usage output should name POSTGRES_URI: %s", out.String())
	}
}

func TestParseFlags_ParseErrorNeverPrintsThePostgresURIEnvValue(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgres://user:"+postgresURIMarker+"@host/db")
	out := captureFlagOutput(t)

	_, _ = parseFlags([]string{"-not-a-real-flag"})

	if strings.Contains(out.String(), postgresURIMarker) {
		t.Fatalf("parse-error output leaked the POSTGRES_URI value: %s", out.String())
	}
	if !strings.Contains(out.String(), "POSTGRES_URI") {
		t.Fatalf("parse-error output should name POSTGRES_URI: %s", out.String())
	}
}

func TestParseFlags_PostgresURIFallsBackToTheEnvValue(t *testing.T) {
	marker := "postgres://user:" + postgresURIMarker + "@host/db"
	t.Setenv("POSTGRES_URI", marker)
	captureFlagOutput(t)

	f, err := parseFlags([]string{
		"-documents", "does-not-need-to-exist.json",
		"-org", "org-1",
		"-artifact-dir", t.TempDir(),
		"-recorded-by", "chris",
		"-review-evidence", "test",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.postgresURI != marker {
		t.Fatalf("postgresURI = %q, want the env value %q", f.postgresURI, marker)
	}
}

// TestParseFlags_PostgresURIExplicitEmptyWinsOverTheEnvValue: an explicit
// -postgres-uri= (empty) must stay empty, never silently pick up
// POSTGRES_URI.
func TestParseFlags_PostgresURIExplicitEmptyWinsOverTheEnvValue(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgres://user:"+postgresURIMarker+"@host/db")
	captureFlagOutput(t)

	f, err := parseFlags([]string{
		"-postgres-uri=",
		"-documents", "does-not-need-to-exist.json",
		"-org", "org-1",
		"-artifact-dir", t.TempDir(),
		"-recorded-by", "chris",
		"-review-evidence", "test",
		"-dry-run",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.postgresURI != "" {
		t.Fatalf("postgresURI = %q, want empty (an explicit -postgres-uri= must not fall back to POSTGRES_URI)", f.postgresURI)
	}
}

// TestOpenPostgresPool_DetectsAnUnreachableHostEagerly: pgxpool.New never
// dials -- the connection is only attempted on first real use, so a caller
// that only checked pgxpool.New's own (near-always nil) error would sail
// past an unreachable/misconfigured DSN and only discover it later, inside
// whatever call (readRoutingState's own pool.Query, a Begin) happens to be
// the first real operation -- surfacing pgx's OWN connection error (which
// can carry the DSN) through THAT call's unredacted %w wrap instead of the
// one call() already redacts right after openPostgresPool. Ping forces the
// dial here, so the failure is caught at THIS return, while the caller is
// still one `if err != nil` away from secrets.RedactedConnectError.
func TestOpenPostgresPool_DetectsAnUnreachableHostEagerly(t *testing.T) {
	// 192.0.2.0/24 is TEST-NET-1 (RFC 5737): reserved for documentation,
	// never routable, so the connection attempt fails fast and
	// deterministically without depending on any real network or DNS
	// resolver behaving a particular way.
	dsn := "postgres://u:" + postgresURIMarker + "@192.0.2.1:1/db?connect_timeout=1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := openPostgresPool(ctx, dsn); err == nil {
		t.Fatal("want a connection error against an unreachable host, returned from openPostgresPool itself")
	}
}
