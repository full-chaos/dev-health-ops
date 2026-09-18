package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// postgresURIMarker is a recognisable, obviously-fake credential-shaped value:
// present in test output only if a redaction guard fails to hold.
const postgresURIMarker = "MARKER-PW-7f3a9c"

func TestParseFlags_PostgresURIUsageNeverPrintsTheEnvValue(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgres://user:"+postgresURIMarker+"@host/db")
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	var out bytes.Buffer
	flag.CommandLine.SetOutput(&out)
	restoreArgs := setOSArgs(t, []string{"go-api-rest-prove", "-h"})
	defer restoreArgs()

	if _, err := parseFlags(); err == nil {
		t.Fatal("want an error from -h")
	}
	if strings.Contains(out.String(), postgresURIMarker) {
		t.Fatalf("usage output leaked the POSTGRES_URI value: %s", out.String())
	}
	if !strings.Contains(out.String(), "POSTGRES_URI") {
		t.Fatalf("usage output should name POSTGRES_URI: %s", out.String())
	}
}

func TestParseFlags_ParseErrorNeverPrintsThePostgresURIEnvValue(t *testing.T) {
	t.Setenv("POSTGRES_URI", "postgres://user:"+postgresURIMarker+"@host/db")
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	var out bytes.Buffer
	flag.CommandLine.SetOutput(&out)
	restoreArgs := setOSArgs(t, []string{"go-api-rest-prove", "-not-a-real-flag"})
	defer restoreArgs()

	if _, err := parseFlags(); err == nil {
		t.Fatal("want an error for an undefined flag")
	}
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
	resetFlagsForTest(t)
	restoreArgs := setOSArgs(t, []string{
		"go-api-rest-prove",
		"-python-api-url", "http://api:8000",
		"-candidate-bearer-exec", `["/bin/true"]`,
		"-baseline-bearer-exec", `["/bin/true"]`,
		"-org", "org-1",
		"-recorded-by", "chris",
		"-review-evidence", "test",
		"-artifact-dir", t.TempDir(),
	})
	defer restoreArgs()

	f, err := parseFlags()
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
	resetFlagsForTest(t)
	restoreArgs := setOSArgs(t, []string{
		"go-api-rest-prove",
		"-postgres-uri=",
		"-python-api-url", "http://api:8000",
		"-candidate-bearer-exec", `["/bin/true"]`,
		"-baseline-bearer-exec", `["/bin/true"]`,
		"-org", "org-1",
		"-recorded-by", "chris",
		"-review-evidence", "test",
		"-artifact-dir", t.TempDir(),
		"-dry-run",
	})
	defer restoreArgs()

	f, err := parseFlags()
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if f.postgresURI != "" {
		t.Fatalf("postgresURI = %q, want empty (an explicit -postgres-uri= must not fall back to POSTGRES_URI)", f.postgresURI)
	}
}

// TestNewPGXPool_DetectsAnUnreachableHostEagerly: pgxpool.New never dials
// -- the connection is only attempted on first real use, so a caller that
// only checked pgxpool.New's own (near-always nil) error would sail past
// an unreachable/misconfigured DSN and only discover it later, inside
// WriteReceipt's own goapiproof.WriteRESTAtomic (Begin, then the insert)
// -- surfacing pgx's OWN connection error (which can carry the DSN)
// through THAT call's unredacted %w wrap instead of the one newPGXPool
// already redacts. Ping forces the dial here, so the failure is caught at
// THIS return, already behind secrets.RedactedConnectError.
func TestNewPGXPool_DetectsAnUnreachableHostEagerly(t *testing.T) {
	// 192.0.2.0/24 is TEST-NET-1 (RFC 5737): reserved for documentation,
	// never routable, so the connection attempt fails fast and
	// deterministically without depending on any real network or DNS
	// resolver behaving a particular way.
	dsn := "postgres://u:" + postgresURIMarker + "@192.0.2.1:1/db?connect_timeout=1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := newPGXPool(ctx, dsn)
	if err == nil {
		t.Fatal("want a connection error against an unreachable host, returned from newPGXPool itself")
	}
	if strings.Contains(err.Error(), postgresURIMarker) {
		t.Fatalf("newPGXPool error leaked the DSN: %v", err)
	}
}

// failingTxBeginner satisfies goapiproof.Querier + goapiproof.TxBeginner:
// its Begin fails with a driver error carrying the DSN, standing in for
// a pool that opened and Pinged successfully but whose FIRST real
// transaction -- exactly WriteRESTAtomic's own Begin, reached from
// WriteReceipt -- discovers the DSN is unreachable. This is the class an
// eager Ping at pool-open time does not close by itself: a connection
// that drops, or a driver that only discovers the DSN is bad on the
// first real transaction, after Ping already succeeded. Exec/Query/
// QueryRow are never reached on this path and panic if they ever are.
type failingTxBeginner struct{ beginErr error }

func (failingTxBeginner) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	panic("not reached: Begin fails first")
}
func (failingTxBeginner) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("not reached: Begin fails first")
}
func (failingTxBeginner) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("not reached: Begin fails first")
}
func (f failingTxBeginner) Begin(context.Context) (pgx.Tx, error) { return nil, f.beginErr }

// TestWriteRESTAtomic_BoundaryRedactsABeginErrorEvenAfterAGoodOpen pins
// the exact site an executed reproduction found: goapiproof.
// WriteRESTAtomic's own Begin error, reached through WriteReceipt, is
// what actually discovers a DSN that only fails on first real use --
// composed the same way run(f) itself composes it, secrets.
// NewBoundary(the resolved DSN).Redact(the returned error), against the
// REAL WriteRESTAtomic function.
func TestWriteRESTAtomic_BoundaryRedactsABeginErrorEvenAfterAGoodOpen(t *testing.T) {
	dsn := "postgres://user:" + postgresURIMarker + "@host/db"
	db := failingTxBeginner{beginErr: errors.New("goapiproof: begin REST receipt transaction: failed to connect to `" + dsn + "`: server closed the connection")}

	_, err := goapiproof.WriteRESTAtomic(context.Background(), db, goapiproof.RESTReceipt{})
	if err == nil {
		t.Fatal("want a non-nil error from a failing Begin")
	}

	redacted := secrets.NewBoundary(dsn).Redact(err)

	if strings.Contains(redacted.Error(), postgresURIMarker) {
		t.Fatalf("the boundary left the marker in a later-call error: %v", redacted)
	}
	if strings.Contains(redacted.Error(), dsn) {
		t.Fatalf("the boundary left the DSN in a later-call error: %v", redacted)
	}
}
