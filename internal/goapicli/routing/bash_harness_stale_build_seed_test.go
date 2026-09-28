//go:build integration

package routing

// Postgres holder for tests/tooling/test_bigboy_cut_carry_refusal_text_behavior.py's
// stale-build case: starts a real testcontainers Postgres, seeds one live row with a
// candidate_build the fixture /buildinfo server will disagree with, writes the DSN to a
// file, then blocks until the Python test signals it's done (a sentinel file) so the
// container stays up for the REAL dho binary Python invokes externally.
//
// Reuses this package's own schema/seed helpers (seedLiveRow, startVerbPostgres) rather
// than hand-duplicating DDL in Python -- the whole point of running this AS a Go test.

import (
	"os"
	"testing"
	"time"
)

func TestSeedStaleRowForBashHarness(t *testing.T) {
	// This test exists ONLY to be launched externally by
	// test_bigboy_cut_carry_refusal_text_behavior.py, which sets both env vars below --
	// never as part of an ordinary `go test ./...` sweep. Skip (not fail) when they are
	// absent, or this test fails every general CI run that reaches this package.
	dsnFile := os.Getenv("SCRATCH_DSN_FILE")
	sentinel := os.Getenv("SCRATCH_SENTINEL")
	if dsnFile == "" || sentinel == "" {
		t.Skip("SCRATCH_DSN_FILE and SCRATCH_SENTINEL are unset -- this test is a Postgres " +
			"holder for test_bigboy_cut_carry_refusal_text_behavior.py, not a general-suite test")
	}

	_, dsn := startVerbPostgres(t)
	digest := carryTestDocumentDigest()
	seedLiveRow(t, dsn, digest, "canary")

	// t.Log/t.Logf output is buffered per-test and only flushed when the test itself
	// completes -- useless for signaling a DSN out to a process that needs it WHILE this
	// one is still blocking below. A file the Python side polls for is the only channel
	// that actually crosses that gap.
	if err := os.WriteFile(dsnFile, []byte(dsn), 0o600); err != nil {
		t.Fatalf("write dsn file: %v", err)
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sentinel); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the Python harness to signal completion")
}
