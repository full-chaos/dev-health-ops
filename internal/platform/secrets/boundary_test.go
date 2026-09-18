package secrets

import (
	"errors"
	"strings"
	"testing"
)

func TestCredentialComponents_URLForm(t *testing.T) {
	dsn := "postgres://user:" + marker + "@host/db"
	got := CredentialComponents(dsn)
	if len(got) != 2 || got[0] != dsn || got[1] != marker {
		t.Fatalf("CredentialComponents(%q) = %v, want [dsn, password]", dsn, got)
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
