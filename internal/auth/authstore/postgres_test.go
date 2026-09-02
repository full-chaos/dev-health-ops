package authstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// unreachable is a syntactically valid DSN whose port is reserved and unbound,
// so a dial is refused immediately rather than hanging. No container is
// needed: every assertion below is about how this package CLASSIFIES a
// failure, not about a live database.
const unreachable = "postgres://auth:hunter2@127.0.0.1:1/devhealth"

func TestOpenRejectsAMissingSchema(t *testing.T) {
	_, err := Open(context.Background(), Config{URI: unreachable})
	var storage *Error
	if !errors.As(err, &storage) || storage.Reason != ReasonInvalidConfig {
		t.Fatalf("Open = %v, want a %q rejection", err, ReasonInvalidConfig)
	}
}

func TestOpenRejectsAMalformedDSN(t *testing.T) {
	_, err := Open(context.Background(), Config{URI: "not-a-dsn", Schema: "auth"})
	var storage *Error
	if !errors.As(err, &storage) || storage.Reason != ReasonInvalidConfig {
		t.Fatalf("Open = %v, want a %q rejection", err, ReasonInvalidConfig)
	}
}

// TestOpenPerformsNoNetworkIO is the contract the executed readiness proof
// depends on: an unreachable database must not prevent the process from
// starting, or /readyz has nothing to report and an orchestrator sees a
// crash-loop instead of an unready replica.
func TestOpenPerformsNoNetworkIO(t *testing.T) {
	store, err := Open(context.Background(), Config{
		URI: unreachable, Schema: "auth", MaxConns: 2, ConnectTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Open on an unreachable DSN failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Shutdown(context.Background()) })

	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("Start performed I/O and failed: %v", err)
	}
}

func TestProbeClassifiesAnUnreachableDatabase(t *testing.T) {
	store, err := Open(context.Background(), Config{
		URI: unreachable, Schema: "auth", MaxConns: 2, ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Shutdown(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probeErr := store.Probe(ctx)

	var storage *Error
	if !errors.As(probeErr, &storage) || storage.Reason != ReasonUnreachable {
		t.Fatalf("Probe = %v, want a %q failure", probeErr, ReasonUnreachable)
	}
	// The leak control: pgx dial errors render host:port and can render the
	// user name, and this error is logged. Error() must carry the bounded
	// reason and nothing else.
	if strings.Contains(probeErr.Error(), "hunter2") ||
		strings.Contains(probeErr.Error(), "127.0.0.1") ||
		strings.Contains(probeErr.Error(), "devhealth") {
		t.Fatalf("the error text leaked the DSN: %q", probeErr)
	}
	// The cause is withheld from the message, not discarded.
	if errors.Unwrap(probeErr) == nil {
		t.Fatal("the underlying driver error was discarded rather than withheld")
	}
}

func TestReasonOfFallsBackToAGenericLabel(t *testing.T) {
	if got := ReasonOf(errors.New("dial tcp 10.0.0.5:5432: connect: refused")); got != "storage_failed" {
		t.Fatalf("ReasonOf on a foreign error = %q, want the generic label", got)
	}
	if got := ReasonOf(failure(ReasonSchemaMissing, nil)); got != string(ReasonSchemaMissing) {
		t.Fatalf("ReasonOf = %q, want %q", got, ReasonSchemaMissing)
	}
}

func TestShutdownIsSafeOnAnUnconstructedStore(t *testing.T) {
	var store *Postgres
	if err := store.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown on a nil store: %v", err)
	}
	if err := store.Probe(context.Background()); err == nil {
		t.Fatal("Probe on a nil store reported healthy")
	}
}

// TestComponentNameIsStable pins the name lifecycle.Runtime uses for
// uniqueness and for the shutdown log line an operator reads.
func TestComponentNameIsStable(t *testing.T) {
	if got := (&Postgres{}).Name(); got != "auth-postgres" {
		t.Fatalf("Name = %q, want auth-postgres", got)
	}
}
