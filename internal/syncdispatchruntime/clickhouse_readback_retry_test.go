package syncdispatchruntime

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// queryErrConn fails every Query with one fixed error; it streams nothing.
type queryErrConn struct {
	driver.Conn
	err error
}

func (conn queryErrConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, conn.err
}

func transientClickHouseError() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

func TestClickHouseReadbackKeepsATransientCauseRetryable(t *testing.T) {
	verifier, err := NewClickHouseReadbackVerifier(queryErrConn{err: transientClickHouseError()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, teamErr := verifier.MissingTeamKeys(ctx, "org", "jira", []string{"T1"})
	_, sprintErr := verifier.MissingSprintIDs(ctx, "org", "jira", []string{"S1"})
	for name, got := range map[string]error{"team keys": teamErr, "sprint ids": sprintErr} {
		if !errors.Is(got, ErrReferenceDiscoveryUnavailable) {
			t.Fatalf("%s: the readback failure lost ErrReferenceDiscoveryUnavailable: %v", name, got)
		}
		var opErr *net.OpError
		if !errors.As(got, &opErr) {
			t.Fatalf("%s: the ClickHouse cause is cut out of the chain: %v", name, got)
		}
		if !isRetryableDiscoveryError(got) {
			t.Fatalf("%s: a refused ClickHouse connection during readback must retry, not terminalize: %v", name, got)
		}
	}

	readback, err := NewReferenceReadbackVerifier(verifier)
	if err != nil {
		t.Fatal(err)
	}
	verifyErr := readback.Verify(ctx, "org", "jira", map[string]any{"reference_team_keys": []any{"T1"}})
	if !isRetryableDiscoveryError(verifyErr) {
		t.Fatalf("Verify must hand the classifier a retryable error for a ClickHouse blip: %v", verifyErr)
	}
}

func TestClickHouseReadbackKeepsAPermanentCauseNonRetryable(t *testing.T) {
	verifier, err := NewClickHouseReadbackVerifier(queryErrConn{err: errors.New("code: 62, syntax error")})
	if err != nil {
		t.Fatal(err)
	}
	_, got := verifier.MissingTeamKeys(context.Background(), "org", "jira", []string{"T1"})
	if !errors.Is(got, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("the readback failure lost ErrReferenceDiscoveryUnavailable: %v", got)
	}
	if isRetryableDiscoveryError(got) {
		t.Fatalf("a permanent ClickHouse error must not retry: %v", got)
	}
	if isRetryableDiscoveryError(ErrReferenceDiscoveryUnavailable) {
		t.Fatal("a missing verifier stays a permanent failure")
	}
}
