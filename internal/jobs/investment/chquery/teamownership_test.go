package chquery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTeamRepoDonorReadFailureIsNotMissingOwnership(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatal(err)
	}
	if donors, err := reader.FetchTeamRepoDonors(context.Background(), []string{"issue"}, "org", time.Now()); err == nil || donors != nil {
		t.Fatalf("read failure became success: donors=%v err=%v", donors, err)
	}
	fake.query = ""
	if donors, err := reader.FetchTeamRepoDonors(context.Background(), nil, "org", time.Now()); err != nil || len(donors) != 0 || fake.query != "" {
		t.Fatalf("empty request must not query: donors=%v err=%v query=%s", donors, err, fake.query)
	}
	for _, org := range []string{"", " "} {
		fake.query = ""
		if _, err := reader.FetchTeamRepoDonors(context.Background(), []string{"issue"}, org, time.Now()); err == nil {
			t.Fatalf("unscoped query accepted for org %q", org)
		}
		// The fake conn errors on every call, so "an error came back" alone
		// cannot tell a refusal apart from an unscoped query that happened to
		// fail. Only the untouched query proves the refusal came first.
		if fake.query != "" {
			t.Fatalf("org %q reached ClickHouse unscoped: %s", org, fake.query)
		}
	}
}

// TestTeamRepoDonorPreconditionsAreRefusedNotDefaulted pins the remaining
// preconditions one clause at a time. Each of these would otherwise degrade
// silently: a nil reader would panic in production, and a zero as-of would
// make every ownership interval compare against the zero time and return
// nothing -- indistinguishable from "this org has no ownership".
func TestTeamRepoDonorPreconditionsAreRefusedNotDefaulted(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatal(err)
	}
	var nilReader *Reader
	if _, err := nilReader.FetchTeamRepoDonors(context.Background(), []string{"issue"}, "org", time.Now()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("nil reader err=%v, want ErrUnavailable", err)
	}
	fake.query = ""
	if _, err := reader.FetchTeamRepoDonors(context.Background(), []string{"issue"}, "org", time.Time{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("zero as-of err=%v, want ErrUnavailable", err)
	}
	if fake.query != "" {
		t.Fatalf("zero as-of still issued a query: %s", fake.query)
	}
}
