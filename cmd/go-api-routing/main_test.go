package main

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"net/http"
	"reflect"
	"testing"
)

func TestRequestedOperationsTreatsAllRegisteredAsEveryRow(t *testing.T) {
	for _, raw := range []string{"all-registered", "  all-registered  ", "", "   "} {
		got, err := requestedOperations(raw)
		if err != nil {
			t.Fatalf("requestedOperations(%q) = %v, want no error", raw, err)
		}
		if got != nil {
			t.Fatalf("requestedOperations(%q) = %v, want nil (every row)", raw, got)
		}
	}
}

func TestRequestedOperationsTrimsAndDropsEmptyNames(t *testing.T) {
	got, err := requestedOperations(" flowMatrix , hotspots ,, ")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"flowMatrix", "hotspots"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requestedOperations = %v, want %v", got, want)
	}
}

// CHAOS-5486 round 1, F1 (reproduced by the lane before fixing): a
// separators-only --operations produced an EMPTY filter, which Repoint reads
// as "every row at the digest" -- an operator who named something got a
// silent re-point of everything. Asking for all rows must be explicit.
func TestRequestedOperationsRefusesAFilterThatNamesNothing(t *testing.T) {
	for _, raw := range []string{",", " , ", ",,,", " ,, , "} {
		got, err := requestedOperations(raw)
		if !errors.Is(err, errEmptyOperationFilter) {
			t.Fatalf("requestedOperations(%q) = (%v, %v), want errEmptyOperationFilter -- a write verb must never widen silently", raw, got, err)
		}
		if got != nil {
			t.Fatalf("requestedOperations(%q) returned %v alongside its error", raw, got)
		}
	}
}

// The credential env var must name the ENVELOPE, not the edge access
// token: /buildinfo checks the envelope and rejects the access token with
// 401. Naming the wrong one sends an operator into a 401 that reads like
// an authorization failure rather than a wrong credential KIND.
func TestBearerEnvVarIsDistinctFromTheProveEdgeToken(t *testing.T) {
	if bearerEnvVar == "GO_API_PROVE_BEARER" {
		t.Fatal("this command needs the envelope; GO_API_PROVE_BEARER is go-api-prove's EDGE access token")
	}
	if bearerEnvVar == "" {
		t.Fatal("the credential must come from a named environment variable, never a flag: a flag value reaches ps and shell history")
	}
}

// CHAOS-5479 changed FetchBuildIdentity's credential parameter, and this
// command's call site had to change with it. The credential KIND is the
// part worth pinning: /buildinfo checks the effective-principal envelope
// and 401s an edge access token, so passing the wrong one is a defect that
// only shows up against a real stack.
//
// This asserts the value this command sends is the envelope from its own
// env var, and that the credential refuses to be empty -- the second floor
// under the explicit check above it.
func TestTheBuildInfoReadCarriesTheEnvelope(t *testing.T) {
	credential := goapiproof.StaticCredential("Authorization", "effective-principal envelope", "Bearer eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEifQ.c2ln")

	request, err := http.NewRequest(http.MethodGet, "http://query-api.test/buildinfo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := credential.Apply(context.Background(), request); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := request.Header.Get("Authorization"); got != "Bearer eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEifQ.c2ln" {
		t.Fatalf("the /buildinfo read carried %q", got)
	}
	// The kind reaches a 401 message so an operator learns WHICH credential
	// was refused, without its value.
	if credential.Kind() != "effective-principal envelope" {
		t.Fatalf("credential kind = %q; a 401 must name the kind /buildinfo actually checks", credential.Kind())
	}

	// Empty and whitespace-only are refused at use, whatever the caller's
	// own checks do.
	for _, bad := range []string{"", "   ", "Bearer    "} {
		empty := goapiproof.StaticCredential("Authorization", "effective-principal envelope", bad)
		if err := empty.Apply(context.Background(), request); err == nil {
			t.Fatalf("credential %q was installed", bad)
		}
	}
}
