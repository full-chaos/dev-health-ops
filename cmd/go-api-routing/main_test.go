package main

import (
	"reflect"
	"testing"
)

func TestRequestedOperationsTreatsAllRegisteredAsEveryRow(t *testing.T) {
	for _, raw := range []string{"all-registered", "", "   "} {
		if got := requestedOperations(raw); got != nil {
			t.Fatalf("requestedOperations(%q) = %v, want nil (every row)", raw, got)
		}
	}
}

func TestRequestedOperationsTrimsAndDropsEmptyNames(t *testing.T) {
	got := requestedOperations(" flowMatrix , hotspots ,, ")
	want := []string{"flowMatrix", "hotspots"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("requestedOperations = %v, want %v", got, want)
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
