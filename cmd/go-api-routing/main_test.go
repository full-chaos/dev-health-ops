package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
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
// command's call site had to change with it.
//
// r11 S3: this test used to BUILD its own StaticCredential with the same
// three arguments the production line uses, which proved only that the
// test agrees with itself -- every wrong-argument mutation at the real
// call site survived it. It now calls buildInfoCredential, the one-line
// constructor `runCommand` uses, so each of the three independently
// wrong-able arguments has a killer:
//
//   - the header NAME: /buildinfo reads Authorization; anything else
//     arrives unauthenticated and 401s for the wrong reason.
//   - the `Bearer ` scheme prefix: without it the value is not a bearer
//     credential at all.
//   - the `kind` string: /buildinfo checks the effective-principal
//     envelope and 401s an edge access token, so this is the word that
//     tells an operator WHICH credential was refused. Passing the wrong
//     one is a defect that only shows up against a real stack.
func TestTheBuildInfoReadCarriesTheEnvelope(t *testing.T) {
	bearer := syntheticJWT(t, map[string]string{"sub": "u-1"})
	credential := buildInfoCredential(bearer)

	request, err := http.NewRequest(http.MethodGet, "http://query-api.test/buildinfo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := credential.Apply(context.Background(), request); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The header NAME, and the `Bearer ` prefix, exactly as /buildinfo
	// expects them. Asserting the whole value covers both at once.
	if got := request.Header.Get("Authorization"); got != "Bearer "+bearer {
		t.Fatalf("the /buildinfo read carried Authorization=%q, want %q", got, "Bearer "+bearer)
	}
	// ...and no OTHER header carries it, which is what a wrong header name
	// would look like.
	for name, values := range request.Header {
		if name == "Authorization" {
			continue
		}
		for _, value := range values {
			if strings.Contains(value, bearer) {
				t.Fatalf("the credential was installed as %s: /buildinfo reads Authorization and would see this request as unauthenticated", name)
			}
		}
	}

	// The kind reaches a 401 message so an operator learns WHICH credential
	// was refused, without its value.
	if credential.Kind() != "effective-principal envelope" {
		t.Fatalf("credential kind = %q; a 401 must name the kind /buildinfo actually checks", credential.Kind())
	}
	// Specifically NOT the edge access token: that is the wrong-credential
	// swap this pin exists for, and it 401s against a real stack.
	if strings.Contains(strings.ToLower(credential.Kind()), "access token") {
		t.Fatalf("credential kind = %q -- /buildinfo checks the envelope and 401s an edge access token", credential.Kind())
	}

	// Empty and whitespace-only are refused at use, whatever the caller's
	// own checks do -- the second floor under runCommand's explicit check.
	for _, bad := range []string{"", "   "} {
		if err := buildInfoCredential(bad).Apply(context.Background(), request); err == nil {
			t.Fatalf("buildInfoCredential(%q) was installed", bad)
		}
	}
}

// syntheticJWT builds a JWT-SHAPED value at RUNTIME, so no `eyJ...`
// literal appears anywhere in the tree. Gitleaks' `jwt` rule matches on
// SHAPE, not on whether the value is real, so a synthetic fixture written
// as a literal fails the secret scan exactly like a leaked one -- and the
// answer is to stop writing the shape into the source, not to teach the
// scanner to skip a file.
func syntheticJWT(t *testing.T, claims map[string]string) string {
	t.Helper()
	segment := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal a JWT segment: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return strings.Join([]string{
		segment(map[string]string{"alg": "EdDSA"}),
		segment(claims),
		base64.RawURLEncoding.EncodeToString([]byte("synthetic-signature")),
	}, ".")
}
