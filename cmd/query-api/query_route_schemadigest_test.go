package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/digest"
	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
)

// TestVerifySchemaDigest_Match is CHAOS-4696 PR2's happy path: a
// GO_API_SCHEMA_DIGEST that was actually produced by
// `registrydump -schema-digest` (i.e. digest.Schema(schemav1.SDL) itself)
// must verify cleanly.
func TestVerifySchemaDigest_Match(t *testing.T) {
	correct := digest.Schema(schemav1.SDL)
	if err := verifySchemaDigest(correct); err != nil {
		t.Fatalf("verifySchemaDigest(%q) = %v, want nil", correct, err)
	}
}

// TestVerifySchemaDigest_Mismatch is the ruling's core proof: a wrong
// value (stale pin, hand-typed placeholder, or a test harness's
// throwaway "sha256:lane-...-schema-digest" reaching a real environment
// -- query_route.go's own comment names this exact scenario) must fail
// LOUDLY and CLOSED, not silently.
func TestVerifySchemaDigest_Mismatch(t *testing.T) {
	cases := []string{
		"",
		"sha256:lane-go-api-livelocal-schema-digest",
		"sha256:0000000000000000000000000000000000000000000000000000000000000",
		digest.Schema(schemav1.SDL) + "-stale",
	}
	for _, configured := range cases {
		t.Run(configured, func(t *testing.T) {
			err := verifySchemaDigest(configured)
			if err == nil {
				t.Fatalf("verifySchemaDigest(%q) = nil, want a mismatch error", configured)
			}
			var mismatchErr *verifySchemaDigestErr
			if !errors.As(err, &mismatchErr) {
				t.Fatalf("verifySchemaDigest(%q) returned %T, want *verifySchemaDigestErr", configured, err)
			}
			if mismatchErr.configured != configured {
				t.Errorf("mismatchErr.configured = %q, want %q", mismatchErr.configured, configured)
			}
			want := digest.Schema(schemav1.SDL)
			if mismatchErr.computed != want {
				t.Errorf("mismatchErr.computed = %q, want %q", mismatchErr.computed, want)
			}
			// The error text must be actionable: name BOTH values and the
			// one-true producer command, not just say "mismatch".
			msg := err.Error()
			for _, want := range []string{configured, "computed=", "registrydump", "-schema-digest"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error message %q does not contain %q", msg, want)
				}
			}
		})
	}
}

// TestBuildQueryRoute_SchemaDigestMismatch_FailsBeforeAnyNetworkIO proves
// verifySchemaDigest runs FIRST in buildQueryRoute -- a schema-digest
// mismatch must fail fast, before this process dials ClickHouse,
// Postgres, or reads a JWKS file it has no business connecting to if its
// own static configuration is already known to be wrong. Uses
// deliberately unreachable/invalid dependency config: if
// verifySchemaDigest did NOT run first, this would fail with a
// ClickHouse or Postgres connection error instead of a
// *verifySchemaDigestErr, which this test would catch.
func TestBuildQueryRoute_SchemaDigestMismatch_FailsBeforeAnyNetworkIO(t *testing.T) {
	cfg := queryRouteConfig{
		ClickHouseURI:       "clickhouse://unreachable-host-that-does-not-resolve.invalid:9999/default",
		RegistryPostgresURI: "postgres://unreachable-host-that-does-not-resolve.invalid:5432/nope",
		EnvelopeJWKSPath:    "/nonexistent/path/to/jwks.json",
		EnvelopeIssuer:      "test-issuer",
		EnvelopeAudience:    "test-audience",
		SchemaDigest:        "sha256:definitely-not-the-real-digest",
	}
	_, _, _, err := buildQueryRoute(cfg)
	if err == nil {
		t.Fatal("buildQueryRoute with a wrong schema digest returned nil error, want a mismatch error")
	}
	var mismatchErr *verifySchemaDigestErr
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("buildQueryRoute returned %v (%T), want *verifySchemaDigestErr -- "+
			"a network-dependency error here means verifySchemaDigest did NOT run first", err, err)
	}
}
