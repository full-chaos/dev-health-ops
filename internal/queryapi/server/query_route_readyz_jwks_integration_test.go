//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-4708: principal.NewVerifier loads its JWKS lazily, per Verify
// call, by deliberate design (a rotated key is picked up without a
// restart) -- which means a missing, unreadable, empty, or malformed
// GO_API_ENVELOPE_JWKS_PATH was, before this fix, invisible to BOTH
// startup (buildQueryRoute never read the file) AND readiness (nothing
// checked it): the process would start, mount /query, and answer
// /readyz with exactly "ready", while every authenticated request
// 401'd -- indistinguishable from outside "Go is enabled and serving"
// vs. "Go is enabled and has never served one byte". This file proves
// both halves of the fix this ticket adds: an EAGER check in
// buildQueryRoute (refuses to start on an already-broken JWKS, matching
// buildQueryRoute's existing ClickHouse-protocol-mismatch precedent) and
// a LIVE, uncached readiness-loop check (catches a JWKS that goes bad
// AFTER a healthy boot -- deleted, truncated, rotated wrong -- without a
// restart, matching the ClickHouse-dies-after-startup precedent in
// query_route_readyz_integration_test.go). Two distinct failure shapes
// are tested throughout -- "missing" (os.ReadFile's *PathError) and
// "malformed" (authverify.ErrInvalidJWKS) -- because they are genuinely
// different code paths inside authverify.Ed25519JWKSVerifier.Keys() and
// a fix could easily close only one.
//
// The eager half is gone: the envelope key is optional and never fails
// start (D6465). A key with no usable JWKS at start is no key: the build
// succeeds with no query_jwks probe. The live half stays: a key usable at
// start that goes bad later still fails query_jwks.

// TestBuildQueryRoute_StartsOnAMissingJWKS: the envelope key is optional and never fails start (D6465). With
// ClickHouse real and Postgres a lazy placeholder, a key whose JWKS path does not exist is no usable key: the build
// succeeds, the envelope class is refused (nil verifier), and readiness has no query_jwks probe.
func TestBuildQueryRoute_StartsOnAMissingJWKS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})

	cfg := queryRouteConfig{
		ClickHouseURI:       ch.URI,
		RegistryPostgresURI: "postgres://unused:unused@127.0.0.1:1/unused",
		EnvelopeJWKSPath:    filepath.Join(t.TempDir(), "does-not-exist.json"),
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
	}

	assertStartsWithoutAUsableJWKS(t, cfg)
}

// TestBuildQueryRoute_StartsOnAMalformedJWKS is the same for a JWKS file that exists but is not a JWKS document.
func TestBuildQueryRoute_StartsOnAMalformedJWKS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})

	malformedPath := filepath.Join(t.TempDir(), "malformed-jwks.json")
	if err := os.WriteFile(malformedPath, []byte("this is not a jwks document"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := queryRouteConfig{
		ClickHouseURI:       ch.URI,
		RegistryPostgresURI: "postgres://unused:unused@127.0.0.1:1/unused",
		EnvelopeJWKSPath:    malformedPath,
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
	}

	assertStartsWithoutAUsableJWKS(t, cfg)
}

// assertStartsWithoutAUsableJWKS builds /query over cfg and wants it built, with no query_jwks probe.
func assertStartsWithoutAUsableJWKS(t *testing.T, cfg queryRouteConfig) {
	t.Helper()
	handlers, _, cleanup, buildErr := buildQueryRoute(withIssuer, cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute failed start over a key with no usable JWKS: %v", buildErr)
	}
	t.Cleanup(cleanup)
	if handlers.Query == nil {
		t.Fatal("no /query handler")
	}
	for _, probe := range handlers.Probes {
		if probe.Name == "query_jwks" {
			t.Fatalf("a query_jwks probe without a usable envelope key: %v", handlers.Probes)
		}
	}
}

// TestReadyz_JWKSValidAtStartup_ThenDeletedAfterStartup_Returns503 is the
// readiness-loop RED-FIRST proof, and the one that most literally matches
// this ticket's evidence bar: start a real server (real ClickHouse, real
// Postgres, a genuinely valid JWKS) so /readyz answers 200, THEN delete
// the JWKS file out from under the running process (simulating a mount
// that goes bad, or an operator/rotation mistake) and prove the VERY NEXT
// /readyz call flips to 503 -- no restart, no cache, matching
// TestReadyz_BothDependenciesReachable_ThenClickHouseDiesAfterStartup's
// proof shape for ClickHouse exactly. This also proves CHAOS-4708 does
// NOT defeat the JWKS's own no-restart rotation contract: the check
// re-reads the file fresh on every call.
//
// RED-on-parent (executed, mutation): with readinessCheck's
// `verifier.CheckJWKS()` call (query_route.go) commented out, the
// "jwks deleted after startup: 503" subtest below fails -- /readyz keeps
// answering 200 after the JWKS file is gone, exactly CHAOS-4708's defect
// ("Go is enabled and serving" vs. "Go is enabled and has never served
// one byte" become indistinguishable from outside).
func TestReadyz_JWKSValidAtStartup_ThenDeletedAfterStartup_Returns503(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})

	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start Postgres test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})

	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)

	cfg := queryRouteConfig{
		ClickHouseURI:       ch.URI,
		RegistryPostgresURI: pg.URI,
		EnvelopeJWKSPath:    jwksPath,
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
	}

	handlers, _, cleanup, buildErr := buildQueryRoute(withIssuer, cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute: %v", buildErr)
	}
	defer cleanup()

	t.Run("jwks valid at startup: 200", func(t *testing.T) {
		code, body := operatorReadyz(t, handlers.Probes...)
		if code != http.StatusOK {
			t.Fatalf("/readyz = %d, want 200 with a valid JWKS and both other dependencies reachable (body %q)", code, body)
		}
	})

	t.Run("jwks deleted after startup: 503, not cached", func(t *testing.T) {
		if err := os.Remove(jwksPath); err != nil {
			t.Fatalf("remove jwks file: %v", err)
		}

		code, body := operatorReadyz(t, handlers.Probes...)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503 once the JWKS file is gone (body %q) -- CHAOS-4708's defect: a JWKS that dies after startup must not be cached as still-healthy", code, body)
		}
		if !strings.Contains(body, "jwks") {
			t.Fatalf("/readyz 503 body %q does not name the jwks dependency", body)
		}
	})
}

// TestReadyz_JWKSValidAtStartup_ThenOverwrittenMalformedAfterStartup_Returns503
// is the "malformed after boot" companion to the deletion test above --
// the file still exists and is readable (so this is NOT the same code
// path as a missing file: os.ReadFile succeeds, JSON decode/validation
// fails), simulating a rotation that wrote bad content rather than one
// that removed the mount entirely.
func TestReadyz_JWKSValidAtStartup_ThenOverwrittenMalformedAfterStartup_Returns503(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})

	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start Postgres test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})

	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)

	cfg := queryRouteConfig{
		ClickHouseURI:       ch.URI,
		RegistryPostgresURI: pg.URI,
		EnvelopeJWKSPath:    jwksPath,
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
	}

	handlers, _, cleanup, buildErr := buildQueryRoute(withIssuer, cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute: %v", buildErr)
	}
	defer cleanup()

	t.Run("jwks valid at startup: 200", func(t *testing.T) {
		code, body := operatorReadyz(t, handlers.Probes...)
		if code != http.StatusOK {
			t.Fatalf("/readyz = %d, want 200 with a valid JWKS (body %q)", code, body)
		}
	})

	t.Run("jwks overwritten with malformed content after startup: 503, not cached", func(t *testing.T) {
		// Same path, same file -- an atomically-wrong rewrite, not a
		// deletion. If a real rotation writes an empty/partial file
		// mid-write, this is the shape that catches it.
		if err := os.WriteFile(jwksPath, []byte("{not valid json"), 0o600); err != nil {
			t.Fatalf("overwrite jwks file: %v", err)
		}

		code, body := operatorReadyz(t, handlers.Probes...)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503 once the JWKS file is malformed (body %q)", code, body)
		}
		if !strings.Contains(body, "jwks") {
			t.Fatalf("/readyz 503 body %q does not name the jwks dependency", body)
		}
	})
}
