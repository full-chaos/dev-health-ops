//go:build integration

package main

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
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

// TestBuildQueryRoute_FailsFastOnMissingJWKS is the eager-check RED-FIRST
// proof for the "missing" shape: ClickHouse is REAL and reachable (so the
// existing eager ClickHouse ping does not fail this build for the wrong
// reason -- same discipline TestBuildQueryRoute_FailsFastOnWrongClickHouseProtocol
// and the Postgres-laziness test above apply), Postgres is a deliberately
// unreachable-but-lazy placeholder (buildQueryRoute must never reach it --
// the JWKS check fails first), and EnvelopeJWKSPath points at a path that
// does not exist on disk.
//
// RED-on-parent (executed, mutation): with the eager
// `verifier.CheckJWKS()` block in buildQueryRoute (query_route.go)
// commented out, this test's `buildErr == nil` assertion fails --
// buildQueryRoute mounts /query successfully against a JWKS path that
// does not exist, exactly CHAOS-4708's defect.
func TestBuildQueryRoute_FailsFastOnMissingJWKS(t *testing.T) {
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
		SchemaDigest:        itRealSchemaDigest,
	}

	_, _, _, buildErr := buildQueryRoute(cfg)
	if buildErr == nil {
		t.Fatal("buildQueryRoute succeeded against a JWKS path that does not exist on disk -- CHAOS-4708's defect: this instance would have mounted /query, answered /readyz with 'ready', and 401'd every authenticated request")
	}
	if !strings.Contains(buildErr.Error(), "JWKS") {
		t.Fatalf("buildQueryRoute error = %v, want it to name the JWKS readiness check", buildErr)
	}
}

// TestBuildQueryRoute_FailsFastOnMalformedJWKS is the eager-check
// RED-FIRST proof for the "malformed" shape -- same structure as the
// missing-file test above, but the file exists and is readable, just not
// a valid JWKS document (garbage bytes, not even JSON).
//
// RED-on-parent (executed, mutation): same as the missing-file test --
// with the eager check commented out, buildErr is nil here too.
func TestBuildQueryRoute_FailsFastOnMalformedJWKS(t *testing.T) {
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
		SchemaDigest:        itRealSchemaDigest,
	}

	_, _, _, buildErr := buildQueryRoute(cfg)
	if buildErr == nil {
		t.Fatal("buildQueryRoute succeeded against a malformed (non-JSON) JWKS document -- CHAOS-4708's defect")
	}
	if !strings.Contains(buildErr.Error(), "JWKS") {
		t.Fatalf("buildQueryRoute error = %v, want it to name the JWKS readiness check", buildErr)
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
		SchemaDigest:        itRealSchemaDigest,
	}

	_, ready, cleanup, buildErr := buildQueryRoute(cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute: %v", buildErr)
	}
	defer cleanup()

	t.Run("jwks valid at startup: 200", func(t *testing.T) {
		rec := httptest.NewRecorder()
		readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("/readyz = %d, want 200 with a valid JWKS and both other dependencies reachable (body %q)", rec.Code, rec.Body.String())
		}
	})

	t.Run("jwks deleted after startup: 503, not cached", func(t *testing.T) {
		if err := os.Remove(jwksPath); err != nil {
			t.Fatalf("remove jwks file: %v", err)
		}

		rec := httptest.NewRecorder()
		readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503 once the JWKS file is gone (body %q) -- CHAOS-4708's defect: a JWKS that dies after startup must not be cached as still-healthy", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "jwks") {
			t.Fatalf("/readyz 503 body %q does not name the jwks dependency", rec.Body.String())
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
		SchemaDigest:        itRealSchemaDigest,
	}

	_, ready, cleanup, buildErr := buildQueryRoute(cfg)
	if buildErr != nil {
		t.Fatalf("buildQueryRoute: %v", buildErr)
	}
	defer cleanup()

	t.Run("jwks valid at startup: 200", func(t *testing.T) {
		rec := httptest.NewRecorder()
		readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("/readyz = %d, want 200 with a valid JWKS (body %q)", rec.Code, rec.Body.String())
		}
	})

	t.Run("jwks overwritten with malformed content after startup: 503, not cached", func(t *testing.T) {
		// Same path, same file -- an atomically-wrong rewrite, not a
		// deletion. If a real rotation writes an empty/partial file
		// mid-write, this is the shape that catches it.
		if err := os.WriteFile(jwksPath, []byte("{not valid json"), 0o600); err != nil {
			t.Fatalf("overwrite jwks file: %v", err)
		}

		rec := httptest.NewRecorder()
		readyzHandler(ready)(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503 once the JWKS file is malformed (body %q)", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "jwks") {
			t.Fatalf("/readyz 503 body %q does not name the jwks dependency", rec.Body.String())
		}
	})
}
