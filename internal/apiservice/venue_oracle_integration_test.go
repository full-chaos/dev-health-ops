//go:build integration

package apiservice

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The venue oracle (internal/testsupport/venueoracle) for this service's
// routes: the REAL Python api and the REAL dho api (configure(), as the api
// role after the River migration) answer the same requests against two
// copies of one seeded Postgres database. Responses must match byte for
// byte, and the rows a write touches must match afterwards.

const venueKey = "venue-oracle-signing-key-0123456789abcdef"

func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// TestVenueOracleProtectedRoutes is the write-and-read differential.
func TestVenueOracleProtectedRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	var seed venueFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string]map[string]any {
			seed = venueSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})

	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
	}
	base := startVenueAPI(t, ctx, cfg, venue)

	requests := venueRequests(seed, venue.Tokens)
	var receipt strings.Builder
	receipt.WriteString(venueoracle.Diff(t, base, requests, venue.ServePython(t, requests), venueoracle.DiffOptions{}))
	// The rows the writes touched are identical on both copies.
	compareRows(t, ctx, venue, &receipt, "organizations", `SELECT id::text, slug, name, coalesce(description, '<null>'), tier, is_active,
		updated_at > created_at FROM organizations ORDER BY slug`)
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt.String()), 0o600)
	}
	t.Log("\n" + receipt.String())
}

// startVenueAPI runs configure() as a deploy does and returns the api's
// base URL once readiness is true as the api role.
func startVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue) string {
	t.Helper()
	registry := health.NewRegistry(5 * time.Second)
	components, err := configure(ctx, cfg, registry, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range components {
		if err := component.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	})
	if ready := registry.CheckRequired(ctx); !ready.Ready {
		t.Fatalf("dho api not ready as the api role: %+v %s", ready, venue.DiagnoseAPIRole(t, ctx))
	}
	for _, component := range components {
		if server, ok := component.(interface{ Address() string }); ok {
			return "http://" + server.Address()
		}
	}
	t.Fatal("configure started no HTTP server")
	return ""
}

func compareRows(t *testing.T, ctx context.Context, venue *venueoracle.Venue, receipt *strings.Builder, name, query string) {
	t.Helper()
	pyRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
	if pyRows != goRows || pyRows == "" {
		t.Errorf("%s after the writes differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
	}
	fmt.Fprintf(receipt, "%s rows after writes: %s\n", name, venueoracle.Mark(pyRows == goRows && pyRows != ""))
}
