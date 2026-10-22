//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate/schemagatetest"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestBuildQueryRouteRefusesToStartOnADatabaseOneMigrationBehind(t *testing.T) {
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
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := queryRouteConfig{
		ClickHouseURI:       ch.URI,
		RegistryPostgresURI: "postgres://unused:unused@127.0.0.1:1/unused",
		EnvelopeJWKSPath:    writeTestJWKS(t, pub),
		EnvelopeIssuer:      itTestIssuer,
		EnvelopeAudience:    itTestAudience,
	}
	saved := queryAPISchemaRecorder
	queryAPISchemaRecorder = func(*pgxpool.Pool) schemagate.Recorder { return schemagatetest.OneBehind(t) }
	t.Cleanup(func() { queryAPISchemaRecorder = saved })

	_, _, cleanup, buildErr := buildQueryRoute(os.Getenv, cfg)
	if buildErr == nil {
		cleanup()
		t.Fatal("buildQueryRoute started on a database one migration behind the build")
	}
	var behind *schemagate.BehindError
	if !errors.As(buildErr, &behind) {
		t.Fatalf("error = %v, want a BehindError", buildErr)
	}
}
