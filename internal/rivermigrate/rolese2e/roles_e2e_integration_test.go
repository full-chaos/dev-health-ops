//go:build integration

// Package rolese2e holds the end-to-end proof of `dho migrate roles` (CHAOS-6901).
// It is its own package because it calls CheckDomainAuthorization: the venue-posture
// coverage guard (internal/storage/postgres) requires such a package to hand-build
// every posture table, and this one builds no venue -- it runs on the REAL migrated
// schema (pgschema.Apply) -- so it is registered there as exempt, with that reason.
package rolese2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

func randomName(t *testing.T, prefix string) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	return prefix + hex.EncodeToString(suffix)
}

// CHAOS-6901, end to end through the real commands on the real migrated schema:
// `dho migrate roles` provisions the logins, `dho migrate river` then grants their
// declared postures and runs its executed posture gate, and each role -- logged in
// with the password the roles leg set -- passes its own hold-exactly readiness
// check. Running the roles leg again afterwards must not strip a grant (the
// CHAOS-4261 property the old script had to be rewritten to keep).
func TestMigrateRolesThenRiverLeavesEveryRoleHoldingExactlyItsManifest(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)

	const (
		domainPass, queuePass, coordinatorPass = "e2e-domain-pw", "e2e-queue-pw", "e2e-coordinator-pw"
		apiPass, queryAPIPass, kedaPass        = "e2e-api-pw", "e2e-query-api-pw", "e2e-keda-pw"
	)
	names := map[string]string{
		"domain": randomName(t, "e2e_domain_"), "queue": randomName(t, "e2e_queue_"), "coordinator": randomName(t, "e2e_coord_"),
		"api": randomName(t, "e2e_api_"), "query_api": randomName(t, "e2e_qapi_"), "keda": randomName(t, "e2e_keda_"),
	}
	passwords := []string{domainPass, queuePass, coordinatorPass, apiPass, queryAPIPass, kedaPass}
	dir := t.TempDir()
	kedaFile := filepath.Join(dir, "keda-password")
	if err := os.WriteFile(kedaFile, []byte(kedaPass+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		"RIVER_DOMAIN_DATABASE_ROLE":          names["domain"],
		"RIVER_QUEUE_DATABASE_ROLE":           names["queue"],
		"RIVER_COORDINATOR_DATABASE_ROLE":     names["coordinator"],
		"RIVER_DOMAIN_DATABASE_PASSWORD":      domainPass,
		"RIVER_QUEUE_DATABASE_PASSWORD":       queuePass,
		"RIVER_COORDINATOR_DATABASE_PASSWORD": coordinatorPass,
		"API_DATABASE_ROLE":                   names["api"],
		"API_DATABASE_PASSWORD":               apiPass,
		"QUERY_API_DATABASE_ROLE":             names["query_api"],
		"QUERY_API_DATABASE_PASSWORD":         queryAPIPass,
	}
	run := func(fn func(context.Context, string, []string, func(string) (string, bool), *bytes.Buffer, *bytes.Buffer) int, args []string, extra map[string]string) (int, string, string) {
		settings := map[string]string{}
		for key, value := range base {
			settings[key] = value
		}
		for key, value := range extra {
			settings[key] = value
		}
		var stdout, stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := settings[key]; return value, ok }
		code := fn(ctx, "dho", args, lookup, &stdout, &stderr)
		for _, secret := range passwords {
			if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
				t.Fatalf("a password leaked into the command output:\n%s\n%s", stdout.String(), stderr.String())
			}
		}
		return code, stdout.String(), stderr.String()
	}
	roles := func(args []string, extra map[string]string) (int, string, string) {
		return run(func(c context.Context, s string, a []string, l func(string) (string, bool), o, e *bytes.Buffer) int {
			return rivermigrate.ExecuteRoles(c, s, a, l, o, e)
		}, args, extra)
	}
	river := func(extra map[string]string) (int, string, string) {
		return run(func(c context.Context, s string, a []string, l func(string) (string, bool), o, e *bytes.Buffer) int {
			return rivermigrate.Execute(c, s, a, l, o, e)
		}, nil, extra)
	}

	// 1. roles (the River schema does not exist yet: no KEDA login on a fresh install).
	if code, stdout, stderr := roles(nil, nil); code != cli.ExitOK || !strings.Contains(stdout, "meet the bootstrap postconditions") {
		t.Fatalf("migrate roles: exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	// 2. river: grants the postures and runs its executed posture gate.
	if code, stdout, stderr := river(nil); code != cli.ExitOK {
		t.Fatalf("migrate river after migrate roles: exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	loginPool := func(role, password string) *pgxpool.Pool {
		config, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.User, config.ConnConfig.Password = role, password
		config.MaxConns = 1
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	assertHoldExactly := func(when string) {
		t.Helper()
		checkCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		for label, check := range map[string]func() error{
			"domain": func() error {
				return postgresstore.CheckDomainAuthorization(checkCtx, loginPool(names["domain"], domainPass), names["domain"], "river")
			},
			"queue": func() error {
				return postgresstore.CheckQueueAuthorization(checkCtx, loginPool(names["queue"], queuePass), names["queue"], "river")
			},
			"coordinator": func() error {
				return postgresstore.CheckCoordinatorAuthorization(checkCtx, loginPool(names["coordinator"], coordinatorPass), names["coordinator"], "river")
			},
			"api": func() error {
				return postgresstore.CheckAPIAuthorization(checkCtx, loginPool(names["api"], apiPass), names["api"], "river")
			},
			"query_api": func() error {
				return postgresstore.CheckQueryAPIAuthorization(checkCtx, loginPool(names["query_api"], queryAPIPass), names["query_api"], "river")
			},
		} {
			if err := check(); err != nil {
				t.Errorf("%s: the %s role does not hold exactly its manifest: %v", when, label, err)
			}
		}
	}
	assertHoldExactly("after migrate roles then migrate river")

	// 3. the roles leg again, now with the KEDA login (the River schema exists), via
	// its _FILE password; then --check; then river once more.
	withKeda := map[string]string{
		"RIVER_KEDA_READONLY_DATABASE_ROLE": names["keda"],
		"RIVER_KEDA_READONLY_PASSWORD_FILE": kedaFile,
	}
	if code, stdout, stderr := roles(nil, withKeda); code != cli.ExitOK {
		t.Fatalf("migrate roles with KEDA: exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if code, stdout, stderr := roles([]string{"--check"}, withKeda); code != cli.ExitOK || strings.Contains(stdout, "provisioned:") {
		t.Fatalf("migrate roles --check: exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if code, stdout, stderr := river(nil); code != cli.ExitOK {
		t.Fatalf("migrate river after a second migrate roles: exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	assertHoldExactly("after a second migrate roles")
	var kedaReads bool
	if err := loginPool(names["keda"], kedaPass).QueryRow(ctx, `SELECT count(*) >= 0 FROM river.river_job`).Scan(&kedaReads); err != nil || !kedaReads {
		t.Fatalf("the KEDA login must read river_job with its file-supplied password: %v", err)
	}
	// CHAOS-6946: and it runs the go-sync planned-backlog trigger's query, the state the
	// grant exists to reach (on the real migrated schema, through the real commands).
	var planned int
	if err := loginPool(names["keda"], kedaPass).QueryRow(ctx,
		`SELECT count(*) FROM public.sync_run_units WHERE status = 'planned' AND (available_at IS NULL OR available_at <= now())`).Scan(&planned); err != nil {
		t.Fatalf("the KEDA login must read public.sync_run_units after `migrate roles`: %v", err)
	}
}
