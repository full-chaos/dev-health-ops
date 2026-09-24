//go:build integration

package admincli_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/admincli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// upgradeProgram runs the Python upgrade the migrate Job runs, with no River
// step, so the database holds the schema and the migrations' own seed rows.
const upgradeProgram = `
import argparse, sys
from dev_health_ops.db import normalize_async_postgres_uri
from dev_health_ops.migrate import _run_upgrade
sys.exit(_run_upgrade(argparse.Namespace(db=normalize_async_postgres_uri(sys.argv[1]), revision="head")))
`

// seedProgram runs the Python verb the migrate Job runs after the upgrade:
// `dev-hops admin features seed`.
const seedProgram = `
import argparse, sys
from dev_health_ops.api.admin.cli import seed_features
sys.exit(seed_features(argparse.Namespace(db=sys.argv[1])))
`

// registryProgram prints Python's STANDARD_FEATURES registry.
const registryProgram = `
import json
from dev_health_ops.licensing.registry import STANDARD_FEATURES
print(json.dumps([[k, n, c.value, t.value, d] for k, n, c, t, d in STANDARD_FEATURES]))
`

// TestStandardFeaturesMatchPython requires the Go registry to equal Python's,
// row for row and in order.
func TestStandardFeaturesMatchPython(t *testing.T) {
	root, python := pythonAt(t)
	output := runPython(t, root, python, registryProgram)
	var rows [][]string
	if err := json.Unmarshal(output, &rows); err != nil {
		t.Fatalf("decode the Python registry: %v", err)
	}
	var want []admincli.Feature
	for _, row := range rows {
		want = append(want, admincli.Feature{Key: row[0], Name: row[1], Category: row[2], MinTier: row[3], Description: row[4]})
	}
	if len(want) == 0 || !reflect.DeepEqual(admincli.StandardFeatures, want) {
		t.Fatalf("admincli.StandardFeatures differs from Python's STANDARD_FEATURES:\n  go     %v\n  python %v", admincli.StandardFeatures, want)
	}
}

// TestSeedMatchesPython is the differential oracle: two databases built by
// the real Python upgrade, the same feature rows removed from both, the
// Python verb on one and dho's on the other. The rows must then be the same
// -- every column but the random id and the run-time stamps, which are
// checked for shape -- on a database missing features and on one that
// holds all of them.
func TestSeedMatchesPython(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	root, python := pythonAt(t)
	t.Setenv("DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", "1")
	t.Setenv("MIGRATION_DATABASE_URI", "")
	os.Unsetenv("MIGRATION_DATABASE_URI")
	admin, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	removed := []string{admincli.StandardFeatures[0].Key, admincli.StandardFeatures[7].Key, admincli.StandardFeatures[len(admincli.StandardFeatures)-1].Key}
	var databases [2]*pgx.Conn
	var uris [2]string
	for index := range databases {
		name := scratchDatabase(t, ctx, admin)
		uris[index] = databaseURI(t, instance.URI, name)
		runPython(t, root, python, upgradeProgram, uris[index])
		conn, err := pgx.Connect(ctx, uris[index])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		if _, err := conn.Exec(ctx, "DELETE FROM feature_flags WHERE key = ANY($1)", removed); err != nil {
			t.Fatal(err)
		}
		databases[index] = conn
	}

	for _, pass := range []string{"missing features", "nothing missing"} {
		pythonStarted := time.Now()
		output := runPython(t, root, python, seedProgram, uris[0])
		pythonWindow := [2]time.Time{pythonStarted, time.Now()}
		goStarted := time.Now()
		result, err := admincli.Seed(ctx, databases[1], time.Now)
		if err != nil {
			t.Fatalf("%s: dho seed: %v", pass, err)
		}
		goWindow := [2]time.Time{goStarted, time.Now()}

		wantCreated := removed
		wantPython := fmt.Sprintf("Seeded %d feature flags.", len(removed))
		if pass == "nothing missing" {
			wantCreated, wantPython = []string{}, "All feature flags already exist."
		}
		if !reflect.DeepEqual(sorted(result.Created), sorted(wantCreated)) {
			t.Fatalf("%s: dho created %v, want %v", pass, result.Created, wantCreated)
		}
		if !strings.Contains(string(output), wantPython) {
			t.Fatalf("%s: the Python seed printed %q, want %q", pass, output, wantPython)
		}
		pythonRows := featureRows(t, ctx, databases[0], removed, pythonWindow)
		goRows := featureRows(t, ctx, databases[1], removed, goWindow)
		if !reflect.DeepEqual(goRows, pythonRows) {
			var differing []string
			for index := 0; index < len(goRows) || index < len(pythonRows); index++ {
				var g, p string
				if index < len(goRows) {
					g = goRows[index]
				}
				if index < len(pythonRows) {
					p = pythonRows[index]
				}
				if g != p {
					differing = append(differing, fmt.Sprintf("\n  dho    %s\n  python %s", g, p))
				}
			}
			t.Fatalf("%s: feature_flags differ in %d row(s):%s", pass, len(differing), strings.Join(differing, ""))
		}
	}
}

// featureRows reads feature_flags with every value compared as text, except
// the id and the stamps of the rows a seed created: those are random or
// run-time, so each is replaced by whether it has the shape the model gives it
// (a uuid4 id; created_at = updated_at, inside the run window).
func featureRows(t *testing.T, ctx context.Context, conn *pgx.Conn, created []string, window [2]time.Time) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT key, name, coalesce(description, '<null>'), category, min_tier, is_enabled, is_beta, is_deprecated, "+
		"coalesce(config_schema::text, '<null>'), id::text, created_at, updated_at FROM feature_flags ORDER BY key")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seeded := map[string]bool{}
	for _, key := range created {
		seeded[key] = true
	}
	var out []string
	for rows.Next() {
		var key, name, description, category, tier, schema, id string
		var enabled, beta, deprecated bool
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&key, &name, &description, &category, &tier, &enabled, &beta, &deprecated, &schema, &id, &createdAt, &updatedAt); err != nil {
			t.Fatal(err)
		}
		stamps := "migration"
		if seeded[key] {
			inWindow := !createdAt.Before(window[0].Add(-time.Second)) && !createdAt.After(window[1].Add(time.Second))
			stamps = fmt.Sprintf("uuid4=%v stamps_equal=%v in_run=%v", isUUID4(id), createdAt.Sub(updatedAt).Abs() < time.Millisecond, inWindow)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%s|%v|%v|%v|%s|%s", key, name, description, category, tier, enabled, beta, deprecated, schema, stamps))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func isUUID4(id string) bool {
	return len(id) == 36 && id[14] == '4' && strings.ContainsRune("89ab", rune(id[19]))
}

func sorted(values []string) []string {
	out := append([]string{}, values...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func pythonAt(t *testing.T) (string, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root, pyoracle.Resolve(t, root)
}

func runPython(t *testing.T, root, python, program string, args ...string) []byte {
	t.Helper()
	command := exec.Command(python, append([]string{"-c", program}, args...)...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("python: %v", pyoracle.RunError(python, err, output))
	}
	return output
}

func scratchDatabase(t *testing.T, ctx context.Context, admin *pgx.Conn) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dho_features_seed_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

func databaseURI(t *testing.T, uri, database string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	return parsed.String()
}
