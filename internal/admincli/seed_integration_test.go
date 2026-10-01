//go:build integration

package admincli_test

import (
	"bytes"
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
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// seedPythonBuild is the build whose seed_feature_flags_async and
// STANDARD_FEATURES answered the frozen goldens: main when they were recorded.
const seedPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// seedGolden is the GoldenSpec of one oracle of this file.
func seedGolden(t *testing.T, file, digest string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        "testdata/golden/" + file + ".json",
		PythonBuild: seedPythonBuild,
		SHA256:      digest,
		Recipe: "git worktree add --detach $DIR " + seedPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/admincli/ -test '^" + t.Name() + "$' -python-root $DIR",
	}
}

// seedProgram runs the producer behind the Python verb the migrate Job runs
// after the upgrade (`dev-hops admin features seed`): seed_feature_flags_async
// on a session built the way the verb builds it (an async engine on the DSN,
// expire_on_commit=False). It prints the number of rows created. The verb's
// own module (dev_health_ops.api.admin.cli) imports the whole API, a Python
// closure the Go integration shards do not install; the producer is what
// writes the rows.
const seedProgram = `
import asyncio, sys
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from dev_health_ops.db import normalize_async_postgres_uri
from dev_health_ops.api.services.licensing import seed_feature_flags_async

async def main():
    engine = create_async_engine(normalize_async_postgres_uri(sys.argv[1]), pool_pre_ping=True)
    session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)()
    try:
        print(await seed_feature_flags_async(session))
    finally:
        await session.close()
        await engine.dispose()

asyncio.run(main())
`

// registryProgram prints Python's STANDARD_FEATURES registry.
const registryProgram = `
import json
from dev_health_ops.licensing.registry import STANDARD_FEATURES
print(json.dumps([[k, n, c.value, t.value, d] for k, n, c, t, d in STANDARD_FEATURES]))
`

// TestStandardFeaturesMatchThePythonRegistry requires the Go registry to equal Python's,
// row for row and in order. Python's registry is frozen at seedPythonBuild.
func TestStandardFeaturesMatchThePythonRegistry(t *testing.T) {
	golden := venueoracle.OpenGolden(t, seedGolden(t, "standard_features", "872225b77164e3a5e095cd15d9799b8c00cd2ce174de1b03a0bed8e4b7461922"))
	root := golden.PythonRoot(t, repoRoot(t))
	answers := golden.Produce(t, root, []venueoracle.Request{venueoracle.ProgramRequest("standard features", registryProgram, nil, nil)},
		func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
			return []venueoracle.Response{{Body: string(runPython(t, producer, registryProgram))}}
		})
	golden.Consumed(t, answers...)
	var rows [][]string
	if err := json.Unmarshal([]byte(answers[0].Body), &rows); err != nil {
		t.Fatalf("decode the Python registry: %v", err)
	}
	var want []admincli.Feature
	for _, row := range rows {
		want = append(want, admincli.Feature{Key: row[0], Name: row[1], Category: row[2], MinTier: row[3], Description: row[4]})
	}
	if len(want) == 0 || !reflect.DeepEqual(admincli.StandardFeatures, want) {
		t.Fatalf("admincli.StandardFeatures differs from Python's STANDARD_FEATURES:\n  go     %v\n  python %v", admincli.StandardFeatures, want)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// TestSeedMatchesThePythonProducer is the differential oracle: two databases built by
// the migrator, the same feature rows removed from both, the Python producer
// on one and dho's on the other. The rows must then be the same -- every
// column but the random id and the run-time stamps, which are checked for
// shape -- on a database missing features and on one that holds all of them.
// The producer's output and rows were executed once on seedPythonBuild and are
// frozen in testdata/golden/seed.json.
func TestSeedMatchesThePythonProducer(t *testing.T) {
	golden := venueoracle.OpenGolden(t, seedGolden(t, "seed", "cd0f089f8a447748858cc8408f60428fca7da678693f43bc9afa4fd5081ac884"))
	root := golden.PythonRoot(t, repoRoot(t))
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
		pgschema.ApplyURI(ctx, t, uris[index])
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
		before := [2]map[string]string{existingRows(t, ctx, databases[0]), existingRows(t, ctx, databases[1])}
		// The Python producer runs only while recording; its printed count
		// and the rows it left are frozen.
		var pythonWindow [2]time.Time
		answers := golden.Produce(t, root, []venueoracle.Request{venueoracle.ProgramRequest("seed: "+pass, seedProgram, nil, nil)},
			func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
				pythonStarted := time.Now()
				output := runPython(t, producer, seedProgram, uris[0])
				pythonWindow = [2]time.Time{pythonStarted, time.Now()}
				return []venueoracle.Response{{Body: string(output)}}
			})
		golden.Consumed(t, answers...)
		output := answers[0].Body
		goStarted := time.Now()
		result, err := admincli.Seed(ctx, databases[1], time.Now)
		if err != nil {
			t.Fatalf("%s: dho seed: %v", pass, err)
		}
		goWindow := [2]time.Time{goStarted, time.Now()}

		wantCreated := removed
		wantPython := fmt.Sprint(len(removed))
		if pass == "nothing missing" {
			wantCreated, wantPython = []string{}, "0"
		}
		if !reflect.DeepEqual(sorted(result.Created), sorted(wantCreated)) {
			t.Fatalf("%s: dho created %v, want %v", pass, result.Created, wantCreated)
		}
		if strings.TrimSpace(output) != wantPython {
			t.Fatalf("%s: the Python seed created %q row(s), want %s", pass, output, wantPython)
		}
		// A row that existed before the seed is left exactly as it was, id and
		// stamps included, by both seeds (the Python database's rows are read
		// while recording, when its seed ran).
		unchanged := func(index int) {
			for key, row := range existingRows(t, ctx, databases[index]) {
				if was, existed := before[index][key]; existed && row != was {
					t.Fatalf("%s: database %d: the seed changed the existing row %s:\n  before %s\n  after  %s", pass, index, key, was, row)
				}
			}
		}
		unchanged(1)
		pythonRows := strings.Split(golden.InspectRows(t, "python feature rows: "+pass, func() string {
			unchanged(0)
			return strings.Join(featureRows(t, ctx, databases[0], wantCreated, pythonWindow), "\n")
		}), "\n")
		goRows := featureRows(t, ctx, databases[1], wantCreated, goWindow)
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

	// The verb itself, as the migrate Job runs it: it resolves the database
	// from MIGRATION_DATABASE_URI, names the source, host and database at
	// Info without credentials, and prints the result.
	var run func(context.Context, cli.Env) int
	for _, child := range admincli.Command().Children[0].Children {
		if child.Name == "seed" {
			run = child.Run
		}
	}
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) {
		value, ok := map[string]string{"MIGRATION_DATABASE_URI": uris[1]}[key]
		return value, ok
	}
	if code := run(ctx, cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &stderr}); code != cli.ExitOK {
		t.Fatalf("the seed verb exited %d: %s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != `{"created":[]}` {
		t.Fatalf("the seed verb printed %q, want {\"created\":[]}", stdout.String())
	}
	parsed, err := url.Parse(uris[1])
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	logged := stderr.String()
	for _, want := range []string{`"msg":"feature seed database"`, `"source":"MIGRATION_DATABASE_URI"`, `"database":"` + strings.TrimPrefix(parsed.Path, "/") + `"`, `"msg":"feature seed done"`} {
		if !strings.Contains(logged, want) {
			t.Fatalf("the seed verb's log lacks %s: %s", want, logged)
		}
	}
	if password != "" && strings.Contains(logged, password) {
		t.Fatalf("the seed verb logged the password: %s", logged)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// existingRows reads every feature_flags row in full, id and stamps
// included, keyed by key.
func existingRows(t *testing.T, ctx context.Context, conn *pgx.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT key, row_to_json(f)::text FROM feature_flags f")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key, row string
		if err := rows.Scan(&key, &row); err != nil {
			t.Fatal(err)
		}
		out[key] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// featureRows reads feature_flags with every value compared as text, except
// the id and the stamps: those are random or run-time. For a row this pass's
// seed created, they are replaced by whether they have the shape the model
// gives them (a uuid4 id; created_at = updated_at, inside this seed's run
// window); for every other row they are left out here, and the full-row
// snapshot above requires them unchanged.
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

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// runPython runs program with args through the harness's launcher: the pinned
// interpreter in the closed environment, so nothing ambient shapes the answer.
// args are the program's own (the address of the run's database).
func runPython(t *testing.T, producer *venueoracle.Producer, program string, args ...string) []byte {
	t.Helper()
	command, err := producer.Command(context.Background(), nil, nil, append([]string{"-c", program}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("python: %v", pyoracle.RunError(command.Path, err, stderr))
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
