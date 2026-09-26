//go:build integration

package prove

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof/writeproof"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
)

const writeDoc = "mutation SyntheticCreate($name: String!) { syntheticCreate(name: $name) { id } }"

type verbSeeder struct{}

func (verbSeeder) Seed(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
	_, err := db.Exec(ctx, `INSERT INTO wp_verb (org, run, name) VALUES ($1,$2,'seed')`, org, string(run))
	return err
}

func (verbSeeder) Teardown(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
	_, err := db.Exec(ctx, `DELETE FROM wp_verb WHERE org=$1 AND run=$2`, org, string(run))
	return err
}

func verbCase(name, baseline string) writeproof.Case {
	return writeproof.Case{
		Name: name, Operation: "syntheticCreate", VariablesJSON: `{"name":"made"}`, Seeder: verbSeeder{},
		Tables:         []writeproof.Table{{Label: "wp_verb", SQL: `SELECT name FROM wp_verb WHERE org=$1 AND run=$2 ORDER BY name`}},
		BaselineDigest: baseline,
	}
}

func TestTheWriteVerbEndToEndAgainstARealDatabase(t *testing.T) {
	ctx := context.Background()
	startCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(startCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(startCtx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, ddl := range []string{registryschema.DDL, `CREATE TABLE wp_verb (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), seq BIGSERIAL, org TEXT NOT NULL, run TEXT NOT NULL, name TEXT NOT NULL)`} {
		if _, err := pool.Exec(startCtx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	withFakePool(t, poolAdapter{pool})

	const build = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	digest := goapidigest.Document(writeDoc)
	registryBody := fmt.Sprintf(`{"schema_digest":"sha256:verbschema","operations":[{"operation":"syntheticCreate","document_digest":%q}]}`, digest)
	registry := httptest.NewServer(writeStaticJSONHandler(registryBody))
	t.Cleanup(registry.Close)
	// /buildinfo names the build; when buildMoves is set the FIRST read (the
	// opening one) names it and every later read (the closing stability check)
	// names another: the build moved under the run.
	var buildReads int
	buildMoves := false
	buildinfo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buildReads++
		commit := build
		if buildMoves && buildReads > 1 {
			commit = "0000000000000000000000000000000000000000"
		}
		writeStaticJSONHandler(`{"commit":"`+commit+`","modified":false}`)(w, r)
	}))
	t.Cleanup(buildinfo.Close)

	// The deployed build: persists a row named by the request, or a divergent
	// name when told to, and stamps its build on the response.
	var posts int
	divergent := false
	omitBuild := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			Variables struct {
				Name string `json:"name"`
			} `json:"variables"`
		}
		_ = json.Unmarshal(raw, &body)
		posts++
		name := body.Variables.Name
		if divergent {
			name = "a divergent write"
		}
		// The run tag is the seeded row's; the verb's mutation stand-in writes into it.
		var run string
		if err := pool.QueryRow(r.Context(), `SELECT run FROM wp_verb WHERE name='seed' AND run NOT LIKE 'gwc-wp-probe-%' ORDER BY seq DESC LIMIT 1`).Scan(&run); err != nil {
			t.Errorf("no seeded dataset when the mutation arrived: %v", err)
		}
		if _, err := pool.Exec(r.Context(), `INSERT INTO wp_verb (org, run, name) SELECT org, run, $1 FROM wp_verb WHERE run=$2 LIMIT 1`, name, run); err != nil {
			t.Errorf("mutation effect: %v", err)
		}
		if !omitBuild {
			w.Header().Set("x-dev-health-build", build)
		}
		_, _ = w.Write([]byte(`{"data":{"syntheticCreate":{"id":"11111111-1111-4111-8111-111111111111"}}}`))
	}))
	t.Cleanup(server.Close)

	docs, _ := json.Marshal([]map[string]string{{"operation": "syntheticCreate", "document": writeDoc}})
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docs, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(fixtureOrgEnvVar, "org-fixture")
	withOrgMinter(t, "org-fixture", "org-fixture")
	withProverCommit(t, build)

	// Bootstrap each case's baseline by EXECUTING it once (freshness by execution);
	// the case name is part of the digest, so each case gets its own.
	baselineFor := func(name string) string {
		probe := verbCase(name, "sha256:placeholder")
		tag := writeproof.RunTag("gwc-wp-probe-" + name)
		res, err := writeproof.Execute(ctx, pool, "org-fixture", probe, tag, writeDoc, func(c context.Context, _, _ string) (writeproof.Response, error) {
			if _, err := pool.Exec(c, `INSERT INTO wp_verb (org, run, name) VALUES ('org-fixture',$1,'made')`, string(tag)); err != nil {
				return writeproof.Response{}, err
			}
			return writeproof.Response{Status: 200, Body: []byte(`{"data":{"syntheticCreate":{"id":"11111111-1111-4111-8111-111111111111"}}}`)}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return res.Digest
	}
	baseline := baselineFor("verb-match")
	writeproof.Register(verbCase("verb-match", baseline))
	writeproof.Register(verbCase("verb-mismatch", baselineFor("verb-mismatch")))

	args := func(via, caseName string) []string {
		return []string{
			"-registry-url=" + registry.URL + "/registry", "-buildinfo-url=" + buildinfo.URL + "/buildinfo",
			"-query-url=" + server.URL + "/query", "-edge-url=" + server.URL + "/graphql", "-via=" + via,
			"-documents=" + docsPath, "-postgres-uri=postgres://fake/ignored", "-org=org-fixture", "-case=" + caseName,
			"-recorded-by=harness", "-review-evidence=verb e2e", "-timeout=10s",
		}
	}
	count := func(query string, args ...any) int {
		var n int
		if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// rowsOfRun counts the rows of the run tag the verb printed on its first line:
	// the assertions below are about THIS run's dataset, not about any dataset.
	rowsOfRun := func(out string) int {
		m := regexp.MustCompile(`run=(gwc-wp-[0-9a-f]+)`).FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("the verb printed no run tag:\n%s", out)
		}
		return count(`SELECT count(*) FROM wp_verb WHERE run=$1`, m[1])
	}

	// 1. A match via query-api: one post, a proof-route write receipt carrying the
	// digest, the dataset gone.
	var runErr error
	out := captureStdout(t, func() { runErr = runWrite(args(viaQueryAPI, "verb-match")) })
	if runErr != nil {
		t.Fatalf("a match must succeed: %v\n%s", runErr, out)
	}
	if posts != 1 {
		t.Fatalf("the mutation must be posted exactly once, got %d", posts)
	}
	if n := count(`SELECT count(*) FROM go_api_proof_run WHERE stage='write_executed' AND terminal_state='match' AND measurement_route='proof' AND side_effect_digest=$1 AND build_binding='per_request' AND candidate_build=$2 AND selected_operation='syntheticCreate'`, baseline, build); n != 1 {
		t.Fatalf("want exactly one match receipt, got %d\n%s", n, out)
	}
	if n := count(`SELECT count(*) FROM wp_verb WHERE run NOT LIKE 'gwc-wp-probe-%'`); n != 0 {
		t.Fatalf("a match must leave no dataset, %d rows remain", n)
	}
	// The receipt admits the mutation for canary and not for primary.
	admit := func(mode string) bool {
		found, err := goapiproof.OperationsWithEnablementProofByKind(ctx, pool, "sha256:verbschema", build, mode,
			map[string]string{"syntheticCreate": digest}, map[string]string{"syntheticCreate": goapiproof.OperationKindMutation})
		if err != nil {
			t.Fatal(err)
		}
		return found["syntheticCreate"]
	}
	if !admit(goapiproof.TargetModeCanary) || admit(goapiproof.TargetModePrimary) {
		t.Fatal("a proof-route write receipt must admit canary and never primary")
	}

	// 2. The same case via the edge: an edge-route receipt now also admits primary.
	posts = 0
	if err := func() error {
		var e error
		captureStdout(t, func() { e = runWrite(args(viaEdge, "verb-match")) })
		return e
	}(); err != nil {
		t.Fatalf("edge match must succeed: %v", err)
	}
	if !admit(goapiproof.TargetModePrimary) {
		t.Fatal("an edge-route write receipt must admit primary")
	}

	// 3. A divergent write: an error, a mismatch receipt with the observed digest,
	// and the dataset KEPT.
	divergent = true
	posts = 0
	out = captureStdout(t, func() { runErr = runWrite(args(viaQueryAPI, "verb-mismatch")) })
	if runErr == nil || posts != 1 {
		t.Fatalf("a divergent write must fail after one post: err=%v posts=%d", runErr, posts)
	}
	if n := count(`SELECT count(*) FROM go_api_proof_run WHERE stage='write_executed' AND terminal_state='mismatch' AND side_effect_digest <> $1`, baseline); n != 1 {
		t.Fatalf("want one mismatch receipt with the observed digest, got %d\n%s", n, out)
	}
	if n := count(`SELECT count(*) FROM wp_verb WHERE run NOT LIKE 'gwc-wp-probe-%'`); n == 0 {
		t.Fatal("a mismatch must keep the dataset for forensics")
	}

	divergent = false

	// 3b. r1 P1: a response without the candidate build header is never a match:
	// the verb fails, the dataset is KEPT and no receipt can admit the operation.
	omitBuild = true
	posts = 0
	receiptsBefore := count(`SELECT count(*) FROM go_api_proof_run WHERE terminal_state='match'`)
	keptBefore := count(`SELECT count(*) FROM wp_verb WHERE run NOT LIKE 'gwc-wp-probe-%'`)
	out = captureStdout(t, func() { runErr = runWrite(args(viaQueryAPI, "verb-match")) })
	if runErr == nil || posts != 1 {
		t.Fatalf("a response with no build header must fail the verb after one post: err=%v posts=%d", runErr, posts)
	}
	if got := count(`SELECT count(*) FROM go_api_proof_run WHERE terminal_state='match'`); got != receiptsBefore {
		t.Fatalf("no new match receipt may exist, got %d more", got-receiptsBefore)
	}
	if kept := count(`SELECT count(*) FROM wp_verb WHERE run NOT LIKE 'gwc-wp-probe-%'`); kept <= keptBefore {
		t.Fatalf("the dataset must be kept when the write is not tied to the build (%d -> %d)\n%s", keptBefore, kept, out)
	}
	omitBuild = false

	// 3c. r2 P1: the build moves under a run whose effects MATCH. The verb records
	// proof_failed, and the dataset is KEPT: teardown is the last step and only a
	// still-matching, durably recorded run gets it.
	buildMoves, buildReads = true, 0
	posts = 0
	keptBefore = count(`SELECT count(*) FROM wp_verb WHERE run NOT LIKE 'gwc-wp-probe-%'`)
	failedBefore := count(`SELECT count(*) FROM go_api_proof_run WHERE terminal_state='proof_failed'`)
	matchesBefore := count(`SELECT count(*) FROM go_api_proof_run WHERE terminal_state='match'`)
	out = captureStdout(t, func() { runErr = runWrite(args(viaQueryAPI, "verb-match")) })
	buildMoves = false
	if runErr == nil || posts != 1 {
		t.Fatalf("a build that moved under the run must fail the verb after one post: err=%v posts=%d\n%s", runErr, posts, out)
	}
	if got := count(`SELECT count(*) FROM go_api_proof_run WHERE terminal_state='proof_failed'`); got != failedBefore+1 {
		t.Fatalf("want one new proof_failed receipt, got %d\n%s", got-failedBefore, out)
	}
	if got := count(`SELECT count(*) FROM go_api_proof_run WHERE terminal_state='match'`); got != matchesBefore {
		t.Fatalf("no new match receipt may exist when the build moved")
	}
	if rows := rowsOfRun(out); rows < 2 {
		t.Fatalf("THIS run's dataset (seed + the mutation's row) must be KEPT when the build moved, %d rows left\n%s", rows, out)
	}

	// 4. A dry run writes no receipt.
	before := count(`SELECT count(*) FROM go_api_proof_run`)
	divergent = false
	captureStdout(t, func() { _ = runWrite(append(args(viaQueryAPI, "verb-match"), "-dry-run")) })
	if after := count(`SELECT count(*) FROM go_api_proof_run`); after != before {
		t.Fatalf("-dry-run wrote %d receipts", after-before)
	}

	// 5. The receipt cannot be recorded (the table is gone): the verb fails and the
	// dataset of an otherwise matching run is KEPT, not torn down.
	if _, err := pool.Exec(ctx, `DROP TABLE go_api_proof_run CASCADE`); err != nil {
		t.Fatal(err)
	}
	keptBefore = count(`SELECT count(*) FROM wp_verb WHERE run NOT LIKE 'gwc-wp-probe-%'`)
	out = captureStdout(t, func() { runErr = runWrite(args(viaQueryAPI, "verb-match")) })
	if runErr == nil {
		t.Fatalf("a receipt that cannot be recorded must fail the verb\n%s", out)
	}
	if rows := rowsOfRun(out); rows < 2 {
		t.Fatalf("THIS run's dataset must be KEPT when the receipt was not recorded, %d rows left\n%s", rows, out)
	}
}

// poolAdapter keeps the test's pool open across runWrite calls (runWrite closes
// the pool it opened).
type poolAdapter struct{ *pgxpool.Pool }

func (poolAdapter) Close() {}
