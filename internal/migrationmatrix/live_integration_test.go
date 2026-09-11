//go:build integration

package migrationmatrix

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadRoutingState, DRIVEN -- not reconstructed.
//
// The pin written for an earlier third-reader finding executed the
// matrix's query SHAPE against a container while never
// calling the matrix's CODE: `ReadRoutingState` and `routingStateQuery`
// had 0% integration coverage, and mutating the production reader to use
// canary rules for primary SURVIVED the whole suite.
//
// That is the same class this branch fixed in #2428 -- a test that
// reconstructs production instead of driving it, and so can only ever
// agree with itself. It is here twice now, so this drives the exported
// function against a real PostgreSQL: the rows below are the two the
// route split must separate, and the only thing that can make them come
// out right is the production reader running the production predicate.
func TestReadRoutingStateAppliesTheModesOwnRouteRule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, routingMatrixDDL); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	const (
		digest = "sha256:live"
		build  = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	)
	// Two operations, identical except for the MODE of their routing row
	// and the ROUTE of their proof. Under the split, exactly one is proven:
	//   primaryOp  -- mode=primary, proof measured on the PROOF route  -> NOT proven
	//   canaryOp   -- mode=canary,  proof measured on the PROOF route  ->     proven
	// A reader that applies one rule to both gets one of them wrong, and
	// which one it gets wrong says which rule it collapsed to.
	for _, seed := range []struct {
		operation, document, mode, route string
	}{
		{"primaryOp", "doc-primary", "primary", "proof"},
		{"canaryOp", "doc-canary", "canary", "proof"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_candidate_build
			   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
			 VALUES ($1,$2,$3,$4, now())`,
			digest, seed.document, seed.operation, build); err != nil {
			t.Fatalf("seed candidate build: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_routing_state
			   (schema_digest, document_digest, selected_operation, mode,
			    current_candidate_build, rollout_percentage, owner, updated_at)
			 VALUES ($1,$2,$3,$4,$5,100,'go', now())`,
			digest, seed.document, seed.operation, seed.mode, build); err != nil {
			t.Fatalf("seed routing state: %v", err)
		}
		if _, err := pool.Exec(ctx,
			// build_binding is stated, not defaulted: the column is
			// nullable, and CHAOS-5484 made 'per_request' a
			// REQUIREMENT of the rule. A seed that omits it is a row
			// no rule can admit, which would make this test pass by
			// refusing everything -- for a reason that has nothing to
			// do with the route split it exists to measure.
			`INSERT INTO go_api_proof_run
			   (id, schema_digest, document_digest, selected_operation, candidate_build,
			    request_identity, stage, terminal_state, observed_at, recorded_by,
			    measurement_route, build_binding, differences_outside_baseline_defect)
			 VALUES (gen_random_uuid(),$1,$2,$3,$4,'x','deployed_executed','match', now(),'test',$5,'per_request',0)`,
			digest, seed.document, seed.operation, build, seed.route); err != nil {
			t.Fatalf("seed proof run: %v", err)
		}
	}

	rows, total, err := ReadRoutingState(ctx, instance.URI, digest)
	if err != nil {
		t.Fatalf("ReadRoutingState: %v", err)
	}
	if total != 2 {
		t.Fatalf("proof-run total = %d, want 2", total)
	}
	byOperation := map[string]OperationRow{}
	for _, row := range rows {
		byOperation[row.Operation] = row
	}
	if len(byOperation) != 2 {
		t.Fatalf("expected both operations, got %v", byOperation)
	}

	if got := byOperation["primaryOp"]; got.Proven != NoProof {
		t.Fatalf("primaryOp rendered proven=%q on PROOF-route evidence: the matrix is judging a primary row by the canary rule, so the migration-status page certifies real traffic on a measurement that never traversed the edge",
			got.Proven)
	}
	if got := byOperation["canaryOp"]; got.Proven == NoProof {
		t.Fatalf("canaryOp rendered UNPROVEN on proof-route evidence: the matrix is judging every row by the primary rule, which marks a legitimately-proven shadow or canary row unproven -- the other half of the same defect")
	}
}

// Trap #120, read back out of the database rather than out of the SQL text.
//
// The offline -routing path must name a real query;
// pinning "the statement selects the document digest" by substring turned out
// to be VACUOUS -- the join predicate mentions the same identifier, so
// deleting the output column left the substring in place and the unit test
// green (measured; the mutant survived). The column can only be pinned by
// reading a value back, so this seeds the one shape that needs it: TWO
// documents of ONE operation, with OPPOSITE proof outcomes.
//
// Every intermediate keyed on the operation alone -- the reader's rows, the
// renderer's grouping, ValidateRender's R8 duplicate key -- collapses these
// two into one, and whichever survives carries the other's proof.
func TestReadRoutingStateKeepsTwoDocumentsOfOneOperationApart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, routingMatrixDDL); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	const (
		digest    = "sha256:live"
		build     = "b18e56fa79cfe20ce0f75df148144b832d92be36"
		operation = "featureFlags"
	)
	// Same schema digest, same operation, same build. The ONLY difference
	// is the document -- and one of them has no proof at all.
	for _, seed := range []struct {
		document, mode string
		proven         bool
	}{
		{"doc-new", "canary", true},
		{"doc-old", "canary", false},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_candidate_build
			   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
			 VALUES ($1,$2,$3,$4, now())`,
			digest, seed.document, operation, build); err != nil {
			t.Fatalf("seed candidate build: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_routing_state
			   (schema_digest, document_digest, selected_operation, mode,
			    current_candidate_build, rollout_percentage, owner, updated_at)
			 VALUES ($1,$2,$3,$4,$5,100,'go', now())`,
			digest, seed.document, operation, seed.mode, build); err != nil {
			t.Fatalf("seed routing state: %v", err)
		}
		if !seed.proven {
			continue
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_proof_run
			   (id, schema_digest, document_digest, selected_operation, candidate_build,
			    request_identity, stage, terminal_state, observed_at, recorded_by,
			    measurement_route, build_binding, differences_outside_baseline_defect)
			 VALUES (gen_random_uuid(),$1,$2,$3,$4,'x','deployed_executed','match', now(),'test','proof','per_request',0)`,
			digest, seed.document, operation, build); err != nil {
			t.Fatalf("seed proof run: %v", err)
		}
	}

	rows, _, err := ReadRoutingState(ctx, instance.URI, digest)
	if err != nil {
		t.Fatalf("ReadRoutingState: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected both documents of %s, got %d rows: %+v", operation, len(rows), rows)
	}
	byDocument := map[string]OperationRow{}
	for _, row := range rows {
		if row.DocumentDigest == "" {
			t.Fatalf("row %+v came back with an EMPTY document digest: the reader is not selecting the routing key, so nothing downstream can tell these two rows apart", row)
		}
		byDocument[row.DocumentDigest] = row
	}
	if len(byDocument) != 2 {
		t.Fatalf("both rows carry document digest %v; the two documents collapsed into one", byDocument)
	}
	if got := byDocument["doc-new"]; got.Proven == NoProof {
		t.Fatal("doc-new has a deployed-executed match on its own document and rendered UNPROVEN")
	}
	if got := byDocument["doc-old"]; got.Proven != NoProof {
		t.Fatalf("doc-old has NO proof run of its own and rendered proven=%q: it is wearing doc-new's evidence, which is exactly the promotion Trap #120 authorizes", got.Proven)
	}
}

const routingMatrixDDL = `
CREATE TABLE go_api_candidate_build (
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	candidate_build TEXT NOT NULL,
	registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT pk_go_api_candidate_build
		PRIMARY KEY (schema_digest, document_digest, selected_operation, candidate_build)
);

CREATE TABLE go_api_routing_state (
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	current_candidate_build TEXT NOT NULL,
	owner TEXT NOT NULL,
	mode TEXT NOT NULL DEFAULT 'python',
	eligible_orgs JSON,
	rollout_percentage INTEGER NOT NULL DEFAULT 0,
	review_evidence TEXT,
	recorded_by TEXT,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT pk_go_api_routing_state
		PRIMARY KEY (schema_digest, document_digest, selected_operation),
	CONSTRAINT fk_go_api_routing_state_candidate_build
		FOREIGN KEY (schema_digest, document_digest, selected_operation, current_candidate_build)
		REFERENCES go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build),
	CONSTRAINT ck_go_api_routing_state_owner CHECK (owner IN ('python', 'go')),
	CONSTRAINT ck_go_api_routing_state_mode
		CHECK (mode IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
	CONSTRAINT ck_go_api_routing_state_rollout_percentage
		CHECK (rollout_percentage >= 0 AND rollout_percentage <= 100)
);

CREATE TABLE go_api_proof_run (
	id UUID NOT NULL PRIMARY KEY,
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	candidate_build TEXT NOT NULL,
	request_identity TEXT NOT NULL,
	stage TEXT NOT NULL,
	terminal_state TEXT NOT NULL,
	baseline_response_ref TEXT,
	candidate_response_ref TEXT,
	side_effect_digest TEXT,
	data_watermark TEXT,
	org_id TEXT,
	review_evidence TEXT,
	recorded_by TEXT,
	measurement_route TEXT,
	baseline_defect TEXT[],
	differences_outside_baseline_defect INTEGER NOT NULL DEFAULT 0,
	build_binding TEXT,
	observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT fk_go_api_proof_run_candidate_build
		FOREIGN KEY (schema_digest, document_digest, selected_operation, candidate_build)
		REFERENCES go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build),
	CONSTRAINT ck_go_api_proof_run_stage
		CHECK (stage IN ('dual_run', 'deployed_executed', 'shadow', 'canary')),
	CONSTRAINT ck_go_api_proof_run_terminal_state
		CHECK (terminal_state IN ('match', 'mismatch', 'auth_rejected', 'validation_rejected',
			'dependency_failed', 'timeout', 'cancelled', 'resource_exhausted',
			'fallback', 'unsupported', 'proof_failed')),
	CONSTRAINT ck_go_api_proof_run_build_binding
		CHECK (build_binding IS NULL OR build_binding IN ('per_request', 'absent')),
	CONSTRAINT ck_go_api_proof_run_shadow_requires_watermark
		CHECK (stage <> 'shadow' OR data_watermark IS NOT NULL),
	CONSTRAINT ck_go_api_proof_run_measurement_route
		CHECK (measurement_route IS NULL OR measurement_route IN ('edge', 'proof'))
);
`

// ReadRoutingState under a concurrent mode flip: ONE snapshot, always.
//
// The target-mode split briefly made this two queries on one
// connection with no enclosing transaction, partitioned by
// `WHERE (rs.mode = 'primary') = $1`. Two queries are two snapshots, so a
// row whose mode changed between them came back TWICE (primary -> canary)
// or NOT AT ALL (canary -> primary). Measured with 60 rows
// and a concurrent updater: 60 duplicate reads, 4 missing reads.
//
// That is not cosmetic. cmd/dev-health-migration-matrix assigns these rows
// straight into the render, and ValidateRender's R8-duplicate-row rule
// fails it -- on the page whose stated reason for existing is that twelve
// silently dead canary rows should not have looked like health.
//
// A concurrent `enable`/`disable` is the ORDINARY case for this page, so
// this drives exactly that: an updater flipping modes while the reader
// runs, repeatedly. Every read must return each operation exactly once.
func TestReadRoutingStateIsOneSnapshotUnderConcurrentModeFlips(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, routingMatrixDDL); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	const (
		digest = "sha256:snap"
		build  = "b18e56fa79cfe20ce0f75df148144b832d92be36"
		rows   = 60
	)
	for i := 0; i < rows; i++ {
		operation := fmt.Sprintf("op%02d", i)
		document := fmt.Sprintf("doc%02d", i)
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_candidate_build
			   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
			 VALUES ($1,$2,$3,$4, now())`, digest, document, operation, build); err != nil {
			t.Fatalf("seed candidate build: %v", err)
		}
		mode := "canary"
		if i%2 == 0 {
			mode = "primary"
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_routing_state
			   (schema_digest, document_digest, selected_operation, mode,
			    current_candidate_build, rollout_percentage, owner, updated_at)
			 VALUES ($1,$2,$3,$4,$5,100,'go', now())`,
			digest, document, operation, mode, build); err != nil {
			t.Fatalf("seed routing state: %v", err)
		}
	}

	// An updater flipping every row's mode, continuously.
	flipCtx, stopFlipping := context.WithCancel(ctx)
	defer stopFlipping()
	flipping := make(chan struct{})
	go func() {
		defer close(flipping)
		for flipCtx.Err() == nil {
			_, _ = pool.Exec(flipCtx,
				`UPDATE go_api_routing_state
				    SET mode = CASE mode WHEN 'primary' THEN 'canary' ELSE 'primary' END
				  WHERE schema_digest = $1`, digest)
		}
	}()

	for attempt := 0; attempt < 25; attempt++ {
		out, _, err := ReadRoutingState(ctx, instance.URI, digest)
		if err != nil {
			t.Fatalf("attempt %d: ReadRoutingState: %v", attempt, err)
		}
		counts := map[string]int{}
		for _, row := range out {
			counts[row.Operation]++
		}
		if len(out) != rows {
			t.Fatalf("attempt %d: read %d rows, want %d -- two unsynchronised passes return a row twice or not at all when its mode changes between them",
				attempt, len(out), rows)
		}
		for operation, n := range counts {
			if n != 1 {
				t.Fatalf("attempt %d: operation %q returned %d times -- the read is not one snapshot", attempt, operation, n)
			}
		}
	}
	stopFlipping()
	<-flipping
}

// startMatrixPostgres starts a PostgreSQL with the routing tables, for the
// tests below.
func startMatrixPostgres(t *testing.T) (context.Context, *pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, routingMatrixDDL); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return ctx, pool, instance.URI
}

func seedMatrixRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, digest, document, operation, mode, build string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
		 VALUES ($1,$2,$3,$4, now()) ON CONFLICT DO NOTHING`,
		digest, document, operation, build); err != nil {
		t.Fatalf("seed candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_routing_state
		   (schema_digest, document_digest, selected_operation, mode,
		    current_candidate_build, rollout_percentage, owner, updated_at)
		 VALUES ($1,$2,$3,$4,$5,100,'go', now())`,
		digest, document, operation, mode, build); err != nil {
		t.Fatalf("seed routing state: %v", err)
	}
}

// seedMatrixProof records an ADMISSIBLE receipt (for either mode: edge,
// bound, match) for one key, and returns its id.
func seedMatrixProof(ctx context.Context, t *testing.T, pool *pgxpool.Pool, digest, document, operation, build string) string {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
		 VALUES ($1,$2,$3,$4, now()) ON CONFLICT DO NOTHING`,
		digest, document, operation, build); err != nil {
		t.Fatalf("seed candidate build: %v", err)
	}
	var id string
	if err := pool.QueryRow(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state, observed_at, recorded_by,
		    measurement_route, build_binding, differences_outside_baseline_defect)
		 VALUES (gen_random_uuid(),$1,$2,$3,$4,'x','deployed_executed','match', now(),'test','edge','per_request',0)
		 RETURNING id::text`,
		digest, document, operation, build).Scan(&id); err != nil {
		t.Fatalf("seed proof run: %v", err)
	}
	return id
}

// Deleting `pr.candidate_build =
// rs.current_candidate_build` from the matrix's proof subquery survived the
// whole package, unit and integration suite (mutant g35). What that mutant
// does: a routing row at build B rendered PROVEN by a receipt
// recorded against build A. A proof is evidence for one immutable build;
// the page must never carry it to another.
func TestTheMatrixNeverCarriesProofAcrossBuilds(t *testing.T) {
	ctx, pool, uri := startMatrixPostgres(t)
	const (
		digest    = "sha256:live"
		document  = "doc-a"
		operation = "featureFlags"
		buildA    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		buildB    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	// The ONLY admissible receipt is for build A; the routing row is at B.
	seedMatrixProof(ctx, t, pool, digest, document, operation, buildA)
	seedMatrixRow(ctx, t, pool, digest, document, operation, "canary", buildB)

	rows, _, err := ReadRoutingState(ctx, uri, digest)
	if err != nil {
		t.Fatalf("ReadRoutingState: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %+v", rows)
	}
	if rows[0].Proven != NoProof {
		t.Fatalf("a routing row at build %s rendered proven=%q on a receipt recorded against build %s: the matrix is carrying proof across builds", buildB, rows[0].Proven, buildA)
	}

	// Control: a receipt at the row's OWN build proves it, so the refusal
	// above is the build clause and not a predicate that proves nothing.
	id := seedMatrixProof(ctx, t, pool, digest, document, operation, buildB)
	rows, _, err = ReadRoutingState(ctx, uri, digest)
	if err != nil {
		t.Fatalf("ReadRoutingState: %v", err)
	}
	if len(rows) != 1 || rows[0].Proven != id {
		t.Fatalf("the control receipt at the row's own build did not prove it: rows=%+v want proven=%s", rows, id)
	}
}

// An earlier version of the offline instruction produced a file the offline
// reader rejected. The statement -print-routing-sql prints now emits the whole
// snapshot payload, and ReadRoutingState itself reads through it. This runs
// the printed statement the way an operator's psql does -- one statement,
// one text value -- and feeds that value to ParseRoutingSnapshot, the same
// parser the -routing reader calls. The result must be ReadRoutingState's,
// row for row, including the proof total from the same snapshot.
func TestTheRoutingSnapshotStatementIsWhatTheOfflineReaderReads(t *testing.T) {
	ctx, pool, uri := startMatrixPostgres(t)
	const (
		live  = "sha256:live"
		dead  = "sha256:dead"
		build = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	)
	seedMatrixRow(ctx, t, pool, live, "doc-new", "featureFlags", "canary", build)
	seedMatrixRow(ctx, t, pool, live, "doc-old", "featureFlags", "primary", build)
	seedMatrixRow(ctx, t, pool, live, "doc-h", "hotspots", "shadow", build)
	seedMatrixRow(ctx, t, pool, dead, "doc-new", "featureFlags", "canary", build)
	proofID := seedMatrixProof(ctx, t, pool, live, "doc-new", "featureFlags", build)

	statement, err := RoutingStateSQL()
	if err != nil {
		t.Fatalf("RoutingStateSQL: %v", err)
	}
	var printed string
	if err := pool.QueryRow(ctx, statement).Scan(&printed); err != nil {
		t.Fatalf("run the printed statement: %v", err)
	}
	offline, offlineTotal, err := ParseRoutingSnapshot([]byte(printed), live)
	if err != nil {
		t.Fatalf("the offline reader rejected the printed statement's own output: %v\noutput: %s", err, printed)
	}
	online, onlineTotal, err := ReadRoutingState(ctx, uri, live)
	if err != nil {
		t.Fatalf("ReadRoutingState: %v", err)
	}
	if offlineTotal != 1 || onlineTotal != 1 {
		t.Fatalf("proof_run_total offline=%d online=%d, want 1 and 1", offlineTotal, onlineTotal)
	}
	if len(offline) != 4 || len(online) != 4 {
		t.Fatalf("want all 4 rows (live and dead) from both readers, got offline=%d online=%d", len(offline), len(online))
	}
	for i := range online {
		if online[i] != offline[i] {
			t.Fatalf("row %d differs between the live and offline readers:\n online  %+v\n offline %+v", i, online[i], offline[i])
		}
	}
	proven := 0
	for _, row := range online {
		if row.Proven != NoProof {
			proven++
			if row.Proven != proofID || row.DocumentDigest != "doc-new" || !row.Live {
				t.Fatalf("the proof landed on the wrong row: %+v (want doc-new at the live digest, proof %s)", row, proofID)
			}
		}
	}
	if proven != 1 {
		t.Fatalf("want exactly one proven row, got %d: %+v", proven, online)
	}

	// An EMPTY routing table is a real state for the live reader: zero rows,
	// the proof total still read, no error.
	if _, err := pool.Exec(ctx, `DELETE FROM go_api_routing_state`); err != nil {
		t.Fatalf("empty the routing table: %v", err)
	}
	rows, total, err := ReadRoutingState(ctx, uri, live)
	if err != nil || len(rows) != 0 || total != 1 {
		t.Fatalf("an empty routing table: rows=%d total=%d err=%v, want 0, 1, nil", len(rows), total, err)
	}
}

// The page shows the NEWEST admissible receipt for a
// row (mutant g25): `ORDER BY observed_at ASC` survived every suite; with
// two receipts the page would name the oldest -- evidence the next
// re-prove supersedes.
func TestTheMatrixShowsTheNewestAdmissibleReceipt(t *testing.T) {
	ctx, pool, uri := startMatrixPostgres(t)
	const (
		digest   = "sha256:live"
		document = "doc-a"
		op       = "featureFlags"
		build    = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	)
	seedMatrixRow(ctx, t, pool, digest, document, op, "canary", build)
	var older, newer string
	for _, c := range []struct {
		at  string
		dst *string
	}{{"2026-09-01T00:00:00Z", &older}, {"2026-09-10T00:00:00Z", &newer}} {
		if err := pool.QueryRow(ctx,
			`INSERT INTO go_api_proof_run
			   (id, schema_digest, document_digest, selected_operation, candidate_build,
			    request_identity, stage, terminal_state, observed_at, recorded_by,
			    measurement_route, build_binding, differences_outside_baseline_defect)
			 VALUES (gen_random_uuid(),$1,$2,$3,$4,'x','deployed_executed','match', $5::timestamptz,'test','edge','per_request',0)
			 RETURNING id::text`, digest, document, op, build, c.at).Scan(c.dst); err != nil {
			t.Fatalf("seed receipt: %v", err)
		}
	}
	rows, _, err := ReadRoutingState(ctx, uri, digest)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ReadRoutingState: %v %+v", err, rows)
	}
	if rows[0].Proven != newer {
		t.Fatalf("proven=%q, want the newest receipt %q (the older is %q)", rows[0].Proven, newer, older)
	}
	t.Logf("cell two admissible receipts -> page shows the newest: %v", rows[0].Proven == newer)

	// Three admissible receipts can land at ONE observed_at. The page
	// ordered by observed_at alone, so it named whichever row the plan
	// returned, while `enable` names the lowest id among the newest (its
	// ORDER BY observed_at DESC, id). One selection rule for both readers:
	// the newest, then the lowest id.
	// Inserted highest first, so a plan that returns ties in heap order
	// names the wrong one.
	for _, id := range []string{"ffffffff-ffff-4fff-8fff-ffffffffffff", "77777777-7777-4777-8777-777777777777", "00000000-0000-4000-8000-000000000000"} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_proof_run
			   (id, schema_digest, document_digest, selected_operation, candidate_build,
			    request_identity, stage, terminal_state, observed_at, recorded_by,
			    measurement_route, build_binding, differences_outside_baseline_defect)
			 VALUES ($1::uuid,$2,$3,$4,$5,'x','deployed_executed','match','2026-09-11T00:00:00Z'::timestamptz,'test','edge','per_request',0)`,
			id, digest, document, op, build); err != nil {
			t.Fatalf("seed tied receipt: %v", err)
		}
	}
	rows, _, err = ReadRoutingState(ctx, uri, digest)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ReadRoutingState: %v %+v", err, rows)
	}
	if want := "00000000-0000-4000-8000-000000000000"; rows[0].Proven != want {
		t.Fatalf("three receipts tied on observed_at: page names %q, want %q -- the receipt `enable` names (observed_at DESC, id)", rows[0].Proven, want)
	}
	t.Logf("cell three admissible receipts tied on observed_at -> page names the lowest id, as enable does: %s", rows[0].Proven)
}

// R14's printed remedy is run AS PRINTED against
// PostgreSQL. A routing row whose operation carries a quote (`x' OR ”='`)
// used to print a statement that deleted every routing
// row. It must delete exactly that row, and a backslash in a value must be
// read literally too.
func TestTheR14RemedyDeletesExactlyItsOwnRow(t *testing.T) {
	ctx, pool, uri := startMatrixPostgres(t)
	const (
		digest = "sha256:live"
		build  = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	)
	catalog, err := NewCatalog([][2]string{{"featureFlags", "doc-served"}, {"keptOperation", "doc-other"}})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	seedMatrixRow(ctx, t, pool, digest, "doc-served", "featureFlags", "canary", build)
	seedMatrixRow(ctx, t, pool, digest, "doc-other", "keptOperation", "canary", build)
	for _, op := range []string{`x' OR ''='`, `back\slash' OR true --`} {
		seedMatrixRow(ctx, t, pool, digest, "doc-unregistered", op, "primary", build)
		rows, _, err := ReadRoutingState(ctx, uri, digest)
		if err != nil {
			t.Fatalf("ReadRoutingState: %v", err)
		}
		var statement string
		for _, v := range ValidateDocumentDrift(&Render{Operations: rows}, catalog) {
			if v.Subject == op {
				_, after, _ := strings.Cut(v.Detail, "remove it by its full key -- ")
				statement, _, _ = strings.Cut(after, " -- then re-render")
			}
		}
		if statement == "" {
			t.Fatalf("%q: no R14 remedy printed", op)
		}
		var before, after int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&before)
		tag, err := pool.Exec(ctx, statement)
		if err != nil {
			t.Fatalf("%q: the printed remedy does not run: %v\n%s", op, err, statement)
		}
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&after)
		var left int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state WHERE selected_operation = $1`, op).Scan(&left)
		if tag.RowsAffected() != 1 || after != before-1 || left != 0 {
			t.Fatalf("%q: the printed remedy deleted %d row(s) (%d -> %d, %d of its own left), want exactly its own row\n%s", op, tag.RowsAffected(), before, after, left, statement)
		}
		t.Logf("cell operation %-26q printed remedy run as printed: DELETE %d (%d -> %d rows)", op, tag.RowsAffected(), before, after)
	}
}
