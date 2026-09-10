//go:build integration

package migrationmatrix

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadRoutingState, DRIVEN -- not reconstructed.
//
// astra r3 (P3) found the pin written for r2's third-reader finding
// executing the matrix's query SHAPE against a container while never
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
// opus r5 (P2c) asked for the offline -routing path to name a real query;
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
// opus r5 (P2): the target-mode split briefly made this two queries on one
// connection with no enclosing transaction, partitioned by
// `WHERE (rs.mode = 'primary') = $1`. Two queries are two snapshots, so a
// row whose mode changed between them came back TWICE (primary -> canary)
// or NOT AT ALL (canary -> primary). Measured by the reviewer with 60 rows
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
