//go:build integration

package migrationmatrix

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
)

// ReadRESTProof, DRIVEN against a real PostgreSQL -- the same discipline
// internal/goapiproof's TestWrittenReceiptSatisfiesTheEnablementPredicate
// applies to the GraphQL reader (OperationsWithEnablementProof), so this
// reader's SQL, not a reconstruction of it, is what the test result
// depends on. registryschema.DDL (shared with internal/goapiproof's own
// integration suite and cmd/go-api-routing's) already mirrors alembic
// 0134's go_api_rest_proof_run table, so this test needs no DDL of its
// own.
func TestReadRESTProofAppliesEnablementProofClause(t *testing.T) {
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
	if _, err := pool.Exec(ctx, registryschema.DDL); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	const build = "b18e56fa79cfe20ce0f75df148144b832d92be36"

	seedProofRun := func(method, path, terminalState, measurementRoute, buildBinding string, differencesOutside int) {
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_rest_proof_run
			   (id, method, path, candidate_build,
			    request_identity, stage, terminal_state, measurement_route,
			    baseline_defect, differences_outside_baseline_defect, build_binding, observed_at)
			 VALUES (gen_random_uuid(),$1,$2,$3,'req','deployed_executed',$4,$5,'{}',$6,$7, now())`,
			method, path, build, terminalState, measurementRoute, differencesOutside, buildBinding); err != nil {
			t.Fatalf("seed proof run for %s %s: %v", method, path, err)
		}
	}

	seedProofRun(http.MethodGet, "/api/v1/quadrant", "match", "proof", "per_request", 0)

	// Recorded with NO build binding -- the pre-0129-equivalent shape
	// EnablementProofClause deliberately excludes (build_binding IS NULL).
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_rest_proof_run
		   (id, method, path, candidate_build,
		    request_identity, stage, terminal_state, measurement_route,
		    baseline_defect, differences_outside_baseline_defect, observed_at)
		 VALUES (gen_random_uuid(),$1,$2,$3,'req','deployed_executed','match','proof','{}',0, now())`,
		http.MethodGet, "/api/v1/filters/options", build); err != nil {
		t.Fatalf("seed unbound proof run: %v", err)
	}

	seedProofRun(http.MethodPost, "/api/v1/drilldown/prs", "mismatch", "proof", "per_request", 1)

	proven, err := ReadRESTProof(ctx, instance.URI, build)
	if err != nil {
		t.Fatalf("ReadRESTProof: %v", err)
	}

	admissibleOp := RESTOperationName(http.MethodGet, "/api/v1/quadrant")
	unboundOp := RESTOperationName(http.MethodGet, "/api/v1/filters/options")
	mismatchOp := RESTOperationName(http.MethodPost, "/api/v1/drilldown/prs")

	if _, ok := proven[admissibleOp]; !ok {
		t.Errorf("proven does not include %s, which recorded a clean deployed_executed match with a bound build: %v", admissibleOp, proven)
	}
	if _, ok := proven[unboundOp]; ok {
		t.Errorf("proven includes %s, whose receipt carried no build_binding -- EnablementProofClause must exclude it: %v", unboundOp, proven)
	}
	if _, ok := proven[mismatchOp]; ok {
		t.Errorf("proven includes %s, whose receipt was an uncited mismatch: %v", mismatchOp, proven)
	}
}
