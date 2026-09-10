//go:build integration

package goapiproof

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file and tests/api/graphql/test_enablement_proof_admission_cases.py
// are the two halves of one pin.
//
// The Python preflight and this Go reader implement the SAME admission
// rule over the same table, in deliberately different SQL: Python composes
// SQLAlchemy column expressions and matches the operation/document pairs
// as a tuple-OR; this side writes raw SQL and matches them with JOIN
// unnest(...). Neither shape is incidental -- a cross-product
// "IN (...) AND ... IN (...)" would admit a proof recorded against a
// DIFFERENT registered document -- so the two statements cannot be
// compared as text, and a text pin would be pinning a proxy anyway.
//
// So they are pinned by BEHAVIOUR. Both sides drive their own PRODUCTION
// predicate over tests/fixtures/enablement_proof_admission_cases.json.
// Neither test restates the rule, so neither can pass by agreeing with
// itself, and a case the two answer differently fails on whichever side
// is wrong. Adding a case to that file changes the rule for both.

type admissionKey struct {
	SchemaDigest      string `json:"schema_digest"`
	DocumentDigest    string `json:"document_digest"`
	SelectedOperation string `json:"selected_operation"`
	CandidateBuild    string `json:"candidate_build"`
}

// admissionCase carries only what the assertion needs. The RECEIPT is
// read from the raw JSON instead (see rawAdmissionCases), because
// "measurement_route": null and an absent measurement_route are different
// cases and a typed decode cannot tell them apart.
type admissionCase struct {
	Name        string        `json:"name"`
	TargetMode  string        `json:"target_mode"`
	KeyOverride *admissionKey `json:"key_override"`
	Admits      bool          `json:"admits"`
	Why         string        `json:"why"`
}

type admissionFixture struct {
	Key   admissionKey    `json:"key"`
	Cases []admissionCase `json:"cases"`
}

func loadAdmissionFixture(t *testing.T) admissionFixture {
	t.Helper()
	// Repo-relative from internal/goapiproof. Named explicitly rather than
	// discovered so a moved fixture fails loudly instead of silently
	// running zero cases.
	path := filepath.Join("..", "..", "tests", "fixtures", "enablement_proof_admission_cases.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read admission fixture: %v", err)
	}
	var fixture admissionFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("parse admission fixture: %v", err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("admission fixture declares no cases: a pin that asserts nothing is worse than no pin, because it reads as coverage")
	}
	return fixture
}

func TestEnablementPredicateMatchesTheSharedAdmissionTable(t *testing.T) {
	ctx := context.Background()
	fixture := loadAdmissionFixture(t)
	raw := rawAdmissionCases(t)

	// ONE container for the whole table, truncated between cases. Twenty
	// separate Postgres startups pushed this package past `go test`'s
	// 10-minute default and the whole run failed on a timeout that said
	// nothing about the predicate. Isolation still holds -- each case sees
	// only its own row -- and it now costs one container rather than one
	// per case.
	pool := startRegistryPostgres(t)

	var admits, refuses int
	for i, c := range fixture.Cases {
		c, i := c, i
		t.Run(c.Name, func(t *testing.T) {
			if _, err := pool.Exec(ctx,
				`TRUNCATE go_api_proof_run, go_api_routing_state, go_api_candidate_build`); err != nil {
				t.Fatalf("truncate between cases: %v", err)
			}
			key := fixture.Key
			if c.KeyOverride != nil {
				key = mergeKey(key, *c.KeyOverride)
			}
			seedAdmissionRow(ctx, t, pool, key, mergeReceipt(t, raw[i]))

			found, err := OperationsWithEnablementProof(ctx, pool,
				fixture.Key.SchemaDigest, fixture.Key.CandidateBuild, c.TargetMode,
				map[string]string{fixture.Key.SelectedOperation: fixture.Key.DocumentDigest})
			if err != nil {
				t.Fatalf("OperationsWithEnablementProof: %v", err)
			}
			got := found[fixture.Key.SelectedOperation]
			if got != c.Admits {
				t.Fatalf("case %q: predicate returned admits=%v, fixture says %v.\nwhy: %s",
					c.Name, got, c.Admits, c.Why)
			}
		})
		if c.Admits {
			admits++
		} else {
			refuses++
		}
	}

	// A table of only-refusals would pass against a predicate that admits
	// NOTHING, and a table of only-admissions against one that admits
	// everything. Requiring both directions is what stops this test
	// holding vacuously -- the same property the admission-invariant test
	// asserts one layer down.
	if admits == 0 || refuses == 0 {
		t.Fatalf("the shared admission table must contain both admitted and refused cases (admits=%d refuses=%d): a one-sided table passes against a predicate that is constant", admits, refuses)
	}
}

// An unknown target mode must be an ERROR, never a fallthrough to the
// more permissive branch. A silent fallthrough is exactly how a promotion
// to served traffic would come to rest on measurement-only evidence.
func TestEnablementPredicateRefusesAnUnknownTargetMode(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)
	fixture := loadAdmissionFixture(t)

	for _, mode := range []string{"", "shadow", "python", "disabled", "PRIMARY"} {
		if _, err := OperationsWithEnablementProof(ctx, pool,
			fixture.Key.SchemaDigest, fixture.Key.CandidateBuild, mode,
			map[string]string{fixture.Key.SelectedOperation: fixture.Key.DocumentDigest}); err == nil {
			t.Fatalf("target mode %q was accepted: an undefined route rule must refuse, not default", mode)
		}
	}
}

// rawAdmissionCases re-reads the fixture as generic maps, because
// "measurement_route": null and an ABSENT measurement_route are different
// cases and encoding/json cannot tell a typed struct which happened.
func rawAdmissionCases(t *testing.T) []map[string]any {
	t.Helper()
	path := filepath.Join("..", "..", "tests", "fixtures", "enablement_proof_admission_cases.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read admission fixture: %v", err)
	}
	var doc struct {
		Defaults map[string]any   `json:"receipt_defaults"`
		Cases    []map[string]any `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse admission fixture: %v", err)
	}
	out := make([]map[string]any, 0, len(doc.Cases))
	for _, c := range doc.Cases {
		merged := map[string]any{}
		for k, v := range doc.Defaults {
			merged[k] = v
		}
		if override, ok := c["receipt"].(map[string]any); ok {
			for k, v := range override {
				merged[k] = v
			}
		}
		out = append(out, merged)
	}
	return out
}

type seededReceipt struct {
	stage           string
	terminalState   string
	route           any
	binding         any
	baselineDefect  any
	outsideDiffs    int
	requestIdentity string
}

func mergeReceipt(t *testing.T, m map[string]any) seededReceipt {
	t.Helper()
	str := func(k string) string {
		v, ok := m[k].(string)
		if !ok {
			t.Fatalf("fixture receipt field %q must be a string, got %#v", k, m[k])
		}
		return v
	}
	out := seededReceipt{
		stage:           str("stage"),
		terminalState:   str("terminal_state"),
		requestIdentity: str("request_identity"),
		route:           m["measurement_route"],
		binding:         m["build_binding"],
	}
	switch v := m["baseline_defect"].(type) {
	case nil:
		out.baselineDefect = nil
	case []any:
		// Kept as []any, NOT flattened to []string. A JSON `null` element
		// is a SQL NULL citation, and coercing every item with
		// `item.(string)` panicked on it -- so the loader itself could not
		// REPRESENT the case that opus r5 found promoting to primary. A
		// harness that cannot express an input cannot test it.
		tickets := make([]any, 0, len(v))
		for _, item := range v {
			switch value := item.(type) {
			case nil:
				tickets = append(tickets, nil)
			case string:
				tickets = append(tickets, value)
			default:
				t.Fatalf("fixture baseline_defect element must be a string or null, got %#v", item)
			}
		}
		out.baselineDefect = tickets
	default:
		t.Fatalf("fixture baseline_defect must be null or a list, got %#v", v)
	}
	if n, ok := m["differences_outside_baseline_defect"].(float64); ok {
		out.outsideDiffs = int(n)
	}
	return out
}

func mergeKey(base, override admissionKey) admissionKey {
	if override.SchemaDigest != "" {
		base.SchemaDigest = override.SchemaDigest
	}
	if override.DocumentDigest != "" {
		base.DocumentDigest = override.DocumentDigest
	}
	if override.SelectedOperation != "" {
		base.SelectedOperation = override.SelectedOperation
	}
	if override.CandidateBuild != "" {
		base.CandidateBuild = override.CandidateBuild
	}
	return base
}

// seedAdmissionRow inserts DIRECTLY rather than through Write.
//
// That is deliberate. Write refuses a receipt with no measurement route or
// no build binding -- correctly, because this package must never produce
// one -- but rows in those shapes EXIST in the table: every row written
// before 0128/0129 has them, and the predicate's job is to read the table
// as it really is, not as this writer would leave it.
func seedAdmissionRow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, key admissionKey, r seededReceipt) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT DO NOTHING`,
		key.SchemaDigest, key.DocumentDigest, key.SelectedOperation, key.CandidateBuild, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state, measurement_route, build_binding,
		    baseline_defect, differences_outside_baseline_defect, observed_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		uuid.New(), key.SchemaDigest, key.DocumentDigest, key.SelectedOperation, key.CandidateBuild,
		r.requestIdentity, r.stage, r.terminalState, r.route, r.binding,
		r.baselineDefect, r.outsideDiffs, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed proof run: %v", err)
	}
}

// The THIRD reader, pinned to the same table.
//
// r2 (P1) found internal/migrationmatrix carrying its own copy of this
// rule, written before CHAOS-5484 split it by target mode. Its comment
// said it was "deliberately the same predicate the enablement command
// uses" -- true when written, false the moment the rule moved, and
// nothing failed. The migration-status page rendered a primary
// proof-route receipt PROVEN while enablement refused it, and a canary
// cited mismatch UNPROVEN while enablement accepted it.
//
// The copy is gone: goapiproof.EnablementProofClause is the one source
// and the matrix composes it. This drives that clause over the SAME table
// the two implementations above are pinned to, in the matrix's own query
// SHAPE -- a correlated subquery per routing row, not a JOIN unnest --
// so a change that works in one shape and not the other fails here.
//
// A shared truth table pins the implementations it knows about. It cannot
// pin one nobody enumerated, which is why the PR body now carries the
// enumerated reader list rather than a claim about how many there are.
func TestTheMigrationMatrixClauseMatchesTheSharedAdmissionTable(t *testing.T) {
	ctx := context.Background()
	fixture := loadAdmissionFixture(t)
	raw := rawAdmissionCases(t)
	pool := startRegistryPostgres(t)

	for i, c := range fixture.Cases {
		c, i := c, i
		t.Run(c.Name, func(t *testing.T) {
			if _, err := pool.Exec(ctx,
				`TRUNCATE go_api_proof_run, go_api_routing_state, go_api_candidate_build`); err != nil {
				t.Fatalf("truncate between cases: %v", err)
			}
			key := fixture.Key
			if c.KeyOverride != nil {
				key = mergeKey(key, *c.KeyOverride)
			}
			seedAdmissionRow(ctx, t, pool, key, mergeReceipt(t, raw[i]))

			clause, err := EnablementProofClause("pr", c.TargetMode)
			if err != nil {
				t.Fatalf("EnablementProofClause(%q): %v", c.TargetMode, err)
			}

			// The matrix's SHAPE: a correlated subquery keyed on the
			// routing row's own four columns, selecting a proof id.
			var proofID *string
			err = pool.QueryRow(ctx, `
				SELECT (
				  SELECT pr.id::text
				    FROM go_api_proof_run pr
				   WHERE pr.schema_digest = $1
				     AND pr.document_digest = $2
				     AND pr.selected_operation = $3
				     AND pr.candidate_build = $4
				     AND `+clause+`
				   ORDER BY pr.observed_at DESC
				   LIMIT 1
				)`,
				fixture.Key.SchemaDigest, fixture.Key.DocumentDigest,
				fixture.Key.SelectedOperation, fixture.Key.CandidateBuild,
			).Scan(&proofID)
			if err != nil {
				t.Fatalf("matrix-shaped query: %v", err)
			}

			got := proofID != nil && *proofID != ""
			if got != c.Admits {
				t.Fatalf("case %q: the migration matrix would render proven=%v, the admission table says %v.\nwhy: %s\nA status page disagreeing with the command that enforces the rule is the shape r2 found.",
					c.Name, got, c.Admits, c.Why)
			}
		})
	}
}

// The FULL input surface, not a sample.
//
// The shared table above is 20 chosen cases. Chosen cases are a sample,
// and a sample is exactly what r1 and r2 kept finding gaps in -- an
// instrument that enumerates only the inputs its author thought of. This
// enumerates every value the predicate can SEE and asserts the rule over
// the whole cross-product, with `want` DERIVED from the rule as stated in
// prose rather than listed per case.
//
// What the predicate reads, and nothing else:
//   - stage                                 (all 4 legal values)
//   - terminal_state                        (all 11 legal values)
//   - measurement_route                     (edge, proof, NULL)
//   - differences_outside_baseline_defect   (0, 1)
//   - baseline_defect                       (NULL, empty, non-empty)
//   - the target mode                       (canary, primary)
//
// build_binding IS in that list now. It was not when this test was
// written -- the predicate did not read the column, and this comment said
// so -- and opus r5 (P2) showed what that cost: every row the OLD writer
// produced with mismatch/edge/outside=0/cited became primary-admissible
// under the new counting rules. The predicate now requires per_request on
// both arms, so the binding dimension is load-bearing rather than a
// control.
//
// The mutant this test kills that the 20 cases do NOT, measured both ways
// rather than argued from the design: admitting a NULL measurement_route
// for the MISMATCH branch only.
//
//	routeClause = "(p.measurement_route IS NOT NULL OR p.terminal_state = 'mismatch')"
//
// Both fixture-driven tests PASS under that mutation; this one fails with
// `terminal=mismatch route=<nil> ... predicate says true, the rule says
// false`. That is a receipt with NO recorded provenance authorizing a
// promotion -- the "any route is not no route" rule 0128 exists to
// enforce. The 20 cases miss it because they cover 8 of the 33
// terminal-state x route pairs and `mismatch x NULL` is not among them,
// so the NULL-route rule is only ever exercised on the `match` branch.
//
// (An earlier version of this comment claimed the fixture would not catch
// a predicate that started reading build_binding. It does -- both Go
// readers fail. The claim was made from this test's design instead of
// from running it, which is the same defect class as a comment asserting
// two implementations are "deliberately the same predicate".)
func TestTheEnablementRuleOverItsWholeInputSurface(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	// astra r4 P3: stage was FIXED at deployed_executed, so mutating the
	// predicate to accept stage='canary' or stage='shadow' survived this
	// test entirely. A dimension held constant is a dimension unasserted,
	// which is the same "enumerates only the inputs its author chose"
	// failure this test was written to end.
	allStages := []string{"dual_run", "deployed_executed", "shadow", "canary"}
	allTerminalStates := []string{
		"match", "mismatch", "auth_rejected", "validation_rejected",
		"dependency_failed", "timeout", "cancelled", "resource_exhausted",
		"fallback", "unsupported", "proof_failed",
	}
	routes := []any{RouteEdge, RouteProof, nil}
	bindings := []any{EdgeBuildPresent, EdgeBuildAbsent, nil}
	outsides := []int{0, 1}
	// The defect-shape dimension gained three cells when enumerating the
	// writer's input domain found that `cardinality(ARRAY[''])` is 1, so a
	// citation naming nothing read as fully cited. A shape the predicate
	// must reject belongs in the dimension, not in a separate test.
	// opus r5 (P1): this dimension held only the space character, so a
	// tab / newline / CR / VT / FF / NBSP / SQL-NULL citation passed both
	// predicates while the writer refused it -- and the reviewer showed
	// that adding ONE cell here, with no production change, fails the
	// test in its own words. Every character of the shared cutset is a
	// cell now, plus a SQL NULL element and a NULL mixed with a real
	// ticket, plus two Unicode spaces OUTSIDE the cutset which both
	// engines must treat as real citations.
	blankTicket := []any{"", " ", "\t", "\n", "\r", "\v", "\f", "\u00a0"}
	defects := []any{
		nil, []string{}, []string{"CHAOS-5448"},
		[]string{"CHAOS-5448", ""},
		// a SQL NULL element, and one beside a real ticket
		[]any{nil}, []any{"CHAOS-5448", nil},
		// outside the cutset: REAL citations, both engines
		[]string{"\u2028"}, []string{"\u3000"},
	}
	for _, blank := range blankTicket {
		defects = append(defects, []string{blank.(string)})
	}
	modes := []string{TargetModeCanary, TargetModePrimary}

	// The rule, in one place, derived rather than tabulated.
	want := func(stage, terminal string, route any, binding any, outside int, defect any, mode string) bool {
		if stage != EnablementProofStage {
			return false
		}
		// The binding is part of the rule now, on both arms. A NULL is a
		// pre-0129 row written under different counting semantics; an
		// `absent` row is one this writer would have downgraded or
		// counted. Neither is proof (opus r5, P2).
		if binding != EdgeBuildPresent {
			return false
		}
		switch mode {
		case TargetModePrimary:
			if route != RouteEdge {
				return false
			}
		default:
			if route == nil {
				return false
			}
		}
		switch terminal {
		case EnablementProofTerminalState:
			return true
		case EnablementCitedMismatchState:
			if outside != 0 {
				return false
			}
			// The rule uses the SHARED cutset, never strings.TrimSpace:
			// deriving `want` from a definition the predicate does not
			// use is exactly how the whitespace gap survived this test.
			switch cited := defect.(type) {
			case []string:
				if len(cited) == 0 {
					return false
				}
				for _, ticket := range cited {
					if strings.Trim(ticket, blankCitationCutset) == "" {
						return false
					}
				}
				return true
			case []any:
				if len(cited) == 0 {
					return false
				}
				for _, ticket := range cited {
					text, isText := ticket.(string)
					if !isText || strings.Trim(text, blankCitationCutset) == "" {
						return false // a SQL NULL element names nothing
					}
				}
				return true
			default:
				return false
			}
		default:
			return false
		}
	}

	key := admissionKey{
		SchemaDigest:      "sha256:surface",
		DocumentDigest:    "doc-surface",
		SelectedOperation: "featureFlags",
		CandidateBuild:    "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
		 VALUES ($1,$2,$3,$4, now()) ON CONFLICT DO NOTHING`,
		key.SchemaDigest, key.DocumentDigest, key.SelectedOperation, key.CandidateBuild,
	); err != nil {
		t.Fatalf("register the candidate build: %v", err)
	}

	combinations := 0
	for _, stage := range allStages {
		for _, terminal := range allTerminalStates {
			for _, route := range routes {
				for _, binding := range bindings {
					for _, outside := range outsides {
						for _, defect := range defects {
							// ck_go_api_proof_run_shadow_requires_watermark:
							// a shadow-stage row without one is not a row
							// the database accepts, so it is not part of
							// the input surface. Supplying it keeps
							// `shadow` IN the enumeration rather than
							// dropping a stage because one combination
							// could not be built.
							var watermark any
							if stage == "shadow" {
								watermark = "2026-09-01"
							}
							if _, err := pool.Exec(ctx, `TRUNCATE go_api_proof_run`); err != nil {
								t.Fatalf("truncate: %v", err)
							}
							if _, err := pool.Exec(ctx,
								`INSERT INTO go_api_proof_run
							   (id, schema_digest, document_digest, selected_operation,
							    candidate_build, request_identity, stage, terminal_state,
							    observed_at, org_id, recorded_by, measurement_route,
							    build_binding, differences_outside_baseline_defect,
							    baseline_defect, data_watermark)
							 VALUES ($1,$2,$3,$4,$5,'surface',$6,$7, now(),'70d529e0','surface',$8,$9,$10,$11,$12)`,
								uuid.New(), key.SchemaDigest, key.DocumentDigest, key.SelectedOperation,
								key.CandidateBuild, stage, terminal,
								route, binding, outside, defect, watermark,
							); err != nil {
								t.Fatalf("seed %s/%v/%v/%d/%v: %v", terminal, route, binding, outside, defect, err)
							}

							for _, mode := range modes {
								combinations++
								found, err := OperationsWithEnablementProof(ctx, pool,
									key.SchemaDigest, key.CandidateBuild, mode,
									map[string]string{key.SelectedOperation: key.DocumentDigest})
								if err != nil {
									t.Fatalf("read: %v", err)
								}
								got := found[key.SelectedOperation]
								expected := want(stage, terminal, route, binding, outside, defect, mode)
								if got != expected {
									t.Fatalf("stage=%s terminal=%s route=%v binding=%v outside=%d defect=%v mode=%s: predicate says %v, the rule says %v",
										stage, terminal, route, binding, outside, defect, mode, got, expected)
								}
							}
						}
					}
				}
			}
		}
	}

	// 4 stages x 11 terminal states x 3 routes x 3 bindings x 2 outside x
	// len(defects) shapes x 2 modes, asserted from the slice itself so
	// the number cannot go stale the way the three hand-written copies of
	// it did (opus r5, P3).
	if combinations != 4*11*3*3*2*len(defects)*2 {
		t.Fatalf("exercised %d combinations, expected the full cross-product of %d", combinations, 4*11*3*3*2*len(defects)*2)
	}
}

// astra r4 P3: every case above asks about ONE operation/document pair,
// so deleting the operation equality from `JOIN unnest(...)` survived.
//
// With one pair, matching on the document alone still returns the right
// answer -- there is nothing else to confuse it with. The defect only
// shows with TWO pairs: a proof recorded for (operationA, documentB) must
// not answer a request that names (operationA, documentA) and
// (operationB, documentB). Cross-matching admits it, and the operation
// comes back proven on a receipt recorded against a document it does not
// use.
//
// This is the multi-pair half of the surface. The cross-product above is
// the single-pair half; neither is sufficient alone.
func TestTheProofKeyPairsOperationWithItsOwnDocument(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	const (
		digest = "sha256:pairs"
		build  = "b18e56fa79cfe20ce0f75df148144b832d92be36"
		opA    = "featureFlags"
		opB    = "hotspots"
		docA   = "doc-a"
		docB   = "doc-b"
	)

	// The ONLY receipt: operation A measured against document B. Neither
	// requested pair is (opA, docB), so nothing may be proven.
	//
	// build_binding is stated on BOTH seeds here (opus r5, P1 made a bound
	// measurement part of the rule). It matters most for the CONTROL at
	// the end: without it the control row is refused for its binding, and
	// the refusals above would then be indistinguishable from a predicate
	// that admits nothing at all -- which is the exact failure mode this
	// control was written to rule out. It caught it.
	for _, pair := range [][2]string{{opA, docB}, {opA, docA}, {opB, docB}} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO go_api_candidate_build
			   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
			 VALUES ($1,$2,$3,$4, now()) ON CONFLICT DO NOTHING`,
			digest, pair[1], pair[0], build); err != nil {
			t.Fatalf("register %v: %v", pair, err)
		}
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state, observed_at, org_id, recorded_by,
		    measurement_route, build_binding, differences_outside_baseline_defect)
		 VALUES ($1,$2,$3,$4,$5,'pairs',$6,$7, now(),'70d529e0','pairs','edge','per_request',0)`,
		uuid.New(), digest, docB, opA, build,
		EnablementProofStage, EnablementProofTerminalState,
	); err != nil {
		t.Fatalf("seed the cross-pair receipt: %v", err)
	}

	found, err := OperationsWithEnablementProof(ctx, pool, digest, build,
		TargetModeCanary, map[string]string{opA: docA, opB: docB})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}

	if found[opA] {
		t.Fatalf("%s came back proven: the only receipt is against document %q and the request asked about %q -- the predicate matched the operation to another pair's document",
			opA, docB, docA)
	}
	if found[opB] {
		t.Fatalf("%s came back proven: the only receipt names operation %q -- the predicate matched the document to another pair's operation",
			opB, opA)
	}

	// Control: the SAME shape with the receipt on a requested pair must
	// prove, or this test would also pass with the predicate returning
	// nothing at all.
	if _, err := pool.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state, observed_at, org_id, recorded_by,
		    measurement_route, build_binding, differences_outside_baseline_defect)
		 VALUES ($1,$2,$3,$4,$5,'pairs',$6,$7, now(),'70d529e0','pairs','edge','per_request',0)`,
		uuid.New(), digest, docA, opA, build,
		EnablementProofStage, EnablementProofTerminalState,
	); err != nil {
		t.Fatalf("seed the matching receipt: %v", err)
	}
	found, err = OperationsWithEnablementProof(ctx, pool, digest, build,
		TargetModeCanary, map[string]string{opA: docA, opB: docB})
	if err != nil {
		t.Fatalf("OperationsWithEnablementProof: %v", err)
	}
	if !found[opA] {
		t.Fatal("the matching pair did not prove: the predicate refuses what it should admit, which would make the assertions above pass for the wrong reason")
	}
	if found[opB] {
		t.Fatalf("%s proved off %s's receipt", opB, opA)
	}
}

// The two engines agree on "names nothing", character by character.
//
// opus r5 (P1): the change shipped TWO definitions -- the writer's
// strings.TrimSpace and the predicates' one-argument btrim() -- which
// disagree on every whitespace character except the space itself. A tab,
// newline, CR, VT, FF or NBSP citation was refused by the writer and
// ADMITTED by both predicates, and promoted to primary through the
// shipped CLI.
//
// They cannot share Unicode's definition: measured on this Postgres, the
// explicit cutset strips NBSP but not U+2028, POSIX [[:space:]] is the
// exact reverse, and unicode.IsSpace strips both. So the shared rule is
// ENUMERATED, the SQL definition is the authority, and Go implements it.
//
// This asserts that, per character, INCLUDING characters outside the set:
// there, both sides must agree the value is a REAL citation. A test that
// only checked the in-set characters would pass with Go still using
// TrimSpace, which is how the original gap survived.
func TestTheBlankDefinitionIsIdenticalInBothEngines(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	for _, c := range []struct {
		name  string
		value string
		blank bool // the shared rule says this "names nothing"
	}{
		{"space", " ", true},
		{"tab", "\t", true},
		{"newline", "\n", true},
		{"carriage return", "\r", true},
		{"vertical tab", "\v", true},
		{"form feed", "\f", true},
		{"NBSP", " ", true},
		{"all of them together", " \t\n\r\v\f ", true},
		// OUTSIDE the set on purpose: both sides must call these real.
		{"line separator U+2028", " ", false},
		{"ideographic space U+3000", "　", false},
		{"a real ticket", "CHAOS-5448", false},
		{"a ticket with padding", "  CHAOS-5448  ", false},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			goSaysBlank := strings.Trim(c.value, blankCitationCutset) == ""

			var sqlSaysBlank bool
			if err := pool.QueryRow(ctx,
				`SELECT btrim($1, `+blankCitationSQL()+`) = ''`, c.value,
			).Scan(&sqlSaysBlank); err != nil {
				t.Fatalf("ask Postgres: %v", err)
			}

			if goSaysBlank != sqlSaysBlank {
				t.Fatalf("the two engines DISAGREE on %q: Go says blank=%v, Postgres says blank=%v -- two definitions of \"names nothing\" is the defect class this PR exists to fix",
					c.value, goSaysBlank, sqlSaysBlank)
			}
			if goSaysBlank != c.blank {
				t.Fatalf("%q: both engines say blank=%v, the shared rule says %v", c.value, goSaysBlank, c.blank)
			}
		})
	}
}
