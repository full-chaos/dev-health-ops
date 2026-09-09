//go:build integration

package goapiproof

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
		tickets := make([]string, 0, len(v))
		for _, item := range v {
			tickets = append(tickets, item.(string))
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
