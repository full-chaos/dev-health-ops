//go:build integration

package goapiproof

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-6807: canary and primary are both "on for every authenticated org".
// status must say so for every reachable row that records a control no plane
// obeys, and stay silent for a row that makes no such claim.
func TestRoutingStatusRowsFlagsTheControlsNoPlaneObeys(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	type seeded struct {
		operation, mode string
		rollout         int
		eligible        string // SQL literal for the json column
		want            []string
	}
	rows := []seeded{
		{"cleanCanary", "canary", 100, "NULL", []string{}},
		{"emptyList", "canary", 100, "'[]'::json", []string{}},
		{"emptyObject", "canary", 100, "'{}'::json", []string{}},
		{"jsonNull", "canary", 100, "'null'::json", []string{}},
		// A json column keeps insignificant whitespace: these are as empty as
		// their compact forms.
		{"spacedEmptyList", "canary", 100, "'[ ]'::json", []string{}},
		{"newlineEmptyList", "canary", 100, "E'[\\n]'::json", []string{}},
		{"spacedEmptyObject", "canary", 100, "'{ }'::json", []string{}},
		{"spacedNull", "canary", 100, "' null '::json", []string{}},
		{"partialRollout", "canary", 50, "NULL", []string{"rollout_percentage=50"}},
		{"zeroRollout", "canary", 0, "NULL", []string{"rollout_percentage=0"}},
		{"allowlist", "canary", 100, `'["org-a"]'::json`, []string{`eligible_orgs=["org-a"]`}},
		{"primaryBoth", "primary", 60, `'["org-b"]'::json`, []string{"rollout_percentage=60", `eligible_orgs=["org-b"]`}},
		// Not reachable: the row makes no serving claim, so nothing to flag.
		{"pythonPartial", "python", 50, `'["org-c"]'::json`, []string{}},
		{"shadowPartial", "shadow", 10, `'["org-d"]'::json`, []string{}},
		{"disabledZero", "disabled", 0, `'["org-e"]'::json`, []string{}},
	}
	catalog := map[string]string{}
	for _, row := range rows {
		catalog[row.operation] = testDocumentDigest
		if _, err := pool.Exec(ctx, registerCandidateBuildSQL, testSchemaDigest, testDocumentDigest, row.operation, testCandidateBuild); err != nil {
			t.Fatalf("register %s: %v", row.operation, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build,
				 owner, mode, rollout_percentage, eligible_orgs, review_evidence, recorded_by)
			VALUES ($1, $2, $3, $4, 'go', $5, $6, `+row.eligible+`, 'seeded', 'test')`,
			testSchemaDigest, testDocumentDigest, row.operation, testCandidateBuild, row.mode, row.rollout); err != nil {
			t.Fatalf("seed %s: %v", row.operation, err)
		}
	}

	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest, "", catalog)
	if err != nil {
		t.Fatalf("RoutingStatusRows: %v", err)
	}
	got := map[string][]string{}
	for _, status := range statuses {
		got[status.Operation] = status.UnenforcedControls()
	}
	for _, row := range rows {
		if !reflect.DeepEqual(got[row.operation], row.want) {
			t.Errorf("%s (%s, rollout %d, eligible %s): flagged %v, want %v", row.operation, row.mode, row.rollout, row.eligible, got[row.operation], row.want)
		}
	}
}

// A partial rollout is refused, not recorded, and nothing is written.
func TestEnableRefusesARolloutNoPlaneObeysAndWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	for _, rollout := range []int{0, 1, 50, 99} {
		request := enableRequest("featureFlags")
		request.RolloutPercentage = rollout
		_, err := Enable(ctx, pool, request)
		if !errors.Is(err, ErrEnableRequestRefused) || !strings.Contains(err.Error(), "neither plane enforces") {
			t.Fatalf("Enable rollout %d = %v, want the named not-enforced refusal", rollout, err)
		}
		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("a refused enable at rollout %d wrote %d row(s)", rollout, rows)
		}
	}
}
