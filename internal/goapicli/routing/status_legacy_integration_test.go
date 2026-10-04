//go:build integration

package routing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// CHAOS-8649, end to end: `status` reads the catalog's legacy entries, so a row carried under a registered
// legacy digest reads MATCH and reachable with its own mode and build, and a row under a digest the catalog
// does not register for its operation stays unreachable.
func TestStatusCountsARowUnderARegisteredLegacyDigestAsServed(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := t.Context()
	live := localSchemaDigest()
	const (
		legacyOp    = "capacityForecast"
		strayOp     = "hotspots"
		legacyBuild = "2222222222222222222222222222222222222222"
	)
	current := strings.Repeat("a", 64)
	legacyDigest := strings.Repeat("b", 64)
	strayCurrent := strings.Repeat("c", 64)
	stray := strings.Repeat("d", 64)

	catalog, err := json.Marshal([]map[string]any{
		{"operation": legacyOp, "digest": current},
		{"operation": legacyOp, "digest": legacyDigest, "legacy": true},
		{"operation": strayOp, "digest": strayCurrent},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(t.TempDir(), "go_api_operations.json")
	if err := os.WriteFile(catalogPath, catalog, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ operation, document string }{{legacyOp, legacyDigest}, {strayOp, stray}} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4)`, live, row.document, row.operation, legacyBuild); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build,
				 owner, mode, rollout_percentage, review_evidence, recorded_by)
			VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'carried', 'operator')`,
			live, row.document, row.operation, legacyBuild); err != nil {
			t.Fatal(err)
		}
	}
	server := startQueryAPI(t, live, map[string]string{legacyOp: current, strayOp: strayCurrent})
	registry := server.URL + "/registry"

	status := catalogRuleStatus(t, dsn, catalogPath, registry)
	got := status[legacyOp]
	for key, want := range map[string]any{
		"digest_state": "MATCH", "document_class": "legacy", "row_document_digest": legacyDigest, "document_digest": current,
		"mode": "canary", "current_candidate_build": legacyBuild, "reachable": true, "reachable_reason": nil,
	} {
		if !reflect.DeepEqual(got[key], want) {
			t.Errorf("%s %s = %v, want %v", legacyOp, key, got[key], want)
		}
	}
	if unreachable, _ := got["unreachable_document_digests"].([]any); len(unreachable) != 0 {
		t.Errorf("%s unreachable_document_digests = %v, want []", legacyOp, unreachable)
	}
	got = status[strayOp]
	for key, want := range map[string]any{
		"digest_state": "STALE", "document_class": nil, "row_document_digest": nil, "mode": nil, "reachable": false,
		"unreachable_document_digests": []any{stray}, "accepted_document_digests": []any{},
	} {
		if !reflect.DeepEqual(got[key], want) {
			t.Errorf("%s %s = %v, want %v", strayOp, key, got[key], want)
		}
	}

	text, _, err := captureVerb(t, "status", "-postgres-uri", dsn, "-catalog", catalogPath, "-registry-url", registry)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "LEGACY document digest the catalog registers for this operation (served alike, CHAOS-8000 dual accept): "+legacyDigest) ||
		!strings.Contains(text, "the edge can never reach, document digest: ["+stray+"]") {
		t.Errorf("status text:\n%s", text)
	}
}
