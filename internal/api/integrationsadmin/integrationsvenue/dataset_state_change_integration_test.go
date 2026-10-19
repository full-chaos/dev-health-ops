//go:build integration

package integrationsvenue

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestDatasetStateChangeVenueOracle pins CHAOS-10242: the dataset routes serve
// state_changed_at and state_changed_by. A seeded row has neither; a switch
// stamps the dataset endpoint's writer name and a time, and a later read
// serves the same stamp.
func TestDatasetStateChangeVenueOracle(t *testing.T) {
	ctx := context.Background()
	const jwtKey = "venue-oracle-test-secret-key-for-datasets-32b!"
	v := newIDs()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: repoRoot(t), GoOnly: true, JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			seed(t, ctx, admin, v)
			return map[string]map[string]any{
				"adminA": {"user_id": v.adminA.String(), "email": "admin-a@example.com", "org_id": v.orgA.String(), "role": "admin"},
			}
		},
	})
	base := startGoServer(t, ctx, venue, jwtKey, nil)
	path := integrationsPath + "/" + v.intGitHub.String() + "/datasets"
	call := func(name, method, body string) map[string]map[string]any {
		t.Helper()
		headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["adminA"], "Content-Type": "application/json"}
		request := venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers}
		if body != "" {
			request.Body = venueoracle.B64(body)
		}
		response := venueoracle.Do(t, base, request)
		if response.Status != http.StatusOK {
			t.Fatalf("%s: status %d: %s", name, response.Status, response.Body)
		}
		var rows []map[string]any
		if err := json.Unmarshal([]byte(response.Body), &rows); err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		byKey := map[string]map[string]any{}
		for _, row := range rows {
			byKey[row["dataset_key"].(string)] = row
		}
		return byKey
	}

	seeded := call("seeded rows", http.MethodGet, "")
	for _, key := range []string{"commits", "prs"} {
		row, ok := seeded[key]
		if !ok {
			t.Fatalf("seeded rows lack %s: %v", key, seeded)
		}
		for _, field := range []string{"state_changed_at", "state_changed_by"} {
			value, present := row[field]
			if !present || value != nil {
				t.Errorf("seeded %s: %s = %v (present %v), want null", key, field, value, present)
			}
		}
	}

	switched := call("switch both", http.MethodPatch, `{"datasets":[{"dataset_key":"commits","is_enabled":false},{"dataset_key":"prs","is_enabled":true}]}`)
	read := call("read back", http.MethodGet, "")
	for _, key := range []string{"commits", "prs"} {
		row := switched[key]
		if row["state_changed_by"] != providersync.DatasetWriterDatasetEndpoint {
			t.Errorf("switched %s: state_changed_by = %v, want %s", key, row["state_changed_by"], providersync.DatasetWriterDatasetEndpoint)
		}
		if at, _ := row["state_changed_at"].(string); at == "" {
			t.Errorf("switched %s: state_changed_at = %v, want a time", key, row["state_changed_at"])
		}
		for _, field := range []string{"state_changed_at", "state_changed_by"} {
			if read[key][field] != row[field] {
				t.Errorf("read back %s: %s = %v, the switch answered %v", key, field, read[key][field], row[field])
			}
		}
	}

	venueoracle.WriteGoOnlyProof(t, "the dataset routes serve state_changed_at and state_changed_by: null on a seeded row, the dataset endpoint's name and a time after a switch, the same stamp on a later read")
}
