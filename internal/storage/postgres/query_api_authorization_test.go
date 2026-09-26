package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// The full manifest is pinned entry by entry: each row is a privilege a
// query-api code path executes (see queryAPIPosture's doc comment for the
// statement behind each), and a row added or widened here is a grant a
// deployment then applies to query-api's role.
func TestQueryAPIPostureIsPinned(t *testing.T) {
	t.Parallel()
	want := []TablePrivilege{
		{"saved_reports", true, true, true},
		{"scheduled_jobs", true, true, false},
		{"report_runs", true, false, false},
		{"worker_job_outbox", true, false, false},
		{"go_api_routing_state", false, false, false},
		{"sync_configurations", false, false, false},
		{"job_runs", false, false, false},
		{"sync_runs", false, false, false},
		{"organizations", false, false, false},
		{"org_licenses", false, false, false},
		{"feature_flags", false, false, false},
		{"org_feature_overrides", false, false, false},
		{"settings", false, false, false},
	}
	got := QueryAPIPosture()
	if !reflect.DeepEqual(got.RequiredTables, want) {
		t.Fatalf("query-api posture tables = %+v, want %+v", got.RequiredTables, want)
	}
	if len(got.ColumnScoped) != 0 || len(got.RequiredSequences) != 0 {
		t.Fatalf("the query-api posture declares only table privileges: %+v", got)
	}
}

// The shape rules of the manifest: writes exist only for the saved-report
// mutations, DELETE only on saved_reports, no relation twice, and nothing the
// scheduled execution path or the proof CLI owns.
func TestQueryAPIPostureGrantsNoWriteBeyondTheMutations(t *testing.T) {
	t.Parallel()
	writers := map[string]bool{"saved_reports": true, "scheduled_jobs": true, "report_runs": true, "worker_job_outbox": true}
	seen := map[string]bool{}
	for _, table := range QueryAPIPosture().RequiredTables {
		if seen[table.TableName] {
			t.Errorf("%s is declared twice", table.TableName)
		}
		seen[table.TableName] = true
		switch table.TableName {
		case "scheduled_report_occurrences", "go_api_candidate_build", "go_api_proof_run":
			t.Errorf("%s is not reached by query-api at runtime", table.TableName)
		}
		if (table.AllowInsert || table.AllowUpdate || table.AllowDelete) && !writers[table.TableName] {
			t.Errorf("%s: a write is granted on a read-only relation", table.TableName)
		}
		if table.AllowDelete && table.TableName != "saved_reports" {
			t.Errorf("%s: DELETE is granted only on saved_reports", table.TableName)
		}
	}
}

func TestCheckQueryAPIAuthorizationRefusesWithoutAPoolOrAValidRole(t *testing.T) {
	t.Parallel()
	for name, role := range map[string]string{
		"empty": "", "uppercase": "Query_API", "injection": "q; DROP TABLE x", "too long": string(make([]byte, 64)),
	} {
		if err := CheckQueryAPIAuthorization(context.Background(), nil, role, "river"); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: error %v, want ErrUnavailable", name, err)
		}
	}
	if err := CheckQueryAPIAuthorization(context.Background(), nil, "query_api", "river"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("nil pool: error %v, want ErrUnavailable", err)
	}
}
