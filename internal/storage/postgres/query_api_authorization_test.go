package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// The additive write manifest is pinned entry by entry: each row is a
// privilege a saved-report mutation executes, and a row added or widened here
// is a grant a deployment then applies to query-api's role.
func TestQueryAPIWritePostureIsPinned(t *testing.T) {
	t.Parallel()
	want := []TablePrivilege{
		{"saved_reports", true, true, true},
		{"scheduled_jobs", true, true, false},
		{"report_runs", true, false, false},
		{"worker_job_outbox", true, false, false},
	}
	got := QueryAPIWritePosture()
	if !reflect.DeepEqual(got.RequiredTables, want) {
		t.Fatalf("query-api write posture tables = %+v, want %+v", got.RequiredTables, want)
	}
	if len(got.ColumnScoped) != 0 || len(got.RequiredSequences) != 0 {
		t.Fatalf("the additive posture declares only table privileges: %+v", got)
	}
}

// No deployed table is granted to query-api that another posture reserves
// exclusively for a role that must not be widened: each declared table is
// already a table of the api role's or the domain's own manifests, or is the
// mutation's own. The check that matters is the shape of the additive rule:
// no TRUNCATE, no DELETE beyond saved_reports, no scheduled_report_occurrences.
func TestQueryAPIWritePostureGrantsNothingBeyondTheMutations(t *testing.T) {
	t.Parallel()
	for _, table := range QueryAPIWritePosture().RequiredTables {
		if table.TableName == "scheduled_report_occurrences" {
			t.Errorf("%s is written only by the scheduled execution path", table.TableName)
		}
		if table.AllowDelete && table.TableName != "saved_reports" {
			t.Errorf("%s: DELETE is granted only on saved_reports", table.TableName)
		}
	}
}

func TestCheckQueryAPIWriteGrantsRefusesWithoutAPoolOrAValidRole(t *testing.T) {
	t.Parallel()
	for name, role := range map[string]string{
		"empty": "", "uppercase": "Query_API", "injection": "q; DROP TABLE x", "too long": string(make([]byte, 64)),
	} {
		if err := CheckQueryAPIWriteGrants(context.Background(), nil, role); !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: error %v, want ErrUnavailable", name, err)
		}
	}
	if err := CheckQueryAPIWriteGrants(context.Background(), nil, "query_api"); !errors.Is(err, ErrUnavailable) {
		t.Errorf("nil pool: error %v, want ErrUnavailable", err)
	}
}
