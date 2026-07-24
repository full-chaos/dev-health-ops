package domaingrants

import "testing"

// TestLoadGroundTruth_MatchesKnownShapeOnMain pins the ground-truth parser
// against the current, hand-maintained state of migrate.go and
// domain_authorization.go on this branch (origin/main plus the
// grantcheck_export.go shims). If either hand-maintained file changes
// shape, this test should be the first thing that tells you the parser
// needs updating -- NOT a silent mis-parse inside the CI gate test.
func TestLoadGroundTruth_MatchesKnownShapeOnMain(t *testing.T) {
	gt, err := LoadGroundTruth()
	if err != nil {
		t.Fatalf("LoadGroundTruth: %v", err)
	}

	wantSelectOnly := []string{
		"integrations", "integration_sources", "integration_datasets",
		"integration_credentials", "sync_runs", "worker_job_routes",
		"sync_dispatch_transport_routes",
	}
	for _, table := range wantSelectOnly {
		for _, src := range []struct {
			name string
			m    map[string]PrivilegeSet
		}{{"grants", gt.Grants}, {"required", gt.RequiredTablePrivileges}} {
			set, ok := src.m[table]
			if !ok {
				t.Errorf("%s: table %q missing", src.name, table)
				continue
			}
			if !set.Has(PrivSelect) || set.Has(PrivInsert) || set.Has(PrivUpdate) || set.Has(PrivDelete) {
				t.Errorf("%s: table %q = %+v, want SELECT only", src.name, table, set)
			}
		}
	}

	wantSelectUpdate := []string{"sync_run_units"}
	for _, table := range wantSelectUpdate {
		set := gt.RequiredTablePrivileges[table]
		if !set.Has(PrivSelect) || !set.Has(PrivUpdate) || set.Has(PrivInsert) {
			t.Errorf("required: table %q = %+v, want SELECT+UPDATE only", table, set)
		}
	}

	wantSelectInsertUpdate := []string{"sync_watermarks", "sync_dispatch_outbox"}
	for _, table := range wantSelectInsertUpdate {
		set := gt.RequiredTablePrivileges[table]
		if !set.Has(PrivSelect) || !set.Has(PrivInsert) || !set.Has(PrivUpdate) {
			t.Errorf("required: table %q = %+v, want SELECT+INSERT+UPDATE", table, set)
		}
	}

	set := gt.RequiredTablePrivileges["worker_job_outbox"]
	if !set.Has(PrivSelect) || !set.Has(PrivInsert) || set.Has(PrivUpdate) {
		t.Errorf("required: worker_job_outbox = %+v, want SELECT+INSERT only", set)
	}

	if len(gt.RequiredTablePrivileges) != 11 {
		t.Errorf("required_table_privileges: got %d rows, want 11 (pre lane/domain-grant-reconciliation merge) -- "+
			"if this changed intentionally, the ground-truth parser is fine, just update this pin", len(gt.RequiredTablePrivileges))
	}
	if len(gt.Grants) != 11 {
		t.Errorf("runtimeGrantStatements: got %d granted tables, want 11 -- see note above", len(gt.Grants))
	}
}
