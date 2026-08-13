package domaingrants

import "testing"

// TestLoadGroundTruth_MatchesKnownShape pins the ground-truth reader against
// the current, hand-maintained state of migrate.go's runtimeGrantStatements
// and the domain posture. If either changes shape, this test should be the
// first thing that tells you the reader needs updating -- NOT a silent
// mis-read inside the CI gate test.
//
// Updated for the Option B two-role split. Three things moved:
// worker_job_routes left the domain role entirely (it is coordinator-exclusive
// now, so it must be absent from BOTH lists); sync_runs became one of the
// dual-grant tables and carries INSERT+UPDATE on the domain side; and DELETE became
// expressible, so the DELETE tables are asserted here rather than being
// permanently unrepresentable.
func TestLoadGroundTruth_MatchesKnownShape(t *testing.T) {
	gt, err := LoadGroundTruth()
	if err != nil {
		t.Fatalf("LoadGroundTruth: %v", err)
	}

	wantSelectOnly := []string{
		"integrations",
		"integration_credentials", "sync_dispatch_transport_routes",
		"sync_configurations", "organizations",
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

	// worker_job_routes is coordinator-exclusive under the split: a domain
	// grant or posture row for it would be a regression, not an omission.
	for _, src := range []struct {
		name string
		m    map[string]PrivilegeSet
	}{{"grants", gt.Grants}, {"required", gt.RequiredTablePrivileges}} {
		if _, ok := src.m["worker_job_routes"]; ok {
			t.Errorf("%s: worker_job_routes is coordinator-exclusive and must not appear on the domain side", src.name)
		}
	}

	wantMaterializerDomainWrites := []string{"integration_sources", "integration_datasets", "sync_run_units", "sync_runs"}
	for _, table := range wantMaterializerDomainWrites {
		for _, src := range []struct {
			name string
			m    map[string]PrivilegeSet
		}{{"grants", gt.Grants}, {"required", gt.RequiredTablePrivileges}} {
			set := src.m[table]
			if !set.Has(PrivSelect) || !set.Has(PrivInsert) || !set.Has(PrivUpdate) || set.Has(PrivDelete) {
				t.Errorf("%s: table %q = %+v, want SELECT+INSERT+UPDATE", src.name, table, set)
			}
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

	// sync_run_unit_effect_snapshots pins all four privileges, ABSENCE
	// included. The count pin below cannot see a privilege change -- adding
	// UPDATE to this tuple leaves every other assertion in this file green --
	// and the grants-vs-posture divergence check is advisory by design. The
	// no-UPDATE property is load-bearing: PostgreSQL treats FOR UPDATE and
	// FOR SHARE as UPDATE-class privileges, so granting UPDATE here would
	// silently re-enable the row-locking clause that broke every re-prepare
	// in production. It must be pinned in the posture, not only in migrate.go
	// and the provisioning scripts.
	snapshots := gt.RequiredTablePrivileges["sync_run_unit_effect_snapshots"]
	if !snapshots.Has(PrivSelect) || !snapshots.Has(PrivInsert) ||
		!snapshots.Has(PrivDelete) || snapshots.Has(PrivUpdate) {
		t.Errorf(
			"required: sync_run_unit_effect_snapshots = %+v, want SELECT+INSERT+DELETE and NOT UPDATE",
			snapshots,
		)
	}
	snapshotGrants := gt.Grants["sync_run_unit_effect_snapshots"]
	if !snapshotGrants.Has(PrivSelect) || !snapshotGrants.Has(PrivInsert) ||
		!snapshotGrants.Has(PrivDelete) || snapshotGrants.Has(PrivUpdate) {
		t.Errorf(
			"grants: sync_run_unit_effect_snapshots = %+v, want SELECT+INSERT+DELETE and NOT UPDATE",
			snapshotGrants,
		)
	}

	// DELETE is an ordinary privilege since Phase 2 added AllowDelete to the
	// posture; these tables used to be reported as permanently unrepresentable.
	for _, table := range []string{"dev_conversations", "external_ingest_batch_payloads", "provider_rate_limit_observations"} {
		for _, src := range []struct {
			name string
			m    map[string]PrivilegeSet
		}{{"grants", gt.Grants}, {"required", gt.RequiredTablePrivileges}} {
			if set := src.m[table]; !set.Has(PrivDelete) {
				t.Errorf("%s: table %q = %+v, want DELETE present", src.name, table, set)
			}
		}
	}

	for _, src := range []struct {
		name string
		m    map[string]PrivilegeSet
	}{{"grants", gt.Grants}, {"required", gt.RequiredTablePrivileges}} {
		conversations := src.m["dev_conversations"]
		if !conversations.Has(PrivSelect) || !conversations.Has(PrivUpdate) || !conversations.Has(PrivDelete) || conversations.Has(PrivInsert) {
			t.Errorf("%s: dev_conversations = %+v, want SELECT+UPDATE+DELETE only", src.name, conversations)
		}
		tombstones := src.m["dev_conversation_tombstones"]
		if !tombstones.Has(PrivSelect) || !tombstones.Has(PrivInsert) || tombstones.Has(PrivUpdate) || tombstones.Has(PrivDelete) {
			t.Errorf("%s: dev_conversation_tombstones = %+v, want SELECT+INSERT only", src.name, tombstones)
		}
	}

	if len(gt.RequiredTablePrivileges) != 40 {
		t.Errorf("domain posture: got %d tables, want 40 (Option B two-role split) -- "+
			"if this changed intentionally, the ground-truth reader is fine, just update this pin", len(gt.RequiredTablePrivileges))
	}
	if len(gt.Grants) != 40 {
		t.Errorf("runtimeGrantStatements: got %d granted tables, want 40 -- see note above", len(gt.Grants))
	}
	// The two lists agreeing on their size is the property that matters most
	// here: this checker exists because they disagreed once.
	if len(gt.Grants) != len(gt.RequiredTablePrivileges) {
		t.Errorf("grants cover %d tables but the posture declares %d -- the two hand-maintained lists have drifted apart",
			len(gt.Grants), len(gt.RequiredTablePrivileges))
	}
}
