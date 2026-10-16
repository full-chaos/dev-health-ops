package chmigrate

import "testing"

// CHAOS-9432: prod ClickHouse is at head 135. The teams provider identity
// migration must be the last chain file and must be pending (and the only
// pending file) for a store whose chain is applied up to 135.
func TestTeamsProviderIdentityIsPendingOnAStoreAtMigration135(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	const mine = "137_teams_provider_identity.sql"
	if last := chain[len(chain)-1].Version; last < mine {
		t.Fatalf("last chain file %s sorts before %s", last, mine)
	}
	applied := map[string]bool{}
	for _, version := range baseline.Versions {
		applied[version] = true
	}
	seen135 := false
	for _, file := range chain {
		if file.Version < "136" {
			applied[file.Version] = true
			if file.Version == "135_work_item_metrics_nullable.sql" {
				seen135 = true
			}
		}
	}
	if !seen135 {
		t.Fatal("chain has no 135_work_item_metrics_nullable.sql")
	}
	plan := Decide(applied, nil, baseline, chain)
	if plan.State != StateAtHead {
		t.Fatalf("state = %v, want StateAtHead", plan.State)
	}
	found := false
	for _, file := range plan.Pending {
		if file.Version == mine {
			found = true
		}
		if file.Version < "135_" {
			t.Errorf("%s is pending below the 135 head", file.Version)
		}
	}
	if !found {
		t.Fatalf("%s is not pending on a store at 135: %v", mine, plan.Pending)
	}
}
