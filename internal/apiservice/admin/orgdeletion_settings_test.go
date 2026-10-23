package admin

import "testing"

func TestOrgDeletionMutationSettingsWaitForAllReplicas(t *testing.T) {
	if got := orgDeletionMutationSettings()["mutations_sync"]; got != "2" {
		t.Fatalf("mutations_sync = %v, want 2", got)
	}
}
