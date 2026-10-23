package providersync

import "testing"

func TestCleanupDeleteSettingsWaitForAllReplicas(t *testing.T) {
	if got := cleanupDeleteSettings()["mutations_sync"]; got != "2" {
		t.Fatalf("mutations_sync = %v, want 2", got)
	}
}
