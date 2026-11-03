package authschema

import "testing"

// The runtime role holds no privilege on the External Push token table (the
// domain store): not in the table posture, not in the public column reads.
func TestTheRuntimeRoleHoldsNothingOnTheExternalIngestTokenTable(t *testing.T) {
	for _, entry := range RuntimePosture() {
		if entry.Table == "external_ingest_tokens" {
			t.Errorf("the runtime posture lists %s", entry.Table)
		}
	}
	for _, entry := range PublicReadColumns() {
		if entry.Table == "external_ingest_tokens" {
			t.Errorf("the public column reads list %s", entry.Table)
		}
	}
	if len(RuntimePosture()) == 0 || len(PublicReadColumns()) == 0 {
		t.Fatal("an empty manifest would pass this vacuously")
	}
}
