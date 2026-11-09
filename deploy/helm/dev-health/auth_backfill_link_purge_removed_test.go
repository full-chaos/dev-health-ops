package devhealth_test

import (
	"strings"
	"testing"
)

// The legacy link-token purge verb is gone: where users are owned, a backfill
// Job naming it fails the render on the verb list, and the list names no purge.
func TestBackfillVerbLegacyLinkPurgeFailsRender(t *testing.T) {
	args := withSets(append(append([]string{}, backfillOn...), phase3Secrets...),
		"authService.backfill.verb=legacy-link-purge", "authService.backfill.runId=r1", "authService.phase=4", "authService.usersOwnedStage=owned", "goApi.enabled=true")
	args = append(append(append(args, sessionReaders...), sessionSuccessor...), authTrusted...)
	_, out, err := renderAuth(t, args...)
	if err == nil || !strings.Contains(out, "authService.backfill.verb must be users-backfill, users-reconcile or orgs-backfill") {
		t.Errorf("legacy-link-purge rendered or failed on another rule: err=%v out=%.300s", err, out)
	}
}
