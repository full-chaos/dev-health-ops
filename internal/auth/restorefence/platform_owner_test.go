package restorefence

import (
	"strings"
	"testing"
)

// The fence never inserts a grant; it names the missing assignment ids only. An id can be a platform_owner row, which
// the admin route refuses to grant, so the follow-up must name the verb that grants it.
func TestTheRegrantFollowUpNamesTheOwnerVerb(t *testing.T) {
	got := followUp[ActionRegrantPlatformRole]
	if !strings.Contains(got, "auth-migrate platform-owner-grant") {
		t.Errorf("regrant_platform_role follow-up %q does not name auth-migrate platform-owner-grant for a platform_owner row", got)
	}
	if !strings.Contains(got, "platform_admin") {
		t.Errorf("regrant_platform_role follow-up %q lost the platform_admin route", got)
	}
}
