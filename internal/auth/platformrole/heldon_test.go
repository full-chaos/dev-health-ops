package platformrole

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// HeldOn only reads: one SELECT EXISTS over the stored assignments, with no principal state in it (an admin update
// compares is_superuser with the stored assignment of a disabled user too) and no write.
func TestHeldOnOnlyReadsTheStoredAssignment(t *testing.T) {
	raw, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "func (s *Store) HeldOn(")
	if start < 0 {
		t.Fatal("store.go has no HeldOn")
	}
	body := src[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "SELECT EXISTS (SELECT 1 FROM `+s.q+`.platform_role_assignments") {
		t.Error("HeldOn does not read one SELECT EXISTS over platform_role_assignments")
	}
	for _, banned := range []string{"disabled_at", "principals", "public.users", "is_active"} {
		if strings.Contains(body, banned) {
			t.Errorf("HeldOn reads %s: it compares with the stored assignment, whatever the principal's state", banned)
		}
	}
	if regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|TRUNCATE)\b|tx\.Exec|\.Exec\(`).MatchString(body) {
		t.Error("HeldOn writes: it is a read")
	}
}
