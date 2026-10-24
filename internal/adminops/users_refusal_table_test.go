package adminops

import (
	"os"
	"strings"
	"testing"
)

// The one place the operator users verbs refuse more than Python did is a row
// of testdata/refusals/users_password.tsv (D6129): its kind is "stricter", its
// text says "stricter than Python", and it names Go tests that exist.
func TestUsersPasswordStricterRefusalHasATableRow(t *testing.T) {
	raw, err := os.ReadFile("testdata/refusals/users_password.tsv")
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rows++
		parts := strings.Split(line, "\t")
		if len(parts) != 3 || parts[0] != "users create|update --password" || parts[1] != "stricter" {
			t.Fatalf("row %q: want site, kind stricter, reference", line)
		}
		if !strings.Contains(parts[2], "stricter than Python") {
			t.Errorf("reference %q does not say \"stricter than Python\"", parts[2])
		}
		for _, name := range []string{"TestOperatorCreateUserRefusesElevenCharacterPassword", "TestOperatorUpdateUserRefusesElevenCharacterPassword"} {
			if !strings.Contains(parts[2], name) {
				t.Errorf("reference does not name %s", name)
			}
			source, err := os.ReadFile("../apiservice/admin/operator_password_test.go")
			if err != nil || !strings.Contains(string(source), "func "+name+"(") {
				t.Errorf("the named test %s does not exist", name)
			}
		}
		for _, step := range []string{"3", "6", "48", "50"} {
			if !strings.Contains(parts[2], step+",") && !strings.Contains(parts[2], step+" of") {
				t.Errorf("reference does not name retired step %s", step)
			}
		}
	}
	if rows != 1 {
		t.Fatalf("the table has %d rows, want 1", rows)
	}
}
