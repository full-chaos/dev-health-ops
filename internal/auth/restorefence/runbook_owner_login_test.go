package restorefence

import (
	"os"
	"strings"
	"testing"
)

// The fence reads the legacy public tables, which the least-privilege migrator cannot read. The runbooks must name the
// owner Secret and give the exact commands, so nobody runs export or apply as the migrator.
func TestRunbooksNameTheOwnerSecretForTheFence(t *testing.T) {
	for _, path := range []string{
		"../../../docs/operate/maintain/backup-and-restore.md",
		"../../../docs/operate/runbooks/backup-or-restore-failure.md",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		doc := string(raw)
		for _, want := range []string{
			"auth-migrate-database-owner",
			"auth-migrate restore-fence export -out manifest.json",
			"auth-migrate restore-fence apply -manifest manifest.json -apply",
			"auth-migrate restore-fence status",
		} {
			if !strings.Contains(doc, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}
