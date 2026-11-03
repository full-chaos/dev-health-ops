package contracts

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// auditWireExempt names the packages that hold an AuditEvent literal but are not
// producers of a security_audit_events row, or cannot call auditwire, with the reason.
var auditWireExempt = map[string]string{
	"internal/audit":          "auditwire imports audit; the package's own rows are pinned key by key in this package",
	"internal/auth/contracts": "the parsed audit-event.v1 type, not a producer",
	"internal/joboperator":    "a different AuditEvent type (job operator log), not security_audit_events",
}

var auditEventLiteral = regexp.MustCompile(`\bAuditEvent\{`)
var auditWireCall = regexp.MustCompile(`(?m)^\s*auditwire\.MustValidate\(`)

// Every package that builds an audit.AuditEvent holds at least one of its rows
// to the published contract in a unit test, or is named above with a reason.
func TestEveryAuditProducerPackageChecksItsRowsAgainstTheWireSchema(t *testing.T) {
	root := testRoot(t)
	producers := map[string]bool{}
	hooked := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, filepath.Dir(path))
		switch {
		case strings.HasSuffix(path, "_test.go"):
			if auditWireCall.Match(raw) {
				hooked[rel] = true
			}
		case auditEventLiteral.Match(raw):
			producers[rel] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for dir := range producers {
		if hooked[dir] {
			if reason, ok := auditWireExempt[dir]; ok {
				t.Errorf("%s is hooked and also exempt (%s): drop the exemption", dir, reason)
			}
			continue
		}
		if _, ok := auditWireExempt[dir]; !ok {
			missing = append(missing, dir)
		}
	}
	sort.Strings(missing)
	for _, dir := range missing {
		t.Errorf("%s builds audit rows and no unit test calls auditwire.MustValidate on one", dir)
	}
	for dir := range auditWireExempt {
		if !producers[dir] {
			t.Errorf("the exemption for %s is stale: no AuditEvent literal there", dir)
		}
	}
}
