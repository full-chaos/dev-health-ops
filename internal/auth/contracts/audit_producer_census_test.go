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

// auditWireExempt names the producer packages whose unit tests cannot hold a
// row to audit-event.v1 with auditwire.MustValidate, with the reason. Their
// keys and values are still covered by the per-key parity tests of this
// package; a real-PG read-back of their rows is the lead's integration cell.
var auditWireExempt = map[string]string{
	"internal/audit":                 "auditwire imports audit; the package's own rows are pinned key by key in this package",
	"internal/auth/contracts":        "the parsed audit-event.v1 type, not a producer",
	"internal/auth/delegationend":    "the row is built inside the commit callback; no unit-level capture",
	"internal/auth/oauthgrant":       "the row is built inside the commit callback; no unit-level capture",
	"internal/auth/outboxprojection": "the row is built inside the commit callback; no unit-level capture",
	"internal/auth/restorefence":     "the row is built inside the commit callback; no unit-level capture",
	"internal/auth/session":          "the rows are built inside the commit callback; no unit-level capture",
	"internal/joboperator":           "a different AuditEvent type (job operator log), not security_audit_events",
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
