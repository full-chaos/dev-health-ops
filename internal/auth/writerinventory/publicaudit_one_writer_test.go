package writerinventory

import (
	"strings"
	"testing"
)

var authServiceRoots = []string{"internal/auth/", "cmd/auth-service/"}

func authServiceAuditLogsWriterOutsidePublicAudit(file, table string) bool {
	if table != "public.audit_logs" || strings.HasPrefix(file, "internal/auth/publicaudit/") {
		return false
	}
	for _, root := range authServiceRoots {
		if strings.HasPrefix(file, root) {
			return true
		}
	}
	return false
}

// D6509: internal/auth/publicaudit is auth-service's one writer of public.audit_logs, in the tree and in the allowlist.
func TestPublicAuditIsAuthServicesOneAuditLogsWriter(t *testing.T) {
	root := repoRoot(t)
	watched, err := Watched(root)
	if err != nil {
		t.Fatal(err)
	}
	if !watched["public.audit_logs"] {
		t.Fatal("public.audit_logs is not a watched table")
	}
	found, err := Scan(root, watched)
	if err != nil {
		t.Fatal(err)
	}
	inPublicAudit := 0
	for _, w := range found {
		if authServiceAuditLogsWriterOutsidePublicAudit(w.File, w.Table) {
			t.Errorf("auth-service writes public.audit_logs outside publicaudit (D6509): %s", w.Key())
		}
		if w.Table == "public.audit_logs" && strings.HasPrefix(w.File, "internal/auth/publicaudit/") {
			inPublicAudit++
		}
	}
	if inPublicAudit == 0 {
		t.Error("the scan found no publicaudit writer of public.audit_logs")
	}
	for _, e := range Allowlist {
		if authServiceAuditLogsWriterOutsidePublicAudit(e.File, e.Table) {
			t.Errorf("allowlist row of an auth-service public.audit_logs writer outside publicaudit (D6509): %s", e.Key())
		}
	}
}
