package aclguard_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The auth runtime role's one ungated write on a go-api table (D6509): INSERT on exactly these columns of
// public.audit_logs, go-api's audit row. A widened, narrowed or reordered list fails here first.
func TestThePublicAuditWriteGrantIsPinnedToGoAPIsAuditRowColumns(t *testing.T) {
	want := []string{"id", "org_id", "user_id", "action", "resource_type", "resource_id", "description",
		"changes", "request_metadata", "status", "error_message", "created_at"}
	if got := authschema.PublicAuditWriteColumns(); !slices.Equal(got, want) {
		t.Fatalf("PublicAuditWriteColumns = %v, want %v", got, want)
	}
}

// Only the public audit grant pass names audit_logs among the auth schema grant passes, and its only GRANT is the
// column INSERT: no other pass can widen or revoke the audit grant.
func TestOnlyThePublicAuditPassGrantsOnAuditLogs(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal", "storage", "postgres", "authschema")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no authschema sources (err=%v)", err)
	}
	grant := regexp.MustCompile(`\bGRANT\s+(\w+)`)
	var naming []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), `"audit_logs"`) {
			continue
		}
		naming = append(naming, filepath.Base(f))
		if filepath.Base(f) == "publicaudit_grant.go" {
			for _, m := range grant.FindAllStringSubmatch(string(raw), -1) {
				if strings.ToUpper(m[1]) != "INSERT" {
					t.Errorf("publicaudit_grant.go grants %s", m[1])
				}
			}
		}
	}
	if !slices.Equal(naming, []string{"publicaudit_grant.go"}) {
		t.Fatalf("authschema sources naming audit_logs: %v, want only publicaudit_grant.go", naming)
	}
}
