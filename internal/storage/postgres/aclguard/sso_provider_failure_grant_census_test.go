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

// The auth runtime role's one write on go-api's provider table: UPDATE on exactly these columns of
// public.sso_providers, the provider failure record. Never the status. A widened, narrowed or reordered list fails
// here first.
func TestTheSSOProviderFailureGrantIsPinnedToTheRecordColumns(t *testing.T) {
	want := []string{"last_error", "last_error_at", "updated_at"}
	if got := authschema.SSOProviderFailureColumns(); !slices.Equal(got, want) {
		t.Fatalf("SSOProviderFailureColumns = %v, want %v", got, want)
	}
}

// Among the auth schema grant passes only the public read pass and the provider failure pass name sso_providers;
// the read pass grants only SELECT and the failure pass only UPDATE, and the failure pass revokes nothing.
func TestOnlyTheProviderFailurePassWritesSSOProviders(t *testing.T) {
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
		if !strings.Contains(string(raw), `"sso_providers"`) {
			continue
		}
		naming = append(naming, filepath.Base(f))
		if filepath.Base(f) == "ssoproviderfailure_grant.go" {
			for _, m := range grant.FindAllStringSubmatch(string(raw), -1) {
				if strings.ToUpper(m[1]) != "UPDATE" {
					t.Errorf("ssoproviderfailure_grant.go grants %s", m[1])
				}
			}
			if regexp.MustCompile(`\bREVOKE\b`).Match(raw) {
				t.Error("ssoproviderfailure_grant.go revokes: the read pass owns the table's REVOKE ALL")
			}
		}
	}
	if !slices.Equal(naming, []string{"publicreads.go", "ssoproviderfailure_grant.go"}) {
		t.Fatalf("authschema sources naming sso_providers: %v, want publicreads.go and ssoproviderfailure_grant.go", naming)
	}
}
