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

// The projection reader's one grant (CHAOS-11199, CHAOS-10897): column SELECT on exactly these columns of
// auth.credential_projection, for the worker login that runs the acr and the External Push reconcile. A widened, narrowed or reordered list fails here first.
func TestTheProjectionReaderGrantIsPinnedToTheReadColumns(t *testing.T) {
	want := []string{"domain_owner", "organization_id", "external_reference", "revision", "status", "expires_at", "scopes", "source_id"}
	reads := authschema.ProjectionReaderReads()
	if len(reads) != 1 || reads[0].Table != "credential_projection" || !slices.Equal(reads[0].Columns, want) {
		t.Fatalf("ProjectionReaderReads = %+v, want credential_projection %v", reads, want)
	}
}

// Among the auth schema grant sources only the runtime posture and the projection reader declaration name
// credential_projection; the reader pass grants only column SELECT and revokes only inside the reader's own statements.
func TestOnlyTheKnownPassesGrantOnCredentialProjection(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal", "storage", "postgres", "authschema")
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no authschema sources (err=%v)", err)
	}
	var naming []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"credential_projection"`) {
			naming = append(naming, filepath.Base(f))
		}
		if filepath.Base(f) == "session_readers.go" {
			for _, m := range regexp.MustCompile(`\bGRANT\s+(\w+)`).FindAllStringSubmatch(string(raw), -1) {
				if m[1] != "SELECT" && m[1] != "USAGE" {
					t.Errorf("session_readers.go grants %s", m[1])
				}
			}
		}
	}
	if !slices.Equal(naming, []string{"grants.go", "publicreads.go"}) {
		t.Fatalf("authschema sources naming credential_projection: %v, want grants.go and publicreads.go", naming)
	}
}
