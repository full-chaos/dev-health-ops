package internalidentity

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded Python edge golden is declared. Claims are the
// paths whose change makes the oracle tests fail (backed by the mutation run in the pull request
// that added this file); the two names are labels used in failure messages only.

var edgeGoldenClaims = []string{
	".cases[].expected.ImpersonationActive",
	".cases[].expected.IsSuperuser",
	".cases[].expected.OrgID",
	".cases[].expected.Role",
	".cases[].headers.X-DH-Internal-Impersonation-Active",
	".cases[].headers.X-DH-Internal-Org-Id",
	".cases[].headers.X-DH-Internal-Role",
	".cases[].headers.X-DH-Internal-Superuser",
	".refused[].org_id",
	".refused[].role",
}

var edgeGoldenNotClaims = map[string]string{
	".cases[].name":   "a case label (failure messages only)",
	".refused[].name": "a case label (failure messages only)",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "python_edge_identity_headers.json"))
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, edgeGoldenClaims, edgeGoldenNotClaims)
}
