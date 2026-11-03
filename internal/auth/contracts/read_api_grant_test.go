package contracts_test

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
)

// grant-read holds the key pattern and the roles the grant store holds. The
// store imports this package, so the cell lives in the external test package.
func TestGrantReadKeyPatternAndRolesAreTheGrantStores(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(contracts.SchemaPath(root, "grant-read.v1"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Properties struct {
			ResourceKey struct {
				Pattern string `json:"pattern"`
			} `json:"resource_key"`
			Role struct {
				Enum []string `json:"enum"`
			} `json:"role"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Properties.ResourceKey.Pattern != grant.KeyPattern() {
		t.Errorf("grant-read resource_key pattern %q, grant.KeyPattern %q", doc.Properties.ResourceKey.Pattern, grant.KeyPattern())
	}
	got, want := doc.Properties.Role.Enum, grant.Roles()
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") || len(got) == 0 {
		t.Errorf("grant-read roles %v, grant.Roles %v", got, want)
	}
}
