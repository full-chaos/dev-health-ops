package providersync

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

func TestGitLabCatalogProjectIDStringAgrees(t *testing.T) {
	id, ok := GitLabCatalogProjectID("org-acme", "123")
	if !ok {
		t.Fatal("GitLabCatalogProjectID refused a valid id")
	}
	if got := teamattribution.GitLabCatalogProjectIDString("org-acme", "123"); got != id.String() {
		t.Fatalf("teamattribution builds %q, providersync builds %q", got, id.String())
	}
	if !teamattribution.IsGitLabCatalogProjectID(id.String()) || teamattribution.IsGitLabCatalogProjectID("acme/api") {
		t.Fatal("IsGitLabCatalogProjectID does not tell the catalog form from a path")
	}
}
