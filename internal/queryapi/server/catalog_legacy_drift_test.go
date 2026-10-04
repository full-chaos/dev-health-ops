package server

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// CHAOS-8649: query-api's switch learns an operation's legacy texts from legacyDigestsByOperation
// (query_route.go, NewCatalogSwitchWithLegacy); `dho goapi routing status` learns them from the checked-in
// catalog, through the loader it calls. When the two disagree, the census reports a served row unreachable,
// or a dead row served. The catalog is generated (scripts/go_api/generate_operation_catalog.py over
// cmd/registrydump), so a mismatch here means it was not regenerated.
func TestCheckedInCatalogListsExactlyTheLegacyDigestsTheSwitchAccepts(t *testing.T) {
	path := filepath.Join("..", "..", "..", filepath.FromSlash(goapiproof.DefaultCatalogPath))
	_, _, catalogLegacy, err := goapiproof.LoadOperationCatalogWithKindsAndLegacy(path)
	if err != nil {
		t.Fatalf("load the checked-in catalog: %v", err)
	}
	if !reflect.DeepEqual(catalogLegacy, legacyDigestsByOperation) {
		t.Fatalf("the checked-in catalog's legacy entries differ from legacyDigestsByOperation:\n  catalog: %v\n  switch:  %v\n"+
			"  regenerate the catalog with scripts/go_api/generate_operation_catalog.py and commit it", catalogLegacy, legacyDigestsByOperation)
	}
}
