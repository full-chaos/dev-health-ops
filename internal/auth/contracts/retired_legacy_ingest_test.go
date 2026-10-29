package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The legacy POST /api/v1/ingest/* routes are retired (CHAOS-11813). No route
// table, edge map or endpoint profile may name one: an edge path or a profile
// row for a route nothing mounts is a contract that describes a surface that
// is not there. (external-ingest, /api/v1/external-ingest/*, is another family.)
func TestNoContractNamesARetiredLegacyIngestPath(t *testing.T) {
	root := testRoot(t)
	files := map[string]string{
		"endpoint profiles":     filepath.Join(root, "contracts", "auth", "v1", "endpoint-profiles.go-api.json"),
		"ingress planes":        filepath.Join(root, "contracts", "ingress", "v1", "planes.json"),
		"compose router":        filepath.Join(root, "contracts", "ingress", "v1", "compose-router.nginx.conf"),
		"bigboy traefik planes": filepath.Join(root, "ci", "bigboy", "traefik-dynamic-planes.yml.example"),
		"route response models": filepath.Join(root, "internal", "apiservice", "testdata", "program", "route-response-models.json"),
	}
	for name, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(string(raw), "/api/v1/ingest/") {
			t.Errorf("%s (%s) still names a retired /api/v1/ingest/ path", name, path)
		}
	}
}
