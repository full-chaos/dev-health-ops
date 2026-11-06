package apiservice

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// go-api's decision point client is the plain PEP client: no allow cache sits in front of it, so a read on an enforced
// row asks the decision point every time and a PDP outage refuses it at once, never at a cache expiry.
func TestTheDecisionPointClientIsNotCached(t *testing.T) {
	pdp := newShadowPDPServer(t)
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: pdp.URL})
	dp := newDecisionPoint(proxy, lookupFrom(map[string]string{EnvAuthWorkloadBootstrapFile: bootstrapFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer}), nil)
	decider, err := dp.decider(workloadAsk{Flag: authzenforce.GoAPI.Env() + " lists a namespace"})
	if err != nil {
		t.Fatal(err)
	}
	if _, plain := decider.(*pep.Client); !plain {
		t.Fatalf("decision point client %T, want *pep.Client with no cache", decider)
	}
}

// No production file of the module builds the PEP allow cache. A PEP cache needs its own outage and expiry cells
// before it ships.
func TestNoProductionFileBuildsAPEPDecisionCache(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root %s: %v", root, err)
	}
	scanned := 0
	var hits []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		if strings.Contains(string(raw), "NewCachedClient(") {
			rel, _ := filepath.Rel(root, path)
			hits = append(hits, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("scanned %d production files under %s, want the whole module", scanned, root)
	}
	if len(hits) != 0 {
		t.Fatalf("production files that build a PEP decision cache: %v", hits)
	}
}
