package pdp

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// query-api's gate holds the plain PEP client: no allow cache sits in front of it. An allow the PDP just gave (with an
// expires_at an hour ahead) is not reused once the PDP is down: the next read is refused at once as a local deny.
func TestPDPGateHoldsNoDecisionCache(t *testing.T) {
	srv := pdptest.New(t)
	env := onEnv(t, "enforce", srv.URL)
	env[authzenforce.QueryAPI.Env()] = "analytics"
	g := FromSettings((&recordingLookup{values: env}).lookup, nil)
	if _, plain := g.decider.(*pep.Client); !plain {
		t.Fatalf("gate decider %T, want *pep.Client with no cache", g.decider)
	}
	if _, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg); err != nil {
		t.Fatalf("allow answered %v", err)
	}
	srv.Fail(http.StatusServiceUnavailable)
	_, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
	denied, ok := err.(*DenyError)
	if !ok || !denied.Local || denied.Reason != pep.ReasonUnavailable {
		t.Fatalf("PDP down after an allow answered %#v, want a local DenyError %s", err, pep.ReasonUnavailable)
	}
	if got := len(srv.Requests()); got != 2 {
		t.Fatalf("%d PDP calls, want 2: each read asks the PDP", got)
	}
}

// query-api builds its PEP client in this package (pdp.go); no production file here builds the PEP allow cache.
func TestQueryAPIBuildsNoPEPDecisionCache(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var hits []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "NewCachedClient(") {
			hits = append(hits, name)
		}
	}
	if len(hits) != 0 {
		t.Fatalf("files that build a PEP decision cache: %v", hits)
	}
}
