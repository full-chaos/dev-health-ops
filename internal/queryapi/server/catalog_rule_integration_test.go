//go:build integration

package server

// CHAOS-8517 wiring proof: the REAL newQueryHandler (real Mux, the catalog switch over a real, migrated
// Postgres, real gqlgen) serves a registered operation that has no routing row, and nothing else moved:
// the class gate of /query/run-operation and the MCP listener keep every root dark until its class row
// says otherwise (securityAlerts with them), the proof route still needs a row, a document that is not a
// registered text is refused, and a row -- at the live digest or left at another one -- still holds an
// operation dark. routeswitch's own tests pin the rule; this pins that the routes are wired to it and
// that the class rows are not.

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const catalogRuleOlderSchemaDigest = "sha256:an-older-schema-digest"

func TestCatalogRuleServesAnOperationWithNoRowAndLeavesTheClassRowsAlone(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	// Only hotspots requests reach this handler's resolver (its ClickHouse double answers hotspots).
	handler, proof, _, _, _ := newQueryHandler(&fakeHotspotsCHClient{}, pool, verifier, itTestSchemaDigest, os.Getenv)
	runOperation := markClassGated(handler) // what buildQueryRoute mounts at /query/run-operation
	token := signTestEnvelope(t, priv, "org-1")
	securityAlertsVariables := map[string]any{"orgId": "org-1"}
	classRow := func(schemaDigest, root, mode string) {
		t.Helper()
		pgseed.RoutingState(context.Background(), t, pool, schemaDigest, mcpclass.DocumentDigest(), mcpclass.Operation(root), mode)
	}
	served := func(label string, status int, body string) {
		t.Helper()
		if status != http.StatusOK || !strings.Contains(body, "org/repo-a") {
			t.Fatalf("%s: status %d, body %s; want 200 with the fake row", label, status, body)
		}
	}

	var rows int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("the table must start empty for this test to measure a fresh stack: %d row(s), err %v", rows, err)
	}

	t.Run("empty table: /query serves a registered operation", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables())
		served("hotspots with no routing row", rec.Code, rec.Body.String())
	})

	t.Run("empty table: the securityAlerts DOCUMENT is served to the web caller, as its canary row serves it on production", func(t *testing.T) {
		web, _, _, _, _ := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest, os.Getenv)
		// /query refuses with a 404 and nothing else does, so any other status is the executor's own
		// answer: the request was dispatched. (This handler's ClickHouse double holds no data, so the
		// answer itself is not asserted.)
		rec := postGraphQLWithVariables(t, web, registeredSecurityAlertsDocument, token, securityAlertsVariables)
		if rec.Code == http.StatusNotFound || rec.Code == http.StatusUnauthorized {
			t.Fatalf("securityAlerts with no routing row on /query: status %d, body %s; want the executor's answer, not a refusal", rec.Code, rec.Body.String())
		}
	})

	t.Run("empty table: run_operation is refused for every class root it names, securityAlerts with them", func(t *testing.T) {
		assertClassRefusal(t, "securityAlerts, no class row", postAsRunOperation(t, runOperation, registeredSecurityAlertsDocument, securityAlertsVariables))
		assertClassRefusal(t, "hotspots, no class row", postAsRunOperation(t, runOperation, registeredHotspotsDocument, hotspotsVariables()))
	})

	t.Run("empty table: the MCP listener refuses a root with no class row", func(t *testing.T) {
		listener, ch := classListener(t, pool)
		assertMCPRefused(t, classHotspots(t, listener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)
		class := newClassRowSwitch(pool, itTestSchemaDigest)
		for _, root := range mcpclass.SortedRoots() {
			if class.Enabled(mcpclass.Operation(root)) {
				t.Errorf("empty table: the class-row switch enabled %s", mcpclass.Operation(root))
			}
		}
	})

	t.Run("empty table: the proof route still needs a row", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, proof, registeredHotspotsDocument, token, hotspotsVariables())
		if rec.Code != http.StatusNotFound {
			t.Fatalf("/query/proof for an operation with no row: status %d, want 404", rec.Code)
		}
	})

	t.Run("empty table: a document that is not a registered text is refused", func(t *testing.T) {
		changed := "# not the registered text\n" + registeredHotspotsDocument
		if digestHex(changed) == digestHex(registeredHotspotsDocument) {
			t.Fatal("the changed document has the registered digest: this subtest would measure nothing")
		}
		for label, document := range map[string]string{"a registered text with one line added": changed, "an unregistered document": "query { __typename }"} {
			if rec := postGraphQLWithVariables(t, handler, document, token, hotspotsVariables()); rec.Code != http.StatusNotFound {
				t.Errorf("%s: status %d, want 404", label, rec.Code)
			}
		}
	})

	t.Run("a row left at another schema digest still holds its operation dark", func(t *testing.T) {
		for operation, document := range map[string]string{"cognitiveLoad": registeredCognitiveLoadDocument, "reviewEdges": registeredReviewEdgesDocument} {
			mode := map[string]string{"cognitiveLoad": "disabled", "reviewEdges": "canary"}[operation]
			pgseed.RoutingState(context.Background(), t, pool, catalogRuleOlderSchemaDigest, digestHex(document), operation, mode)
		}
		if rec := postGraphQLWithVariables(t, handler, registeredCognitiveLoadDocument, token, cognitiveLoadVariables()); rec.Code != http.StatusNotFound {
			t.Errorf("cognitiveLoad, disabled at another schema digest: status %d, want 404", rec.Code)
		}
		if rec := postGraphQLWithVariables(t, handler, registeredReviewEdgesDocument, token, reviewEdgesVariables()); rec.Code != http.StatusNotFound {
			t.Errorf("reviewEdges, canary at another schema digest only: status %d, want 404 (a stale row, as before)", rec.Code)
		}
	})

	t.Run("production-like rows: the document rows serve, the securityAlerts class root stays dark", func(t *testing.T) {
		// Production: a canary document row per catalog operation, a canary class row for 13 roots, and
		// the securityAlerts class row only at an older schema digest, in shadow (CHAOS-8143).
		setRoutingMode(t, pool, digestHex(registeredHotspotsDocument), "hotspots", "canary")
		setRoutingMode(t, pool, digestHex(registeredSecurityAlertsDocument), "securityAlerts", "canary")
		for _, root := range mcpclass.SortedRoots() {
			if root != "securityAlerts" {
				classRow(itTestSchemaDigest, root, "canary")
			}
		}
		classRow(catalogRuleOlderSchemaDigest, "securityAlerts", "shadow")

		assertClassRefusal(t, "securityAlerts, class row shadow at an older schema digest", postAsRunOperation(t, runOperation, registeredSecurityAlertsDocument, securityAlertsVariables))
		rec := postAsRunOperation(t, runOperation, registeredHotspotsDocument, hotspotsVariables())
		served("hotspots through run_operation with its class row canary", rec.Code, rec.Body.String())

		classRow(itTestSchemaDigest, "securityAlerts", "shadow")
		assertClassRefusal(t, "securityAlerts, class row shadow at the live schema digest", postAsRunOperation(t, runOperation, registeredSecurityAlertsDocument, securityAlertsVariables))
	})

	t.Run("a row in a non-served mode holds a catalog operation dark", func(t *testing.T) {
		for _, mode := range []string{"disabled", "python", "shadow"} {
			setRoutingMode(t, pool, digestHex(registeredHotspotsDocument), "hotspots", mode)
			if rec := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables()); rec.Code != http.StatusNotFound {
				t.Errorf("hotspots with a %s row: status %d, want 404", mode, rec.Code)
			}
		}
	})
}
