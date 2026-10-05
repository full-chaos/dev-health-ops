//go:build integration

package server

// CHAOS-7831 wiring proof: the REAL newQueryHandler (real Mux, real PostgresSwitch over a real Postgres, real gqlgen) refuses a run_operation request
// (internal listener + acr identity headers) for a root whose class row is dark, serves it when the class row is lit, and does not gate the
// envelope-bearer caller (the web app). The unit tests (class_row_gate_test.go) pin the gate; this pins that the route is wired to it.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

func postAsRunOperation(t *testing.T, handler http.HandlerFunc, document string, variables map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	*request = *request.WithContext(iaInternalCtx(request.Context()))
	request.Header.Set(internalidentity.HeaderOrgID, "org-1")
	request.Header.Set(internalidentity.HeaderRole, "viewer")
	request.Header.Set(internalidentity.HeaderSuperuser, "false")
	request.Header.Set(internalidentity.HeaderImpersonationActive, "false")
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

func setClassRowMode(t *testing.T, pool *pgxpool.Pool, operation, mode string) {
	t.Helper()
	tag, err := pool.Exec(context.Background(), `UPDATE go_api_class_decision SET mode = $1 WHERE operation = $2`, mode, operation)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("set class row %s to %s: %v (rows %d)", operation, mode, err, tag.RowsAffected())
	}
}

func TestRunOperationRouteFollowsTheClassRowOfItsRoot(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _, _, _ := newQueryHandler(&fakeHotspotsCHClient{}, pool, verifier, itTestSchemaDigest, os.Getenv)
	runOperation := markClassGated(handler) // what buildQueryRoute mounts at /query/run-operation
	token := signTestEnvelope(t, priv, "org-1")
	hotspots := mcpclass.Operation("hotspots")

	t.Run("no class row: the run_operation request is refused with the MCP refusal", func(t *testing.T) {
		recorder := postAsRunOperation(t, runOperation, registeredHotspotsDocument, hotspotsVariables())
		assertClassRefusal(t, "dark class root", recorder)
	})

	t.Run("a SHADOW class row is dark: refused on this route and not enabled by the shared constructor", func(t *testing.T) {
		classSeed(t, pool, hotspots) // seeds the class row in mode shadow
		assertClassRefusal(t, "shadow class root", postAsRunOperation(t, runOperation, registeredHotspotsDocument, hotspotsVariables()))
		if newClassRowSwitch(pool).Enabled(hotspots) {
			t.Fatal("the class-row switch enabled a shadow row")
		}
		// The same shadow row on the MCP listener (:8092), built over the same constructor, is refused too: one decision on both ports.
		ch := &countingMCPClient{}
		mcpListener := internalidentity.MCP(newMCPHandlerWithLimits(ch, nil, newClassRowSwitch(pool), func(string) string { return "" }, mcpDefaultLimits()))
		assertMCPRefused(t, classHotspots(t, mcpListener), ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)
	})

	t.Run("the web edge's /query with the identity headers is NOT gated by a dark class row", func(t *testing.T) {
		recorder := postAsRunOperation(t, handler, registeredHotspotsDocument, hotspotsVariables()) // /query: the Python /graphql edge's route
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "org/repo-a") {
			t.Fatalf("/query with identity headers was class-gated: status %d, body %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("a dark class root does not gate the envelope caller (web)", func(t *testing.T) {
		recorder := postGraphQLWithVariables(t, handler, registeredHotspotsDocument, token, hotspotsVariables())
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "org/repo-a") {
			t.Fatalf("the envelope caller was gated: status %d, body %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("class row lit: served", func(t *testing.T) {
		classReceipt(t, pool, hotspots, goapiproof.RouteProof, goapiproof.EdgeBuildPresent)
		if _, err := classEnable(pool, hotspots); err != nil {
			t.Fatal(err)
		}
		if !newClassRowSwitch(pool).Enabled(hotspots) {
			t.Fatal("the class-row switch did not enable a canary row")
		}
		recorder := postAsRunOperation(t, runOperation, registeredHotspotsDocument, hotspotsVariables())
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "org/repo-a") {
			t.Fatalf("the lit root was not served: status %d, body %s", recorder.Code, recorder.Body.String())
		}
	})

	t.Run("class row disabled again: refused on the very next request (live read)", func(t *testing.T) {
		setClassRowMode(t, pool, hotspots, "disabled")
		assertClassRefusal(t, "re-darkened class root", postAsRunOperation(t, runOperation, registeredHotspotsDocument, hotspotsVariables()))
	})
}
