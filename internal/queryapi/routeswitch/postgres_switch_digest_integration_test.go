//go:build integration

package routeswitch

// CHAOS-7845: the three clauses of PostgresSwitch.Enabled's lookup that no other test pinned, and the MCP class row-key separation.
//
//   - document_digest: a canary row of ANOTHER document digest for the same schema and operation must not serve the operation
//     (the row key is the 3-tuple schema_digest, document_digest, selected_operation).
//   - selected_operation, exact match: a row keyed "mcp:<root>" (a class row) carrying the DOCUMENT digest must not enable the document
//     operation "<root>", and a document row named "<root>" must not enable the class key "mcp:<root>".

import "testing"

const (
	digestA = "sha256:7845aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB = "sha256:7845bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestPostgresSwitch_ACanaryRowOfAnotherDocumentDigestDoesNotServe(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, digestB, "hotspots", "canary") // the row exists, at digest B

	if NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"hotspots": digestA}).Enabled("hotspots") {
		t.Fatal("a canary row at document digest B served an operation registered at digest A")
	}
	if !NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"hotspots": digestB}).Enabled("hotspots") {
		t.Fatal("control: the same canary row must serve the operation registered at ITS digest")
	}
}

func TestPostgresSwitch_ClassRowKeyAndDocumentOperationNameDoNotCrossEnable(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, digestA, "mcp:securityOverview", "canary") // a CLASS row carrying the document digest

	if NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"securityOverview": digestA}).Enabled("securityOverview") {
		t.Fatal("a class row mcp:securityOverview enabled the document operation securityOverview")
	}
	if !NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"mcp:securityOverview": digestA}).Enabled("mcp:securityOverview") {
		t.Fatal("control: the class row must enable its own key")
	}

	insertRoutingState(t, pool, digestB, "hotspots", "canary") // a DOCUMENT row named like a class root
	if NewPostgresSwitch(pool, testSchemaDigest, map[string]string{"mcp:hotspots": digestB}).Enabled("mcp:hotspots") {
		t.Fatal("a document row hotspots enabled the class key mcp:hotspots")
	}
}
