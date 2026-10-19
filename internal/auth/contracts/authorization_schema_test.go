package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

func authorizationSchema(t *testing.T, surface string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
	if err != nil {
		t.Fatalf("reading %s: %v", surface, err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing %s: %v", surface, err)
	}
	return document
}

func schemaProps(t *testing.T, node map[string]any) map[string]any {
	t.Helper()
	props, ok := node["properties"].(map[string]any)
	if !ok {
		t.Fatal("schema node has no properties")
	}
	return props
}

func asMap(t *testing.T, value any, what string) map[string]any {
	t.Helper()
	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object", what)
	}
	return m
}

func TestTheDecisionHasNoDefaultVerdictAnywhere(t *testing.T) {
	schema := authorizationSchema(t, AuthorizationDecisionSurface)
	required, _ := schema["required"].([]any)
	found := false
	for _, name := range required {
		if name == "allowed" {
			found = true
		}
	}
	if !found {
		t.Error("`allowed` is not required: a document without a verdict would validate")
	}
	var walk func(node any, path string)
	walk = func(node any, path string) {
		switch value := node.(type) {
		case map[string]any:
			if _, hasDefault := value["default"]; hasDefault {
				t.Errorf("%s declares a default; a decision must never gain a value the sender did not send", path)
			}
			for key, child := range value {
				walk(child, path+"/"+key)
			}
		case []any:
			for i, child := range value {
				walk(child, fmt.Sprintf("%s/%d", path, i))
			}
		}
	}
	walk(schema, "")
	allowed := asMap(t, schemaProps(t, schema)["allowed"], "allowed")
	if allowed["type"] != "boolean" {
		t.Errorf("allowed type = %v, want boolean", allowed["type"])
	}
}

func TestEveryDecisionRequirementTheTicketNamesIsRequired(t *testing.T) {
	schema := authorizationSchema(t, AuthorizationDecisionSurface)
	required := map[string]bool{}
	for _, name := range schema["required"].([]any) {
		required[name.(string)] = true
	}
	for _, name := range []string{"allowed", "expires_at", "policy_revision", "reason_code", "decision_id", "principal_id", "organization_id"} {
		if !required[name] {
			t.Errorf("%s is not required on a decision", name)
		}
	}
	if required["obligations"] {
		t.Error("obligations is required: a deny carries none")
	}
}

func TestAuthorizationSharedFieldsAreReferencesNotCopies(t *testing.T) {
	const principal = "principal.v1.schema.json#/properties/"
	const errorEnvelope = "error.v1.schema.json#/properties/"
	request := authorizationSchema(t, AuthorizationRequestSurface)
	decision := authorizationSchema(t, AuthorizationDecisionSurface)
	resource := asMap(t, schemaProps(t, request)["resource"], "resource")
	context := asMap(t, schemaProps(t, request)["context"], "context")
	cases := []struct {
		name string
		node map[string]any
		ref  string
	}{
		{"request.action", asMap(t, schemaProps(t, request)["action"], "action"), principal + "actor_chain/items/properties/permitted_actions/items"},
		{"request.resource.organization_id", asMap(t, schemaProps(t, resource)["organization_id"], "organization_id"), principal + "organization_id"},
		{"request.context.request_id", asMap(t, schemaProps(t, context)["request_id"], "request_id"), errorEnvelope + "request_id"},
		{"request.context.client_service", asMap(t, schemaProps(t, context)["client_service"], "client_service"), "../endpoint-profile.schema.json#/$defs/endpointProfile/properties/service"},
		{"decision.obligations.tenant_scope", asMap(t, schemaProps(t, asMap(t, schemaProps(t, decision)["obligations"], "obligations"))["tenant_scope"], "tenant_scope"), principal + "organization_id"},
		{"decision.reason_code", asMap(t, schemaProps(t, decision)["reason_code"], "reason_code"), errorEnvelope + "reason_code"},
		{"decision.principal_id", asMap(t, schemaProps(t, decision)["principal_id"], "principal_id"), principal + "principal_id"},
		{"decision.organization_id", asMap(t, schemaProps(t, decision)["organization_id"], "organization_id"), principal + "organization_id"},
		{"decision.policy_revision", asMap(t, schemaProps(t, decision)["policy_revision"], "policy_revision"), principal + "policy_revision"},
		{"decision.expires_at", asMap(t, schemaProps(t, decision)["expires_at"], "expires_at"), principal + "expires_at"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, _ := c.node["$ref"].(string); got != c.ref {
				t.Fatalf("$ref = %q, want %q", got, c.ref)
			}
			// `type` beside a $ref is allowed ONLY to narrow principal.v1's nullable
			// organization_id to a plain string; every other shared field carries none.
			if got, has := c.node["type"]; has {
				narrowsNullableOrg := strings.HasSuffix(c.ref, "/organization_id") && got == "string"
				if !narrowsNullableOrg {
					t.Errorf("%s carries its own type %v beside the $ref", c.name, got)
				}
			}
			for _, banned := range []string{"enum", "pattern", "const", "minimum", "minLength", "maxLength", "format"} {
				if _, present := c.node[banned]; present {
					t.Errorf("%s carries its own %q beside the $ref: a second definition", c.name, banned)
				}
			}
		})
	}
}

func TestTheBatchBoundIsOneNumberInBothBatchSchemas(t *testing.T) {
	request := authorizationSchema(t, AuthorizationBatchRequestSurface)
	response := authorizationSchema(t, AuthorizationBatchResponseSurface)
	requests := asMap(t, schemaProps(t, request)["requests"], "requests")
	decisions := asMap(t, schemaProps(t, response)["decisions"], "decisions")
	index := asMap(t, schemaProps(t, asMap(t, decisions["items"], "items"))["index"], "index")
	maxRequests, _ := requests["maxItems"].(float64)
	maxDecisions, _ := decisions["maxItems"].(float64)
	maxIndex, _ := index["maximum"].(float64)
	if maxRequests == 0 || maxRequests != maxDecisions || maxIndex != maxRequests-1 {
		t.Fatalf("bounds disagree: request maxItems=%v, response maxItems=%v, index maximum=%v (want maximum = maxItems-1)",
			maxRequests, maxDecisions, maxIndex)
	}
	if requests["minItems"] != float64(1) || decisions["minItems"] != float64(1) {
		t.Error("an empty batch must be invalid in both directions")
	}
}

func TestAuthorizationObjectsAreClosed(t *testing.T) {
	request := authorizationSchema(t, AuthorizationRequestSurface)
	decision := authorizationSchema(t, AuthorizationDecisionSurface)
	nodes := map[string]map[string]any{
		"request":              request,
		"request.resource":     asMap(t, schemaProps(t, request)["resource"], "resource"),
		"request.context":      asMap(t, schemaProps(t, request)["context"], "context"),
		"decision":             decision,
		"decision.obligations": asMap(t, schemaProps(t, decision)["obligations"], "obligations"),
		"batch request":        authorizationSchema(t, AuthorizationBatchRequestSurface),
		"batch response":       authorizationSchema(t, AuthorizationBatchResponseSurface),
	}
	for name, node := range nodes {
		if node["additionalProperties"] != false {
			t.Errorf("%s is not closed (additionalProperties must be false)", name)
		}
	}
}

func TestAnErrorEnvelopeIsNeverADecision(t *testing.T) {
	// PDP failure and deny must be distinguishable on the wire: an error.v1
	// body must not validate as a decision, and a decision not as an error.
	root := testRoot(t)
	envelope := map[string]any{
		"schema_version": "error.v1", "status": float64(503), "reason_code": "authority_unavailable",
		"request_id": "req_EXAMPLE0000000000000000001", "occurred_at": "2026-09-03T05:00:00Z",
		"retry_after_seconds": float64(5),
	}
	if err := Validate(root, AuthorizationDecisionSurface, envelope); err == nil {
		t.Error("an error.v1 envelope validated as an authorization decision")
	}
	if err := Validate(root, ErrorSurface, authorizationDocument(t, AuthorizationDecisionSurface, "decision-valid-deny.json")); err == nil {
		t.Error("a deny decision validated as an error envelope")
	}
}
