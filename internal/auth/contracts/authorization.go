package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// The four authorization wire surfaces (TRD sections 8 and 14).
const (
	AuthorizationRequestSurface       = "authorization_request.v1"
	AuthorizationDecisionSurface      = "authorization_decision.v1"
	AuthorizationBatchRequestSurface  = "authorization_batch_request.v1"
	AuthorizationBatchResponseSurface = "authorization_batch_response.v1"
)

// AuthorizationObligations are the enforceable obligations of an allow.
type AuthorizationObligations struct {
	TenantScope         string   `json:"tenant_scope,omitempty"`
	RepositoryAllowlist []string `json:"repository_allowlist,omitempty"`
	Redaction           []string `json:"redaction,omitempty"`
	MinimumAssurance    string   `json:"minimum_assurance,omitempty"`
	StepUp              bool     `json:"step_up,omitempty"`
}

// AuthorizationDecision is one authorization_decision.v1, validated and parsed.
//
// Allowed is a plain bool and is only ever populated from a document the
// schema has proven carries a strict boolean `allowed`: a missing, null or
// non-boolean member never reaches this struct, so the zero value (deny) is
// never produced by a malformed document being read as one.
type AuthorizationDecision struct {
	DecisionID     string                    `json:"decision_id"`
	Allowed        bool                      `json:"allowed"`
	ReasonCode     string                    `json:"reason_code"`
	PrincipalID    string                    `json:"principal_id"`
	OrganizationID *string                   `json:"organization_id"`
	PolicyRevision Revision                  `json:"policy_revision"`
	ExpiresAt      Timestamp                 `json:"expires_at"`
	Obligations    *AuthorizationObligations `json:"obligations,omitempty"`
}

func checkedDocument(root, surface string, raw []byte) (any, error) {
	if err := refuseDuplicateMembers(raw); err != nil {
		return nil, fmt.Errorf("%s: %w", surface, err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("%s: not valid JSON: %w", surface, err)
	}
	if err := Validate(root, surface, document); err != nil {
		return nil, err
	}
	return document, nil
}

func decodeStrict(raw []byte, into any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(into)
}

// ParseAuthorizationDecision parses one decision from the RAW response body.
//
// Order matters: duplicate members are refused on the bytes first (a repeated
// `allowed` keeps the last value in every parser, so the schema would validate
// an allow the sender never unambiguously sent), then the schema, then a strict
// decode. Any failure means "no valid decision" and the caller denies.
func ParseAuthorizationDecision(root string, raw []byte) (*AuthorizationDecision, error) {
	if _, err := checkedDocument(root, AuthorizationDecisionSurface, raw); err != nil {
		return nil, err
	}
	var decision struct {
		AuthorizationDecision
		SchemaVersion string `json:"schema_version"`
	}
	if err := decodeStrict(raw, &decision); err != nil {
		return nil, fmt.Errorf("%s: decoding validated document: %w", AuthorizationDecisionSurface, err)
	}
	return &decision.AuthorizationDecision, nil
}

// ParseAuthorizationBatchResponse parses a batch response against the batch
// request it answers and returns one decision per request, in request order.
//
// The binding checks compare TWO documents, so no schema can express them:
// the response must carry exactly one decision per request, and the decision
// at position i must declare index i. A response that fails either is refused
// WHOLE. Returning the items that happen to line up would let one item's allow
// be applied to another item's resource, which is the failure this surface
// exists to prevent; the caller denies every item.
func ParseAuthorizationBatchResponse(root string, requestRaw, responseRaw []byte) ([]AuthorizationDecision, error) {
	requestDoc, err := checkedDocument(root, AuthorizationBatchRequestSurface, requestRaw)
	if err != nil {
		return nil, err
	}
	if _, err := checkedDocument(root, AuthorizationBatchResponseSurface, responseRaw); err != nil {
		return nil, err
	}
	requests, _ := requestDoc.(map[string]any)["requests"].([]any)

	var response struct {
		Decisions []struct {
			Index    Revision              `json:"index"`
			Decision AuthorizationDecision `json:"decision"`
		} `json:"decisions"`
	}
	dec := json.NewDecoder(bytes.NewReader(responseRaw))
	if err := dec.Decode(&response); err != nil {
		return nil, fmt.Errorf("%s: decoding validated document: %w", AuthorizationBatchResponseSurface, err)
	}
	if len(response.Decisions) != len(requests) {
		return nil, fmt.Errorf("%s: %d decisions for %d requests; refusing the whole response",
			AuthorizationBatchResponseSurface, len(response.Decisions), len(requests))
	}
	out := make([]AuthorizationDecision, len(response.Decisions))
	for position, item := range response.Decisions {
		if int64(item.Index) != int64(position) {
			return nil, fmt.Errorf("%s: decision at position %d declares index %d; refusing the whole response",
				AuthorizationBatchResponseSurface, position, item.Index)
		}
		out[position] = item.Decision
	}
	return out, nil
}
