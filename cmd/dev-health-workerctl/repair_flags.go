package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/repair"
)

// reviewEvidenceMaxBytes mirrors the ledger's own bound on the operator's
// statement of what they verified, in UTF-8 bytes of the text as submitted.
const reviewEvidenceMaxBytes = repair.ReviewEvidenceMaxBytes

// validateReviewEvidence reports whether text is a non-empty, in-bound
// review-evidence string (trimmed non-empty, <= reviewEvidenceMaxBytes UTF-8
// bytes of the ORIGINAL untrimmed text).
func validateReviewEvidence(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	return len(text) <= reviewEvidenceMaxBytes
}

// parseOutputEvidence decodes a --output-evidence flag value: a JSON OBJECT
// (never null, an array, or a bare scalar), with every JSON number decoded via
// UseNumber() so the operator's exact digits reach the ledger (the default
// float64 decode loses precision above 2^53, silently changing which value is
// durably persisted as repair evidence).
//
// The size bound on the canonical encoding is enforced by the repair itself,
// which answers a 422-class refusal for an oversized value.
func parseOutputEvidence(raw string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var evidence map[string]any
	if err := decoder.Decode(&evidence); err != nil {
		return nil, fmt.Errorf("output-evidence must be a JSON object: %w", err)
	}
	if evidence == nil {
		return nil, errors.New("output-evidence must be a non-null JSON object")
	}
	return evidence, nil
}
