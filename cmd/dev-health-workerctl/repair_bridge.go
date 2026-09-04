package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Byte bounds mirrored from the bridge's own Pydantic models (codex round 1,
// P2) -- worker_workgraph.py's RepairRequest.review_evidence and
// worker_metrics.py's MetricExecutionRepairRequest.review_evidence both bound
// review evidence at 2048 UTF-8 bytes; both models' output_evidence is bound
// by _MAX_EVIDENCE_BYTES=4096 on its CANONICAL re-encoding. Checking these
// locally saves a guaranteed-422 round trip; the bridge's own check is still
// the actual authority (these are pre-checks, not the enforcement point).
const (
	reviewEvidenceMaxBytes = 2048
	outputEvidenceMaxBytes = 4096
)

// validateReviewEvidence reports whether text is a non-empty, in-bound
// review-evidence string (trimmed non-empty, <= reviewEvidenceMaxBytes UTF-8
// bytes of the ORIGINAL untrimmed text, matching the bridge's own
// `len(text.encode())` check on the field as submitted).
func validateReviewEvidence(text string) bool {
	if strings.TrimSpace(text) == "" {
		return false
	}
	return len(text) <= reviewEvidenceMaxBytes
}

// parseOutputEvidence decodes a --output-evidence flag value the same way
// the bridge's own confirm_succeeded contract requires: a JSON OBJECT (never
// null, an array, or a bare scalar -- codex round 1, P2: `--output-evidence
// null` previously unmarshalled successfully into a nil map and was sent as
// literal JSON `null`, which both bridge models reject only server-side),
// with every JSON number decoded via UseNumber() so re-marshalling preserves
// the operator's exact digits (codex round 1, P1: the default float64
// decode loses precision above 2^53, silently changing which value gets
// durably persisted as repair evidence -- e.g. a sequence number attesting
// 9007199254740993 was previously re-sent as 9007199254740992). The
// re-encoded size is checked against outputEvidenceMaxBytes as a stand-in
// for the bridge's own canonical-encoding bound; the bridge remains the
// actual authority since Go's json.Marshal key ordering is not guaranteed
// byte-identical to Python's canonical form.
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
	reencoded, err := json.Marshal(evidence)
	if err != nil {
		return nil, err
	}
	if len(reencoded) > outputEvidenceMaxBytes {
		return nil, fmt.Errorf("output-evidence exceeds %d bytes re-encoded", outputEvidenceMaxBytes)
	}
	return evidence, nil
}

// postWorkerBridge posts a JSON payload to the operational bridge at path,
// authenticating with the token resolved from tokenEnvKey. It is the shared
// client every operator repair verb uses (CHAOS-5042's workgraph/metric
// execution repairs, plus the pre-existing `metrics daily-redrive` ledger
// call) so the auth/error/timeout shape stays in exactly one place --
// mirrors what redriveDailyMetricsLedgerChunk did inline before this file
// existed (WORKER_OPERATIONAL_BRIDGE_URL base, Bearer token, 30s timeout,
// 64KiB response cap).
//
// It returns the HTTP status code and the decoded JSON body regardless of
// status -- CHAOS-5042's repair verbs print the bridge's response verbatim
// on every outcome (including 401/409/422/500), not just 2xx, so an operator
// can see exactly why a repair was refused. err is non-nil only for a
// transport-level failure (bad config, network error, undecodable body) that
// never produced a real bridge response to show.
//
// The token itself is NEVER included in the returned error or logged --
// resolveRequired's platformsecrets.Value keeps it out of %v/%s formatting,
// and Reveal() is called only to build the Authorization header below.
func postWorkerBridge(
	ctx context.Context, tokenEnvKey, path string, payload map[string]any,
) (statusCode int, body map[string]any, err error) {
	baseURL, ok := resolveRequired("WORKER_OPERATIONAL_BRIDGE_URL", os.LookupEnv)
	if !ok {
		return 0, nil, errors.New("WORKER_OPERATIONAL_BRIDGE_URL is not configured")
	}
	token, ok := resolveRequired(tokenEnvKey, os.LookupEnv)
	if !ok {
		return 0, nil, fmt.Errorf("%s is not configured", tokenEnvKey)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	requestURL := strings.TrimRight(baseURL.Reveal(), "/") + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token.Reveal())
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil {
		return 0, nil, err
	}
	var decoded map[string]any
	if len(responseBody) > 0 {
		// codex round 1, P1: an earlier version substituted
		// {"raw_response": <text>} here on an unmarshal failure and returned
		// nil error -- redriveDailyMetricsLedgerChunk's type-asserted reads
		// (chunkResult["repaired"].(float64), comma-ok) then silently treated
		// a malformed/truncated 200 body as {repaired:0, skipped_claim_active:0},
		// which reads as "fully repaired, nothing to skip" and lets
		// dispatchMetrics proceed to redrive partitions against an UNREPAIRED
		// ledger -- reintroducing the exact CHAOS-4304 hazard the ledger-repair
		// gate exists to prevent. A response the bridge sent but this CLI
		// cannot parse must be a hard error, never a decodable-looking zero
		// value a caller's arithmetic can silently trust.
		if unmarshalErr := json.Unmarshal(responseBody, &decoded); unmarshalErr != nil {
			return response.StatusCode, nil, fmt.Errorf(
				"bridge returned status %d with an undecodable body (%d bytes): %w",
				response.StatusCode, len(responseBody), unmarshalErr,
			)
		}
	}
	return response.StatusCode, decoded, nil
}
