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
	"reflect"
	"strings"
	"time"
)

// reviewEvidenceMaxBytes mirrors the bridge's own Pydantic bound
// (worker_workgraph.py's RepairRequest.review_evidence /
// worker_metrics.py's MetricExecutionRepairRequest.review_evidence, both
// 2048 UTF-8 bytes on the field as submitted -- a plain string, not a JSON
// re-encoding, so a plain len() check is exact, unlike output_evidence's
// size bound below). Checking locally saves a guaranteed-422 round trip;
// the bridge's own check is still the actual authority.
const reviewEvidenceMaxBytes = 2048

// bridgeResponseBodyCap bounds how much of a bridge response postWorkerBridge
// reads. postWorkerBridge reads bridgeResponseBodyCap+1 bytes and treats a
// body that fills that extra byte as OVER the cap (codex round 3, P1) --
// never silently accepting a truncated PREFIX that happens to parse as
// complete, valid JSON.
const bridgeResponseBodyCap = 1 << 16

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
// 9007199254740993 was previously re-sent as 9007199254740992).
//
// This deliberately does NOT locally enforce the bridge's output_evidence
// size bound (_MAX_EVIDENCE_BYTES=4096 on its CANONICAL Python re-encoding,
// worker_workgraph.py:445/worker_metrics.py:686). Two codex rounds
// (round 2, P2: Go's json.Marshal emits raw UTF-8 where Python's
// ensure_ascii \uXXXX-escapes non-ASCII characters; round 3, P2: json.Number
// preserves the operator's literal lexeme, e.g. "1e+09", but Python
// re-serializes any number with '.'/'e'/'E' via its own float repr,
// "1000000000.0") both found real gaps in trying to REPLICATE Python's
// json.dumps canonicalization in Go well enough to predict this bound --
// team-lead's ruling after the second finding: this is the wrong tool for
// the job. The bridge's own 422 IS the size verdict; postWorkerBridge
// already relays it verbatim (see the non-2xx raw_response path), so an
// oversized payload costs one round trip, never a silent acceptance.
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

// requiredFieldValidator is implemented by every strict bridge-2xx-response
// shape decodeStrictBridgeResponse validates against. Codex rounds 1 and 2
// both found the SAME CLASS of defect on the daily-redrive ledger response
// specifically (round 1: an undecodable body silently read as a
// decodable-looking zero result; round 2: a syntactically valid but
// INCOMPLETE body -- `{}`, `null`, or missing one field -- did too), just
// two different instances of it. Per-instance patches (main.go's field
// presence checks, now removed) fixed the one caller that happened to do
// typed field reads; this interface plus postWorkerBridge's generic type
// parameter below make EVERY bridge-response consumer in workerctl declare
// and enforce its own expected 2xx shape, so a future verb cannot add a new
// instance of the same class by skipping validation the way the original
// code did.
type requiredFieldValidator interface {
	// validateRequiredFields reports every required field that decoded as
	// nil (absent, or present with the wrong JSON type -- json.Unmarshal
	// itself already rejects a field whose JSON type cannot convert to the
	// Go field's type, e.g. a string where a number was expected, as a
	// decode error before this method ever runs).
	validateRequiredFields() error
}

// assertAllFieldsArePointers is codex round 3's answer to its own P3: the
// generic constraint on decodeStrictBridgeResponse (`PT interface{*T;
// requiredFieldValidator}`) cannot itself require every field of T to be a
// pointer type -- nothing in the Go type system stops a future
// *BridgeResponse type from adding a non-pointer required field, which
// could then never distinguish "absent" from "present with the JSON zero
// value" the way this whole mechanism exists to guarantee, and nothing
// would catch it except a reviewer reading the struct by eye. This walks
// T's fields via reflection and fails loud if any is not a pointer Kind.
// Called from a test (repair_test.go) once per concrete response type, NOT
// from the request hot path in postWorkerBridge -- reflection cost is
// intentionally kept off every real bridge call.
func assertAllFieldsArePointers[T any]() error {
	structType := reflect.TypeOf(*new(T))
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if field.Type.Kind() != reflect.Pointer {
			return fmt.Errorf(
				"%s.%s is type %s, not a pointer -- every field must be a pointer so an absent field decodes to nil, never a JSON zero value indistinguishable from a real one",
				structType.Name(), field.Name, field.Type,
			)
		}
	}
	return nil
}

// decodeStrictBridgeResponse decodes raw response bytes into a fresh T
// (every field of T MUST be a pointer type, so an absent field decodes to
// nil rather than a zero value indistinguishable from "the bridge said
// zero/empty") and then runs T's own validateRequiredFields. It rejects:
// invalid JSON; a top-level JSON array/scalar where an object was expected
// (a struct-typed Unmarshal target errors on those); a field present with
// the wrong JSON type (also a json.Unmarshal error, e.g. UnmarshalTypeError);
// and -- via validateRequiredFields, since unmarshaling top-level JSON
// `null`/`{}` into a struct pointer succeeds with every field left nil, not
// an error -- any required field that is simply absent.
// DisallowUnknownFields is deliberately NOT set: an unrecognized field the
// bridge adds later must never fail a round-trip, only a missing or
// wrong-typed REQUIRED field does.
func decodeStrictBridgeResponse[T any, PT interface {
	*T
	requiredFieldValidator
}](body []byte) (*T, error) {
	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, fmt.Errorf("bridge response does not match the expected shape: %w", err)
	}
	if err := PT(&value).validateRequiredFields(); err != nil {
		return nil, err
	}
	return &value, nil
}

// redriveLedgerBridgeResponse is `POST /internal/worker/daily-metrics/v1/redrive`'s
// 2xx shape (`_bulk_redrive_ambiguous_executions`'s `return {"repaired":
// repaired, "skipped_claim_active": skipped}`, worker_metrics.py:1535 --
// both Python `int`s, never fractional). Fields are *int64, not *float64
// (codex round 3, P1): with *float64, a non-contract response like
// `"skipped_claim_active": 0.5` passed strict validation (present, right
// Go-side kind) and was then silently TRUNCATED to 0 by main.go's
// `int(skipped)` conversion, reporting "nothing skipped" for a response
// that was itself already anomalous. json.Unmarshal into *int64 rejects a
// JSON number with a fractional part outright (an UnmarshalTypeError), so
// the strict decode itself refuses the response before any caller ever
// sees a value to truncate.
type redriveLedgerBridgeResponse struct {
	Repaired           *int64 `json:"repaired"`
	SkippedClaimActive *int64 `json:"skipped_claim_active"`
}

func (r *redriveLedgerBridgeResponse) validateRequiredFields() error {
	if r.Repaired == nil {
		return errors.New(`bridge response missing required numeric field "repaired"`)
	}
	if r.SkippedClaimActive == nil {
		return errors.New(`bridge response missing required numeric field "skipped_claim_active"`)
	}
	return nil
}

// workgraphRepairBridgeResponse is `POST
// /internal/worker/workgraph/v1/executions/{id}/repair`'s 2xx shape
// (worker_workgraph.py:523's `return {"status": "repaired", "request_id":
// str(request_id)}`).
type workgraphRepairBridgeResponse struct {
	Status    *string `json:"status"`
	RequestID *string `json:"request_id"`
}

func (r *workgraphRepairBridgeResponse) validateRequiredFields() error {
	if r.Status == nil {
		return errors.New(`bridge response missing required string field "status"`)
	}
	if r.RequestID == nil {
		return errors.New(`bridge response missing required string field "request_id"`)
	}
	return nil
}

// metricExecutionRepairBridgeResponse is `POST
// /internal/worker/metric-executions/v1/{id}/repair`'s 2xx shape --
// `_repair_execution` returns `{"status": "already_applied"|"repaired",
// "execution_id": ..., "state": ...}` on both its idempotent-replay path
// (worker_metrics.py:1331-1335) and its real-repair path
// (worker_metrics.py:1409-1413).
type metricExecutionRepairBridgeResponse struct {
	Status      *string `json:"status"`
	ExecutionID *string `json:"execution_id"`
	State       *string `json:"state"`
}

func (r *metricExecutionRepairBridgeResponse) validateRequiredFields() error {
	if r.Status == nil {
		return errors.New(`bridge response missing required string field "status"`)
	}
	if r.ExecutionID == nil {
		return errors.New(`bridge response missing required string field "execution_id"`)
	}
	if r.State == nil {
		return errors.New(`bridge response missing required string field "state"`)
	}
	return nil
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
// transport-level failure (bad config, network error, undecodable body, or a
// 2xx body that fails T's strict shape validation) that never produced a
// usable bridge response.
//
// T is the caller's expected 2xx response shape (one of the
// *BridgeResponse types above) -- postWorkerBridge decodes the response
// TWICE on a 2xx: once loosely into the map[string]any every caller prints
// verbatim regardless of status, and once strictly via
// decodeStrictBridgeResponse[T] purely as a validation gate (its typed
// result is discarded; only a shape error propagates). A non-2xx status
// (401/409/422/500, whose body shape is `{"detail": "..."}`, not T's shape)
// skips strict validation entirely -- only the loose map decode applies, so
// an error response's own shape is never held to a success shape's contract.
//
// The token itself is NEVER included in the returned error or logged --
// resolveRequired's platformsecrets.Value keeps it out of %v/%s formatting,
// and Reveal() is called only to build the Authorization header below.
func postWorkerBridge[T any, PT interface {
	*T
	requiredFieldValidator
}](
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
	// codex round 3, P1: reading exactly bridgeResponseBodyCap bytes with no
	// overflow check let a body LARGER than the cap be silently truncated to
	// a prefix that can still parse as valid, complete-looking JSON (e.g. a
	// legitimate response plus trailing whitespace, cut off right before
	// more data) -- read one byte past the cap so a body that's actually
	// bigger is DETECTED, not silently accepted as a valid-looking prefix.
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, bridgeResponseBodyCap+1))
	if err != nil {
		return 0, nil, err
	}
	if len(responseBody) > bridgeResponseBodyCap {
		return response.StatusCode, nil, fmt.Errorf(
			"bridge response exceeds the %d-byte cap -- refusing rather than silently accepting a truncated prefix",
			bridgeResponseBodyCap,
		)
	}
	var decoded map[string]any
	if len(responseBody) > 0 {
		if unmarshalErr := json.Unmarshal(responseBody, &decoded); unmarshalErr != nil {
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				// codex round 1, P1: an earlier version substituted
				// {"raw_response": <text>} here on ANY status and returned nil
				// error -- redriveDailyMetricsLedgerChunk's type-asserted reads
				// (chunkResult["repaired"].(float64), comma-ok) then silently
				// treated a malformed/truncated 200 body as
				// {repaired:0, skipped_claim_active:0}, which reads as "fully
				// repaired, nothing to skip" and lets dispatchMetrics proceed
				// to redrive partitions against an UNREPAIRED ledger --
				// reintroducing the exact CHAOS-4304 hazard the ledger-repair
				// gate exists to prevent. A 2xx response the bridge sent but
				// this CLI cannot parse must be a hard error, never a
				// decodable-looking zero value a caller's arithmetic can
				// silently trust.
				return response.StatusCode, nil, fmt.Errorf(
					"bridge returned status %d with an undecodable body (%d bytes): %w",
					response.StatusCode, len(responseBody), unmarshalErr,
				)
			}
			// codex round 3, P2: the check above USED TO run unconditionally
			// on every status, not just 2xx -- a real HTTP error (401/409/422/
			// 500/502, a reverse-proxy error page, plain text, or a JSON array/
			// scalar body) whose body isn't a JSON OBJECT was misclassified as
			// a Go transport error identical to "never reached the bridge at
			// all," and every repair verb then discarded the real response and
			// printed only "operator_backend_unavailable" -- hiding exactly the
			// detail (e.g. the bridge's real 422 message) an operator needs.
			// A non-2xx status ALREADY signals failure on its own; unlike the
			// 2xx case above, nothing downstream ever trusts a FIELD out of a
			// non-2xx body, so relaying the raw text is safe, not a repeat of
			// round 1's hazard.
			decoded = map[string]any{"raw_response": string(responseBody)}
		}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// codex round 2, P1: a syntactically VALID but INCOMPLETE 2xx body
		// (`{}`, `null`, or missing a field) decoded successfully above and
		// previously reached a caller's own ad hoc field-presence checks
		// unevenly -- some callers had them, some didn't, and the class kept
		// reappearing. Every 2xx response now goes through the SAME strict
		// shape check regardless of which verb called postWorkerBridge.
		if _, shapeErr := decodeStrictBridgeResponse[T, PT](responseBody); shapeErr != nil {
			return response.StatusCode, nil, fmt.Errorf(
				"bridge returned status %d but its body failed shape validation: %w",
				response.StatusCode, shapeErr,
			)
		}
	}
	return response.StatusCode, decoded, nil
}
