package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"reflect"
	"strconv"
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
// 9007199254740993 was previously re-sent as 9007199254740992). The size
// bound is checked against pythonCanonicalJSONByteLen, not a plain
// json.Marshal length (codex round 2, P2: Go's json.Marshal emits raw UTF-8
// for non-ASCII characters, but both bridge models' _canonical/_canonical_json
// call Python's json.dumps with its DEFAULT ensure_ascii=True, which
// \uXXXX-escapes every non-ASCII character -- a 4094-byte Go encoding of
// 1362 euro-sign characters canonicalizes to 8180 bytes in Python, so the
// naive Go length let a payload the bridge would reject pass the local
// check).
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
	canonicalLength, err := pythonCanonicalJSONByteLen(evidence)
	if err != nil {
		return nil, err
	}
	if canonicalLength > outputEvidenceMaxBytes {
		return nil, fmt.Errorf("output-evidence exceeds %d bytes canonicalized", outputEvidenceMaxBytes)
	}
	return evidence, nil
}

// pythonCanonicalJSONByteLen returns the byte length v would have under
// Python's `json.dumps(v, sort_keys=True, separators=(",", ":"))` -- the
// exact call both `_canonical` (worker_workgraph.py:60) and
// `_canonical_json` (worker_metrics.py:868) make, with json.dumps' DEFAULT
// ensure_ascii=True in effect (neither call passes ensure_ascii=False).
//
// This walks v RECURSIVELY and re-prices each scalar under Python's own
// rules, rather than trusting Go's json.Marshal output (codex round 2, P2:
// Go emits raw UTF-8 for non-ASCII characters, Python's ensure_ascii
// \uXXXX-escapes them -- fixed below in pythonJSONStringByteLen; codex
// round 3, P2: json.Number PRESERVES THE OPERATOR'S LITERAL LEXEME, e.g.
// "1e+09", but Python's json.loads parses any number containing '.'/'e'/'E'
// as a float and Python's json.dumps re-serializes that float via its own
// repr -- "1e+09" canonicalizes to "1000000000.0", not "1e+09" -- so
// re-marshaling the Go-side json.Number verbatim silently undercounts any
// payload using exponent/decimal notation; fixed via pythonNumberRepr).
// v must be built from a parseOutputEvidence-style decode (UseNumber()):
// map[string]any, []any, json.Number, string, bool, or nil.
func pythonCanonicalJSONByteLen(v any) (int, error) {
	switch value := v.(type) {
	case nil:
		return len("null"), nil
	case bool:
		if value {
			return len("true"), nil
		}
		return len("false"), nil
	case string:
		return pythonJSONStringByteLen(value), nil
	case json.Number:
		repr, err := pythonNumberRepr(value)
		if err != nil {
			return 0, err
		}
		return len(repr), nil
	case float64:
		// Reachable only if a caller passes a plain Go float64 rather than
		// a UseNumber()-decoded json.Number; kept for defensiveness, not
		// exercised by parseOutputEvidence's own decode path.
		return len(pythonFloatRepr(value)), nil
	case map[string]any:
		length := 2 // { }
		first := true
		for key, mapValue := range value {
			if !first {
				length++ // ,
			}
			first = false
			length += pythonJSONStringByteLen(key)
			length++ // :
			valueLength, err := pythonCanonicalJSONByteLen(mapValue)
			if err != nil {
				return 0, err
			}
			length += valueLength
		}
		return length, nil
	case []any:
		length := 2 // [ ]
		for index, item := range value {
			if index > 0 {
				length++ // ,
			}
			itemLength, err := pythonCanonicalJSONByteLen(item)
			if err != nil {
				return 0, err
			}
			length += itemLength
		}
		return length, nil
	default:
		return 0, fmt.Errorf("pythonCanonicalJSONByteLen: unsupported type %T", v)
	}
}

// pythonJSONStringByteLen is the byte length Python's json.dumps(s) (with
// its default ensure_ascii=True) would produce for the JSON-string encoding
// of s, including its two surrounding quotes.
func pythonJSONStringByteLen(s string) int {
	length := 2 // the opening and closing quote
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			length += 2 // \" or \\
		case r == '\n' || r == '\r' || r == '\t':
			length += 2 // \n, \r, or \t
		case r < 0x20:
			length += 6 // \u00XX for any other control character
		case r < 0x80:
			length++
		case r > 0xFFFF:
			length += 12 // a UTF-16 surrogate pair, two \uXXXX escapes
		default:
			length += 6 // one \uXXXX escape
		}
	}
	return length
}

// pythonNumberRepr returns the exact text Python's json.dumps would emit
// for the JSON number number decodes to. A lexeme with no '.', 'e', or 'E'
// parses in Python as an arbitrary-precision int and round-trips through
// json.dumps UNCHANGED (Go's json.Number already preserves this lexeme
// verbatim, which is round 1's UseNumber() fix). A lexeme WITH any of
// those (decimal point or exponent) parses as a Python float, which
// json.dumps re-serializes via float repr -- never the original lexeme.
func pythonNumberRepr(number json.Number) (string, error) {
	lexeme := number.String()
	if !strings.ContainsAny(lexeme, ".eE") {
		return lexeme, nil
	}
	value, err := number.Float64()
	if err != nil {
		return "", fmt.Errorf("output-evidence numeric value %q: %w", lexeme, err)
	}
	return pythonFloatRepr(value), nil
}

// pythonFloatRepr renders f the way CPython's float.__repr__ (which
// json.dumps calls for every Python float) does: the shortest decimal
// string that round-trips to f, in FIXED notation with an explicit ".0" for
// a whole number whenever f's decimal exponent is in [-4, 17) -- CPython's
// own threshold -- and in scientific notation outside that range. Go's
// strconv has no equivalent formatter (its 'g' verb picks scientific
// notation based on significant-digit count, not this fixed exponent
// threshold, and never appends ".0"), so this derives the exponent from
// Go's own shortest round-trip scientific form and re-renders it under
// Python's rule.
func pythonFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	scientific := strconv.FormatFloat(f, 'e', -1, 64)
	exponent := pythonFloatSciExponent(scientific)
	if exponent >= -4 && exponent < 17 {
		fixed := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.ContainsRune(fixed, '.') {
			fixed += ".0"
		}
		return fixed
	}
	// Residual, not byte-proven against Python at this magnitude: outside
	// [-4, 17) both Go's 'e' verb and Python's repr use scientific
	// notation with a signed, zero-padded-to-2-digit exponent, and every
	// measured case agrees -- but no evidence payload realistically
	// reaches 1e17 or 1e-4, so this is intentionally not chased further.
	return scientific
}

// pythonFloatSciExponent extracts the decimal exponent from a Go
// strconv.FormatFloat(f, 'e', -1, 64) string (e.g. "1e+09" -> 9,
// "1.234e-05" -> -5).
func pythonFloatSciExponent(scientific string) int {
	index := strings.IndexByte(scientific, 'e')
	if index < 0 {
		return 0
	}
	exponent, err := strconv.Atoi(scientific[index+1:])
	if err != nil {
		return 0
	}
	return exponent
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
