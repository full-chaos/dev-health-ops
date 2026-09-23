package customerpush

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// maxEnvelopeErrors is the admin route's cap on envelope error rows.
const maxEnvelopeErrors = 50

// rejectedRow is AdminRejectedRecordResponse.
func rejectedRow(index int, kind string, externalID pyjson.Value, code, message string, path pyjson.Value) *pyjson.Object {
	row := pyjson.NewObject()
	row.Set("index", int64(index))
	row.Set("kind", kind)
	row.Set("external_id", externalID)
	row.Set("code", code)
	row.Set("message", message)
	row.Set("path", path)
	return row
}

// validateResponse is AdminValidateResponse.
func validateResponse(valid bool, accepted, rejected int, rows []pyjson.Value) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("valid", valid)
	out.Set("items_accepted", int64(accepted))
	out.Set("items_rejected", int64(rejected))
	if rows == nil {
		rows = []pyjson.Value{}
	}
	out.Set("errors", rows)
	return out
}

// validateFailure is _validate_failure: an envelope-level failure as one
// row of a 200 valid:false result.
func validateFailure(code, message string, path pyjson.Value) *pyjson.Object {
	return validateResponse(false, 0, 0, []pyjson.Value{rejectedRow(0, "unknown", nil, code, message, path)})
}

// errTooLarge is ExternalIngestError(413, "payload_too_large").
var errTooLarge = errors.New("payload_too_large")

// readBodyLimited is router.py _read_body_enforcing_size_limit: a
// Content-Length above the limit fails fast (int() of the header; a value
// int() refuses is ignored), and otherwise the streamed bytes are counted.
func readBodyLimited(r *http.Request, maxBytes *big.Int) ([]byte, error) {
	if header := r.Header.Get("Content-Length"); header != "" {
		if length, err := pythonparity.ParseInt(header); err == nil && length.Cmp(maxBytes) > 0 {
			return nil, errTooLarge
		}
	}
	limit := int64(math.MaxInt64)
	if maxBytes.IsInt64() {
		limit = maxBytes.Int64()
	}
	if limit < 0 {
		limit = -1
	}
	probe := limit + 1
	if limit == math.MaxInt64 {
		probe = limit // limit+1 would wrap negative and read nothing
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, probe))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errTooLarge
	}
	return body, nil
}

// locPath is ".".join(str(part) for part in loc) or None.
func locPath(loc []pyjson.Value) pyjson.Value {
	parts := make([]string, len(loc))
	for index, part := range loc {
		switch typed := part.(type) {
		case string:
			parts[index] = typed
		case int64:
			parts[index] = strconv.FormatInt(typed, 10)
		default:
			parts[index] = fmt.Sprint(typed)
		}
	}
	if joined := strings.Join(parts, "."); joined != "" {
		return joined
	}
	return nil
}

// validateSource is validate_source_payload: the access gate and the
// source's scope, then the batch envelope validated as the data plane's
// POST /validate does, every outcome a 200 result.
func (h *handlers) validateSource(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	if _, ok := h.loadSource(w, r, orgID, r.PathValue("source_id")); !ok {
		return
	}
	// _max_body_bytes(): int() of EXTERNAL_INGEST_MAX_BODY_BYTES alone.
	rawMax, set := h.getenv("EXTERNAL_INGEST_MAX_BODY_BYTES")
	if !set {
		rawMax = fmt.Sprint(externalingest.DefaultLimits.MaxBodyBytes)
	}
	maxBytes, err := pythonparity.ParseInt(rawMax)
	if err != nil {
		h.internal(w, r, "read ingest body limit", err)
		return
	}
	raw, err := readBodyLimited(r, maxBytes)
	if errors.Is(err, errTooLarge) {
		policy.WriteJSON(w, http.StatusOK, validateFailure("payload_too_large", "Request body exceeds "+maxBytes.String()+" bytes", nil), nil)
		return
	}
	if err != nil {
		h.internal(w, r, "read validate body", err)
		return
	}
	envelope, envelopeErrs, err := recordvalidation.ValidateEnvelopeJSON(raw)
	if err != nil {
		h.internal(w, r, "validate envelope", err)
		return
	}
	if len(envelopeErrs) > 0 {
		if len(envelopeErrs) > maxEnvelopeErrors {
			envelopeErrs = envelopeErrs[:maxEnvelopeErrors]
		}
		rows := make([]pyjson.Value, len(envelopeErrs))
		for index, item := range envelopeErrs {
			rows[index] = rejectedRow(0, "unknown", nil, "invalid_envelope", item.Msg, locPath(item.Loc))
		}
		policy.WriteJSON(w, http.StatusOK, validateResponse(false, 0, 0, rows), nil)
		return
	}
	if envelope.SchemaVersion != externalingest.SchemaVersion {
		policy.WriteJSON(w, http.StatusOK, validateFailure("unsupported_schema_version",
			"Unsupported schemaVersion: "+pythonparity.StrRepr(envelope.SchemaVersion), "schemaVersion"), nil)
		return
	}
	limits, err := h.limits()
	if err != nil {
		h.internal(w, r, "read ingest limits", err)
		return
	}
	maxRecordsValue, _ := limits.Get("maxRecordsPerBatch")
	maxRecords := maxRecordsValue.(pyjson.Int).Int
	if big.NewInt(int64(len(envelope.Records))).Cmp(maxRecords) > 0 {
		policy.WriteJSON(w, http.StatusOK, validateFailure("batch_too_large",
			fmt.Sprintf("Batch has %d records; max is %s", len(envelope.Records), maxRecords.String()), "records"), nil)
		return
	}
	inputs := make([]recordvalidation.RecordInput, len(envelope.Records))
	for index, record := range envelope.Records {
		inputs[index] = recordvalidation.RecordInput{Kind: record.Kind, Payload: record.Payload}
	}
	items := recordvalidation.ValidateRecords(inputs)
	rejected := map[int]bool{}
	rows := make([]pyjson.Value, len(items))
	for index, item := range items {
		rejected[item.Index] = true
		var externalID pyjson.Value
		if item.Index >= 0 && item.Index < len(envelope.Records) {
			externalID = envelope.Records[item.Index].ExternalID
		}
		rows[index] = rejectedRow(item.Index, item.Kind, externalID, item.Code, item.Message, item.Path)
	}
	policy.WriteJSON(w, http.StatusOK, validateResponse(len(items) == 0, len(envelope.Records)-len(rejected), len(rejected), rows), nil)
}
