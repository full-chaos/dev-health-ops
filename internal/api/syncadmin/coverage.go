package syncadmin

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

// coverageProjectionVersion is sync_coverage.py's
// SYNC_COVERAGE_PROJECTION_VERSION, the version the read requires.
const coverageProjectionVersion = 2

// getCoverage is get_sync_config_coverage: the config (404 when absent),
// then build_sync_coverage_summary, which reads the stored projection for
// the config at HISTORY_LOOKBACK_DAYS and the current projection version.
// No row is the 503 "pending" answer with Retry-After. A row's payload is
// dict(payload) with projection_refreshing set from invalidated_at, then
// SyncCoverageSummaryResponse.model_validate: a payload the model refuses
// raises, which is the Python api's unhandled 500. (The route's 422
// "too large" answer is raised only by the projection build, never by this
// read, so it is unreachable here.)
func (h *handlers) getCoverage(w http.ResponseWriter, r *http.Request) {
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	// build_sync_coverage_summary's Info events, same names and fields.
	started := time.Now()
	logContext := []any{
		slog.String("org_id", orgID(r)), slog.String("sync_config_id", config.ID.String()),
		slog.Int("history_lookback_days", synccoverage.HistoryLookbackDays),
	}
	h.logger.InfoContext(r.Context(), "sync_coverage_summary_waiting", logContext...)
	projection, err := h.store.coverageProjection(r.Context(), orgID(r), config.ID,
		synccoverage.HistoryLookbackDays, coverageProjectionVersion)
	if err != nil {
		h.fail(w, r, "coverage_projection", err)
		return
	}
	if projection == nil {
		h.logger.InfoContext(r.Context(), "sync_coverage_projection_pending", logContext...)
		detail := pyjson.NewObject()
		detail.Set("code", "sync_coverage_projection_pending")
		detail.Set("message", "Coverage is being prepared. Retry shortly.")
		body := pyjson.NewObject()
		body.Set("detail", detail)
		policy.WriteJSON(w, http.StatusServiceUnavailable, body, http.Header{"Retry-After": {"30"}})
		return
	}
	stored, err := decodeStored(&projection.Payload)
	if err != nil {
		h.fail(w, r, "decode_coverage_payload", err)
		return
	}
	payload, err := strictDict(stored)
	if err != nil {
		h.fail(w, r, "coverage_payload_dict", err)
		return
	}
	payload.Set("projection_refreshing", projection.Invalidated)
	// The completed event reads payload["overall"]["gap_count"],
	// ["failed_range_count"] and payload["projection_version"] before the
	// model runs: a payload without them raises there, with no event.
	fields, err := coverageCompletedFields(payload)
	if err != nil {
		h.fail(w, r, "coverage_log_fields", err)
		return
	}
	h.logger.InfoContext(r.Context(), "sync_coverage_summary_completed", append(append(logContext,
		slog.Float64("elapsed_seconds", math.Round(time.Since(started).Seconds()*1000)/1000)), fields...)...)
	body, err := coverageSummary(payload)
	if err != nil {
		h.fail(w, r, "coverage_model", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, body, nil)
}

// strictDict is dict(value) with no "or {}": None and other non-iterables
// raise; an empty iterable is an empty dict.
func strictDict(value pyjson.Value) (*pyjson.Object, error) {
	switch value.(type) {
	case *pyjson.Object, []pyjson.Value, string:
		return plainDict(value)
	}
	return nil, fmt.Errorf("dict(%s): not iterable", pyjson.Repr(value))
}

// model is one pydantic model's validation of a dict input (python mode,
// extra keys ignored). Every failure is an error: the route answers 500
// for any of them, so which one pydantic would report first is invisible.
type model struct {
	in  *pyjson.Object
	out *pyjson.Object
	err error
}

func newModel(value pyjson.Value, name string) *model {
	object, ok := value.(*pyjson.Object)
	if !ok {
		return &model{out: pyjson.NewObject(), err: fmt.Errorf("%s: model_type, got %s", name, pyjson.Repr(value))}
	}
	return &model{in: object, out: pyjson.NewObject()}
}

func (m *model) fail(field string, format string, args ...any) {
	if m.err == nil {
		m.err = fmt.Errorf("%s: %s", field, fmt.Sprintf(format, args...))
	}
}

// field is the input's value for a field; present is false when absent.
func (m *model) field(name string) (pyjson.Value, bool) {
	if m.in == nil {
		return nil, false
	}
	return m.in.Get(name)
}

// required reports the value of a field with no default.
func (m *model) required(name string) (pyjson.Value, bool) {
	value, present := m.field(name)
	if !present {
		m.fail(name, "missing")
	}
	return value, present
}

func (m *model) str(name string) {
	value, present := m.required(name)
	if !present {
		return
	}
	text, ok := value.(string)
	if !ok {
		m.fail(name, "string_type, got %s", pyjson.Repr(value))
		return
	}
	m.out.Set(name, text)
}

func (m *model) integer(name string) {
	value, present := m.required(name)
	if !present {
		return
	}
	number, kind, _ := pybody.PydanticInt(value)
	if kind != "" {
		m.fail(name, "%s, got %s", kind, pyjson.Repr(value))
		return
	}
	m.out.Set(name, pyjson.Int{Int: number})
}

// boolean is a bool field; hasDefault is `= False`.
func (m *model) boolean(name string, hasDefault bool) {
	value, present := m.field(name)
	if !present {
		if hasDefault {
			m.out.Set(name, false)
		} else {
			m.fail(name, "missing")
		}
		return
	}
	parsed, kind, _ := pybody.PydanticBool(value)
	if kind != "" {
		m.fail(name, "%s, got %s", kind, pyjson.Repr(value))
		return
	}
	m.out.Set(name, parsed)
}

// datetime is a `datetime` field (required) or `datetime | None = None`.
func (m *model) datetime(name string, optional bool) {
	value, present := m.field(name)
	if !present {
		if optional {
			m.out.Set(name, nil)
		} else {
			m.fail(name, "missing")
		}
		return
	}
	if value == nil && optional {
		m.out.Set(name, nil)
		return
	}
	parsed, failure := pybody.PydanticDatetime(value)
	if failure != nil {
		m.fail(name, "%s, got %s", failure.Type, pyjson.Repr(value))
		return
	}
	m.out.Set(name, pytime.Pydantic(parsed))
}

// literal is a Literal[...] str field (required), or `Literal[...] | None
// = None` when optional.
func (m *model) literal(name string, optional bool, allowed ...string) {
	value, present := m.field(name)
	if !present {
		if optional {
			m.out.Set(name, nil)
		} else {
			m.fail(name, "missing")
		}
		return
	}
	if value == nil && optional {
		m.out.Set(name, nil)
		return
	}
	if text, ok := value.(string); ok {
		for _, candidate := range allowed {
			if text == candidate {
				m.out.Set(name, text)
				return
			}
		}
	}
	m.fail(name, "literal_error, got %s", pyjson.Repr(value))
}

// list is a list field; item validates one element. hasDefault is
// `Field(default_factory=list)`.
func (m *model) list(name string, hasDefault bool, item func(pyjson.Value) (pyjson.Value, error)) {
	value, present := m.field(name)
	if !present {
		if hasDefault {
			m.out.Set(name, []pyjson.Value{})
		} else {
			m.fail(name, "missing")
		}
		return
	}
	items, ok := value.([]pyjson.Value)
	if !ok {
		m.fail(name, "list_type, got %s", pyjson.Repr(value))
		return
	}
	out := make([]pyjson.Value, 0, len(items))
	for index, element := range items {
		validated, err := item(element)
		if err != nil {
			m.fail(fmt.Sprintf("%s.%d", name, index), "%v", err)
			return
		}
		out = append(out, validated)
	}
	m.out.Set(name, out)
}

// nested is a field holding another model (required).
func (m *model) nested(name string, validate func(pyjson.Value) (pyjson.Value, error)) {
	value, present := m.required(name)
	if !present {
		return
	}
	validated, err := validate(value)
	if err != nil {
		m.fail(name, "%v", err)
		return
	}
	m.out.Set(name, validated)
}

func (m *model) result() (pyjson.Value, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.out, nil
}

func strItem(value pyjson.Value) (pyjson.Value, error) {
	text, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("string_type, got %s", pyjson.Repr(value))
	}
	return text, nil
}

var (
	coverageHealth = []string{"healthy", "stale", "gaps", "failed", "insufficient_data"}
	coverageStatus = []string{"healthy", "stale", "gaps", "failed", "insufficient_data",
		"paused", "not_scheduled", "running", "not_enabled"}
)

// coverageSummary is SyncCoverageSummaryResponse.model_validate, dumped in
// the model's field order.
func coverageSummary(payload *pyjson.Object) (pyjson.Value, error) {
	m := newModel(payload, "SyncCoverageSummaryResponse")
	m.str("config_id")
	m.str("provider")
	m.datetime("generated_at", false)
	m.literal("data_basis", false, "planner", "legacy")
	m.integer("history_lookback_days")
	m.datetime("truncated_before", false)
	m.datetime("coverage_since", true)
	m.datetime("coverage_through", true)
	m.boolean("is_truncated", true)
	m.literal("truncation_reason", true, "lookback_limit")
	m.integer("projection_version")
	m.boolean("projection_complete", false)
	m.boolean("projection_refreshing", true)
	m.nested("overall", coverageOverall)
	m.list("datasets", false, coverageDataset)
	m.list("sources", false, coverageSource)
	m.list("backfill_windows", true, coverageBackfillWindow)
	return m.result()
}

func coverageOverall(value pyjson.Value) (pyjson.Value, error) {
	m := newModel(value, "SyncCoverageOverall")
	m.literal("health", false, coverageHealth...)
	m.datetime("latest_successful_run_at", true)
	m.datetime("latest_covered_through", true)
	m.datetime("next_scheduled_run_at", true)
	m.integer("gap_count")
	m.integer("stale_dataset_count")
	m.integer("failed_range_count")
	return m.result()
}

func coverageDataset(value pyjson.Value) (pyjson.Value, error) {
	m := newModel(value, "SyncCoverageDataset")
	m.str("dataset_key")
	m.literal("status", false, coverageStatus...)
	m.datetime("covered_through", true)
	for _, name := range []string{"requested_ranges", "covered_ranges", "gaps", "stale_ranges", "failed_ranges"} {
		m.list(name, true, coverageRangeModel)
	}
	return m.result()
}

func coverageRangeModel(value pyjson.Value) (pyjson.Value, error) {
	m := newModel(value, "SyncCoverageRange")
	m.datetime("since", false)
	m.datetime("before", false)
	m.list("source_ids", true, strItem)
	m.list("run_ids", true, strItem)
	return m.result()
}

func coverageSource(value pyjson.Value) (pyjson.Value, error) {
	m := newModel(value, "SyncCoverageSource")
	m.str("source_id")
	m.str("source_name")
	m.literal("status", false, coverageStatus...)
	m.datetime("covered_through", true)
	m.integer("gap_count")
	m.integer("failed_range_count")
	return m.result()
}

func coverageBackfillWindow(value pyjson.Value) (pyjson.Value, error) {
	m := newModel(value, "SyncCoverageBackfillWindow")
	m.awareBoundary("since")
	m.awareBoundary("before")
	m.list("source_ids", true, strItem)
	m.list("dataset_keys", true, strItem)
	m.list("reasons", true, func(item pyjson.Value) (pyjson.Value, error) {
		if text, ok := item.(string); ok && (text == "gap" || text == "failed") {
			return text, nil
		}
		return nil, fmt.Errorf("literal_error, got %s", pyjson.Repr(item))
	})
	return m.result()
}

// errNaive is AwareDatetime's timezone_aware failure.
var errNaive = errors.New("timezone_aware")

// awareBoundary is SyncCoverageBackfillWindow's `since`/`before`: the
// _assume_utc before-validator (a str datetime.fromisoformat reads is that
// datetime, UTC attached when naive; any other str goes on unchanged), then
// AwareDatetime (pydantic's datetime, refused when naive).
func (m *model) awareBoundary(name string) {
	value, present := m.required(name)
	if !present {
		return
	}
	if text, ok := value.(string); ok {
		if parsed, ok := pytime.FromISOFormat(text); ok {
			if !parsed.Aware {
				parsed.Aware, parsed.Offset = true, 0
			}
			m.out.Set(name, pytime.Pydantic(parsed))
			return
		}
	}
	parsed, failure := pybody.PydanticDatetime(value)
	if failure != nil {
		m.fail(name, "%s, got %s", failure.Type, pyjson.Repr(value))
		return
	}
	if !parsed.Aware {
		m.fail(name, "%v, got %s", errNaive, pyjson.Repr(value))
		return
	}
	m.out.Set(name, pytime.Pydantic(parsed))
}

// coverageCompletedFields reads the completed event's payload fields as
// build_sync_coverage_summary subscripts them; a missing key or a
// non-dict overall is the KeyError/TypeError Python raises.
func coverageCompletedFields(payload *pyjson.Object) ([]any, error) {
	overallValue, _ := payload.Get("overall")
	overall, ok := overallValue.(*pyjson.Object)
	if !ok {
		return nil, fmt.Errorf("payload overall is %s, not a dict", pyjson.Repr(overallValue))
	}
	gaps, hasGaps := overall.Get("gap_count")
	failed, hasFailed := overall.Get("failed_range_count")
	version, hasVersion := payload.Get("projection_version")
	if !hasGaps || !hasFailed || !hasVersion {
		return nil, errors.New("payload lacks overall.gap_count, overall.failed_range_count or projection_version")
	}
	refreshing, _ := payload.Get("projection_refreshing")
	return []any{
		logValue("gap_count", gaps), logValue("failed_range_count", failed),
		logValue("projection_version", version), logValue("projection_refreshing", refreshing),
	}, nil
}

// logValue is a stored JSON value as a log attribute carrying its JSON
// form, as the Python JSON logger writes the extra: an integer of any size
// a number, a string quoted, a bool, null, a list or an object as JSON. A
// value json.dumps refuses falls back to its repr.
func logValue(key string, value pyjson.Value) slog.Attr {
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		return slog.String(key, pyjson.Repr(value))
	}
	return slog.Any(key, loggedJSON(encoded))
}

// loggedJSON is a log value already in JSON form: a JSON log handler embeds
// it as is (a big integer stays a number), a text handler prints its text.
type loggedJSON []byte

func (value loggedJSON) MarshalJSON() ([]byte, error) { return value, nil }

func (value loggedJSON) MarshalText() ([]byte, error) { return value, nil }
