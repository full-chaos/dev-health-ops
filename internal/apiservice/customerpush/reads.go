package customerpush

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// sourceRow is one external_ingest_sources row, as IngestSourceResponse
// reads it.
type sourceRow struct {
	ID                         uuid.UUID
	OrgID                      string
	System                     string
	Instance                   string
	EntityFamily               string
	DisplayName                *string
	Mode                       string
	Enabled                    bool
	WebhookMode                string
	MatchedIntegrationSourceID *uuid.UUID
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
}

const sourceColumns = `id, org_id, system, instance, entity_family, display_name, mode, enabled,
	webhook_mode, matched_integration_source_id, created_at, updated_at`

func scanSource(row pgx.Row) (sourceRow, error) {
	var s sourceRow
	err := row.Scan(&s.ID, &s.OrgID, &s.System, &s.Instance, &s.EntityFamily, &s.DisplayName, &s.Mode, &s.Enabled,
		&s.WebhookMode, &s.MatchedIntegrationSourceID, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

// tokenRow is one external_ingest_tokens row, as IngestTokenResponse reads
// it. Scopes is the json column's stored text.
type tokenRow struct {
	ID         uuid.UUID
	OrgID      string
	SourceID   *uuid.UUID
	Name       string
	Prefix     string
	Scopes     []byte
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
	LastUsedAt *time.Time
	CreatedAt  time.Time
}

const tokenColumns = `id, org_id, source_id, name, token_prefix, scopes, expires_at, revoked_at, last_used_at, created_at`

func scanToken(row pgx.Row) (tokenRow, error) {
	var t tokenRow
	err := row.Scan(&t.ID, &t.OrgID, &t.SourceID, &t.Name, &t.Prefix, &t.Scopes, &t.ExpiresAt, &t.RevokedAt, &t.LastUsedAt, &t.CreatedAt)
	return t, err
}

// pydanticTime is pydantic's JSON form of an aware timestamp read from
// Postgres.
func pydanticTime(at time.Time) string { return pytime.Pydantic(pytime.UTC(at)) }

func optionalTime(at *time.Time) pyjson.Value {
	if at == nil {
		return nil
	}
	return pydanticTime(*at)
}

func optionalString(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

func optionalUUID(value *uuid.UUID) pyjson.Value {
	if value == nil {
		return nil
	}
	return value.String()
}

// sourceResponse is _source_to_response.
func sourceResponse(s sourceRow, warnings []string) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", s.ID.String())
	out.Set("org_id", s.OrgID)
	out.Set("system", s.System)
	out.Set("instance", s.Instance)
	out.Set("entity_family", s.EntityFamily)
	out.Set("display_name", optionalString(s.DisplayName))
	out.Set("mode", s.Mode)
	out.Set("enabled", s.Enabled)
	out.Set("webhook_mode", s.WebhookMode)
	out.Set("matched_integration_source_id", optionalUUID(s.MatchedIntegrationSourceID))
	out.Set("created_at", pydanticTime(s.CreatedAt))
	out.Set("updated_at", pydanticTime(s.UpdatedAt))
	list := make([]pyjson.Value, len(warnings))
	for index, warning := range warnings {
		list[index] = warning
	}
	out.Set("warnings", list)
	return out
}

// scopesValue is list(token.scopes or []) from the json column's text.
func scopesValue(raw []byte) (pyjson.Value, error) {
	if len(raw) == 0 {
		return []pyjson.Value{}, nil
	}
	decoded, err := pyjson.Decode(raw)
	if err != nil {
		return nil, err
	}
	if decoded == nil {
		return []pyjson.Value{}, nil
	}
	list, ok := decoded.([]pyjson.Value)
	if !ok {
		return nil, errors.New("scopes column is not a JSON list")
	}
	for _, item := range list {
		if _, ok := item.(string); !ok {
			return nil, errors.New("scopes column holds a non-string")
		}
	}
	return list, nil
}

// tokenResponse is _token_to_response.
func tokenResponse(t tokenRow) (*pyjson.Object, error) {
	scopes, err := scopesValue(t.Scopes)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", t.ID.String())
	out.Set("org_id", t.OrgID)
	out.Set("source_id", optionalUUID(t.SourceID))
	out.Set("name", t.Name)
	out.Set("token_prefix", t.Prefix)
	out.Set("scopes", scopes)
	out.Set("expires_at", optionalTime(t.ExpiresAt))
	out.Set("revoked_at", optionalTime(t.RevokedAt))
	out.Set("last_used_at", optionalTime(t.LastUsedAt))
	out.Set("created_at", pydanticTime(t.CreatedAt))
	return out, nil
}

// loadSource is _get_org_source: a source id uuid.UUID() refuses, or no
// row in the caller's org, is 404 "Source not found". It answers the
// response itself when ok is false.
func (h *handlers) loadSource(w http.ResponseWriter, r *http.Request, orgID, rawID string) (sourceRow, bool) {
	id, err := pythonparity.ParseUUID(rawID)
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, "Source not found", nil)
		return sourceRow{}, false
	}
	source, err := scanSource(h.pool.QueryRow(r.Context(),
		`SELECT `+sourceColumns+` FROM external_ingest_sources WHERE id = $1 AND org_id = $2`, id, orgID))
	if errors.Is(err, pgx.ErrNoRows) {
		policy.WriteDetail(w, http.StatusNotFound, "Source not found", nil)
		return sourceRow{}, false
	}
	if err != nil {
		h.internal(w, r, "load source", err)
		return sourceRow{}, false
	}
	return source, true
}

func (h *handlers) listSources(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	rows, err := h.pool.Query(r.Context(),
		`SELECT `+sourceColumns+` FROM external_ingest_sources WHERE org_id = $1 ORDER BY created_at DESC`, orgID)
	if err != nil {
		h.internal(w, r, "list sources", err)
		return
	}
	sources, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceRow, error) { return scanSource(row) })
	if err != nil {
		h.internal(w, r, "list sources", err)
		return
	}
	out := make([]pyjson.Value, len(sources))
	for index, source := range sources {
		out[index] = sourceResponse(source, nil)
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h *handlers) getSource(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	source, ok := h.loadSource(w, r, orgID, r.PathValue("source_id"))
	if !ok {
		return
	}
	policy.WriteModel(w, http.StatusOK, sourceResponse(source, nil), nil)
}

func (h *handlers) writeTokens(w http.ResponseWriter, r *http.Request, query string, args ...any) {
	rows, err := h.pool.Query(r.Context(), query, args...)
	if err != nil {
		h.internal(w, r, "list tokens", err)
		return
	}
	tokens, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (tokenRow, error) { return scanToken(row) })
	if err != nil {
		h.internal(w, r, "list tokens", err)
		return
	}
	out := make([]pyjson.Value, len(tokens))
	for index, token := range tokens {
		rendered, err := tokenResponse(token)
		if err != nil {
			h.internal(w, r, "render token", err)
			return
		}
		out[index] = rendered
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h *handlers) listSourceTokens(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	source, ok := h.loadSource(w, r, orgID, r.PathValue("source_id"))
	if !ok {
		return
	}
	h.writeTokens(w, r, `SELECT `+tokenColumns+` FROM external_ingest_tokens
		WHERE org_id = $1 AND source_id = $2 ORDER BY created_at DESC`, orgID, source.ID)
}

func (h *handlers) listOrgTokens(w http.ResponseWriter, r *http.Request) {
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	h.writeTokens(w, r, `SELECT `+tokenColumns+` FROM external_ingest_tokens
		WHERE org_id = $1 ORDER BY created_at DESC`, orgID)
}

func int64Pointer(value int64) *int64 { return &value }

// batchListItem is _batch_to_admin_list_item.
func batchListItem(row externalingest.BatchRow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("ingestion_id", row.IngestionID.String())
	out.Set("status", row.Status)
	out.Set("source_system", row.SourceSystem)
	out.Set("source_instance", row.SourceInstance)
	out.Set("producer", optionalString(row.Producer))
	out.Set("items_received", row.ItemsReceived)
	out.Set("items_accepted", row.ItemsAccepted)
	out.Set("items_rejected", row.ItemsRejected)
	out.Set("created_at", pydanticTime(row.CreatedAt))
	out.Set("completed_at", optionalTime(row.CompletedAt))
	return out
}

func (h *handlers) listSourceBatches(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	var errs pybody.Errors
	status := pybody.LastQuery(values, "status")
	producer := pybody.LastQuery(values, "producer")
	from, _ := errs.QueryDatetime("from", pybody.LastQuery(values, "from"))
	to, _ := errs.QueryDatetime("to", pybody.LastQuery(values, "to"))
	limit, _ := errs.QueryInt("limit", pybody.LastQuery(values, "limit"), 50, int64Pointer(1), int64Pointer(200))
	offset, _ := errs.QueryInt("offset", pybody.LastQuery(values, "offset"), 0, int64Pointer(0), nil)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	source, ok := h.loadSource(w, r, orgID, r.PathValue("source_id"))
	if !ok {
		return
	}
	// pydantic's int is unbounded; asyncpg refuses an offset past int64 when
	// it binds the page query, Python's unhandled 500.
	if !offset.IsInt64() {
		h.internal(w, r, "list batches", errors.New("offset does not fit a bigint"))
		return
	}
	rows, total, err := externalingest.ListBatches(r.Context(), h.pool, orgID, externalingest.BatchQuery{
		SourceSystem: &source.System, SourceInstance: &source.Instance, Status: status, Producer: producer,
		CreatedAfter: pybody.Instant(from), CreatedBefore: pybody.Instant(to), Limit: int(limit.Int64()), Offset: int(offset.Int64()),
	})
	if err != nil {
		h.internal(w, r, "list batches", err)
		return
	}
	items := make([]pyjson.Value, len(rows))
	for index, row := range rows {
		items[index] = batchListItem(row)
	}
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", total)
	out.Set("limit", limit.Int64())
	out.Set("offset", offset.Int64())
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h *handlers) getBatch(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	var errs pybody.Errors
	limit, _ := errs.QueryInt("rejected_records_limit", pybody.LastQuery(values, "rejected_records_limit"), 50, int64Pointer(1), int64Pointer(200))
	offset, _ := errs.QueryInt("rejected_records_offset", pybody.LastQuery(values, "rejected_records_offset"), 0, int64Pointer(0), nil)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	orgID := policy.UserFrom(r.Context()).OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	id, err := pythonparity.ParseUUID(r.PathValue("ingestion_id"))
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, "Batch not found", nil)
		return
	}
	batch, err := externalingest.GetBatch(r.Context(), h.pool, orgID, id)
	if err != nil {
		h.internal(w, r, "load batch", err)
		return
	}
	if batch == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Batch not found", nil)
		return
	}
	if !offset.IsInt64() {
		h.internal(w, r, "load batch", errors.New("rejected_records_offset does not fit a bigint"))
		return
	}
	out, err := h.batchDetail(r.Context(), orgID, *batch, int(limit.Int64()), int(offset.Int64()))
	if err != nil {
		h.internal(w, r, "render batch", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// batchDetail is _batch_to_admin_response.
func (h *handlers) batchDetail(ctx context.Context, orgID string, batch externalingest.BatchRow, limit, offset int) (*pyjson.Object, error) {
	rejections, total, err := externalingest.ListRejections(ctx, h.pool, orgID, batch.IngestionID, limit, offset)
	if err != nil {
		return nil, err
	}
	// The batch row's JSON columns were already read the way status.py's
	// _parse_json reads them (a value that is not a dict never reaches here).
	var recordCounts, errorSummary pyjson.Value
	if batch.RecordCounts != nil {
		recordCounts = batch.RecordCounts
	}
	if batch.ErrorSummary != nil {
		errorSummary = batch.ErrorSummary
	}
	out := pyjson.NewObject()
	out.Set("ingestion_id", batch.IngestionID.String())
	out.Set("org_id", batch.OrgID)
	out.Set("status", batch.Status)
	out.Set("attempts", batch.Attempts)
	out.Set("source_system", batch.SourceSystem)
	out.Set("source_instance", batch.SourceInstance)
	out.Set("producer", optionalString(batch.Producer))
	out.Set("producer_version", optionalString(batch.ProducerVersion))
	out.Set("schema_version", batch.SchemaVersion)
	out.Set("window_started_at", optionalTime(batch.WindowStartedAt))
	out.Set("window_ended_at", optionalTime(batch.WindowEndedAt))
	out.Set("items_received", batch.ItemsReceived)
	out.Set("items_accepted", batch.ItemsAccepted)
	out.Set("items_rejected", batch.ItemsRejected)
	out.Set("record_counts", recordCounts)
	out.Set("error_summary", errorSummary)
	out.Set("created_at", pydanticTime(batch.CreatedAt))
	out.Set("updated_at", pydanticTime(batch.UpdatedAt))
	out.Set("completed_at", optionalTime(batch.CompletedAt))
	rejected := make([]pyjson.Value, len(rejections))
	for index, rejection := range rejections {
		item := pyjson.NewObject()
		item.Set("index", rejection.Index)
		item.Set("kind", rejection.Kind)
		item.Set("external_id", optionalString(rejection.ExternalID))
		item.Set("code", rejection.Code)
		item.Set("message", rejection.Message)
		item.Set("path", optionalString(rejection.Path))
		rejected[index] = item
	}
	out.Set("rejected_records", rejected)
	out.Set("rejected_records_total", total)
	out.Set("rejected_records_limit", limit)
	out.Set("rejected_records_offset", offset)
	return out, nil
}
