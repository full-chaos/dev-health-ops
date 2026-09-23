package externalingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/google/uuid"
)

// handleListSchemas is router.py's list_schemas (GET /schemas).
func (d Deps) handleListSchemas() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.schemasList, forwardedIP(r)); err != nil {
			writeIngestError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"schemaVersions": []string{schemaVersion},
			"recordKinds":    recordKinds,
			"limits":         limitsPayload(d.limits()),
		})
	}
}

// handleGetSchema is router.py's get_schema (GET /schemas/{schema_version}).
func (d Deps) handleGetSchema() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.schemasGet, forwardedIP(r)); err != nil {
			writeIngestError(w, err)
			return
		}
		version := r.PathValue("schema_version")
		if version != schemaVersion {
			writeIngestError(w, newIngestError(http.StatusNotFound, "unsupported_schema_version",
				fmt.Sprintf("Unknown schema version: %q", version)))
			return
		}
		document, err := schemaDocument(d.limits())
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to build schema document"))
			return
		}
		etag, err := computeETag(document)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to compute schema ETag"))
			return
		}
		if r.Header.Get("If-None-Match") == etag {
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "public, max-age=3600, must-revalidate")
		writeJSON(w, http.StatusOK, document)
	}
}

// handleAvailability is router.py's get_availability (GET /availability).
func (d Deps) handleAvailability() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, authErr := d.requireIngestScope(r.Context(), r, "schema:read", false)
		if authErr != nil {
			writeIngestError(w, authErr)
			return
		}
		customerPushDecision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(r.Context(), authCtx.OrgID, "customer_push_ingest")
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve feature state"))
			return
		}
		customerPush := customerPushDecision.Allowed
		canonicalIncidentDecision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(r.Context(), authCtx.OrgID, "canonical_incident_ingestion")
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve feature state"))
			return
		}
		canonicalIncident := canonicalIncidentDecision.Allowed

		available := map[string]bool{}
		unavailable := map[string]bool{}
		switch {
		case !customerPush:
			for _, kind := range recordKinds {
				unavailable[kind] = true
			}
		case !canonicalIncident:
			for _, kind := range recordKinds {
				if operationalRecordKinds[kind] {
					unavailable[kind] = true
				} else {
					available[kind] = true
				}
			}
		default:
			for _, kind := range recordKinds {
				available[kind] = true
			}
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"schemaVersion": schemaVersion,
			"features": map[string]any{
				"customerPushIngest":         customerPush,
				"canonicalIncidentIngestion": canonicalIncident,
			},
			"availableRecordKinds":   sortedSet(available),
			"unavailableRecordKinds": sortedSet(unavailable),
		})
	}
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// handleValidate is router.py's validate_batch (POST /validate).
func (d Deps) handleValidate() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, authErr := d.requireIngestScope(r.Context(), r, "schema:read", true)
		if authErr != nil {
			writeIngestError(w, authErr)
			return
		}
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.validate, ingestTokenRateLimitKey(authCtx.TokenID)); err != nil {
			writeIngestError(w, err)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "invalid_envelope", "Malformed batch envelope"))
			return
		}
		envelope, err := parseEnvelope(raw)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "invalid_envelope", err.Error()))
			return
		}
		if envelope.SchemaVersion != schemaVersion {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "unsupported_schema_version",
				fmt.Sprintf("Unsupported schemaVersion: %q", envelope.SchemaVersion)))
			return
		}
		if len(envelope.Records) > d.limits().MaxRecords {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "batch_too_large",
				fmt.Sprintf("Batch has %d records; max is %d", len(envelope.Records), d.limits().MaxRecords)))
			return
		}
		if requiresCanonicalIncidentIngestion(envelope.Records) {
			decision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(r.Context(), authCtx.OrgID, "canonical_incident_ingestion")
			if err != nil {
				writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve feature state"))
				return
			}
			if !decision.Allowed {
				writeIngestError(w, newIngestError(http.StatusForbidden, "feature_not_enabled",
					"Canonical incident ingestion is not enabled for this organization"))
				return
			}
		}
		errs := validateRecords(envelope.Records)
		rejectedIndices := map[int]bool{}
		for _, e := range errs {
			rejectedIndices[e.Index] = true
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"valid":         len(errs) == 0,
			"itemsAccepted": len(envelope.Records) - len(rejectedIndices),
			"itemsRejected": len(rejectedIndices),
			"errors":        nonNilErrors(errs),
		})
	}
}

func nonNilErrors(errs []ValidationErrorItem) []ValidationErrorItem {
	if errs == nil {
		return []ValidationErrorItem{}
	}
	return errs
}

func requiresCanonicalIncidentIngestion(records []Record) bool {
	for _, r := range records {
		if operationalRecordKinds[r.Kind] {
			return true
		}
	}
	return false
}

// handleAcceptBatch is router.py's accept_batch (POST /batches): the full
// CC22 accept sequence.
func (d Deps) handleAcceptBatch() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, authErr := d.requireIngestScope(r.Context(), r, "ingest:write", true)
		if authErr != nil {
			writeIngestError(w, authErr)
			return
		}
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.batches, ingestTokenRateLimitKey(authCtx.TokenID)); err != nil {
			writeIngestError(w, err)
			return
		}
		if d.Pool == nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "external-ingest database is not configured"))
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "invalid_envelope", "Malformed batch envelope"))
			return
		}
		envelope, err := parseEnvelope(raw)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "invalid_envelope", err.Error()))
			return
		}
		if idempotencyHeader := r.Header.Get("Idempotency-Key"); idempotencyHeader != "" && idempotencyHeader != envelope.IdempotencyKey {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "idempotency_key_mismatch",
				"Idempotency-Key header does not match body idempotencyKey"))
			return
		}
		if envelope.SchemaVersion != schemaVersion {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "unsupported_schema_version",
				fmt.Sprintf("Unsupported schemaVersion: %q", envelope.SchemaVersion)))
			return
		}
		kinds := make([]string, len(envelope.Records))
		for i, rec := range envelope.Records {
			kinds[i] = rec.Kind
			if _, known := recordKindValidators[rec.Kind]; !known {
				writeIngestError(w, newIngestError(http.StatusBadRequest, "unknown_record_kind",
					fmt.Sprintf("Unknown record kind at index %d: %q", i, rec.Kind)))
				return
			}
		}
		if len(envelope.Records) > d.limits().MaxRecords {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "batch_too_large",
				fmt.Sprintf("Batch has %d records; max is %d", len(envelope.Records), d.limits().MaxRecords)))
			return
		}
		if requiresCanonicalIncidentIngestion(envelope.Records) {
			decision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(r.Context(), authCtx.OrgID, "canonical_incident_ingestion")
			if err != nil {
				writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve feature state"))
				return
			}
			if !decision.Allowed {
				writeIngestError(w, newIngestError(http.StatusForbidden, "feature_not_enabled",
					"Canonical incident ingestion is not enabled for this organization"))
				return
			}
		}
		expectedFamily := entityFamilyForRecordKinds(kinds)
		if expectedFamily == "" || expectedFamily != envelope.Source.EntityFamily {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "entity_family_mismatch",
				"source.entityFamily must match the submitted record kinds"))
			return
		}
		if err := requireMatchingSource(authCtx, envelope.Source.System, envelope.Source.Instance, envelope.Source.EntityFamily); err != nil {
			writeIngestError(w, err)
			return
		}

		mode, err := resolveEffectiveMode(r.Context(), d.Pool, authCtx.OrgID, envelope.Source.System, envelope.Source.Instance, envelope.Source.EntityFamily)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve source ownership"))
			return
		}
		if mode != modeCustomerPush {
			writeIngestError(w, ownershipError(mode, envelope.Source.System, envelope.Source.Instance))
			return
		}

		payloadHash, err := computePayloadHash(envelope)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to hash payload"))
			return
		}

		outcome, batch, ingestErr := d.acceptBatchTransaction(r.Context(), authCtx.OrgID, envelope, payloadHash, raw)
		if ingestErr != nil {
			writeIngestError(w, ingestErr)
			return
		}
		if outcome.Kind == outcomeConflict {
			writeIngestError(w, newIngestError(http.StatusConflict, "idempotency_conflict",
				fmt.Sprintf("Idempotency key %q was already used for source '%s:%s' with a different payload. "+
					"Use a new idempotencyKey, or retry with the exact original payload to get the cached status.",
					envelope.IdempotencyKey, envelope.Source.System, envelope.Source.Instance)))
			return
		}
		if outcome.Kind == outcomeReplay {
			d.writeReplayStatus(w, r.Context(), authCtx.OrgID, batch)
			return
		}

		stream, streamErr := enqueueBatch(r.Context(), d.Valkey, enqueueParams{
			OrgID: authCtx.OrgID, IngestionID: batch.IngestionID.String(),
			SourceSystem: envelope.Source.System, SourceInstance: envelope.Source.Instance,
			SchemaVersion: envelope.SchemaVersion, IdempotencyKey: envelope.IdempotencyKey,
			RecordCount: len(envelope.Records), WindowStartedAt: batch.WindowStartedAt, WindowEndedAt: batch.WindowEndedAt,
		})
		if streamErr != nil {
			if markErr := markStreamUnavailableTx(r.Context(), d.Pool, authCtx.OrgID, batch.IngestionID); markErr != nil {
				// The row is stuck at status='accepted' with a fresh
				// updated_at: a same-key retry inside the stale window
				// would misclassify as REPLAY (idempotency.go's
				// classifyExisting) and return a false 200, never
				// re-enqueueing -- the exact silent-data-loss shape this
				// package exists to prevent. We cannot make good on the
				// "retry once available" promise the 503 body below makes,
				// so this is reported as an internal error, loudly, rather
				// than the routine-retry framing.
				d.logger().LogAttrs(r.Context(), slog.LevelError, "external-ingest: failed to mark batch stream_unavailable after an enqueue failure",
					slog.String("org_id", authCtx.OrgID), slog.String("ingestion_id", batch.IngestionID.String()),
					slog.String("enqueue_error", streamErr.Error()), slog.String("mark_unavailable_error", markErr.Error()))
				writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error",
					"The batch could not be durably recorded as unavailable for retry. Contact support with this ingestionId: "+batch.IngestionID.String()))
				return
			}
			writeIngestError(w, newIngestError(http.StatusServiceUnavailable, "stream_unavailable",
				fmt.Sprintf("The durable ingest stream is temporarily unavailable. The batch was recorded as %q; "+
					"retry with the same idempotencyKey once available.", batch.IngestionID)))
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]any{
			"ingestionId":   batch.IngestionID.String(),
			"status":        "accepted",
			"itemsReceived": len(envelope.Records),
			"stream":        stream,
		})
	}
}

// acceptBatchTransaction runs the idempotency resolution, RETRY reset, and
// payload upsert inside one transaction -- the RETURN of accept_batch's
// "resolve_batch_idempotency -> [reset_for_retry] -> upsert_payload ->
// COMMIT" sequence, before the enqueue (which must run AFTER commit,
// per the fail-closed payload-durability precondition).
func (d Deps) acceptBatchTransaction(
	ctx context.Context, orgID string, envelope *BatchEnvelope, payloadHash string, raw []byte,
) (idempotencyOutcome, *BatchRow, *ingestError) {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to begin accept transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	outcome, err := resolveBatchIdempotency(ctx, tx, createBatchParams{
		OrgID: orgID, IdempotencyKey: envelope.IdempotencyKey, PayloadHash: payloadHash,
		SourceSystem: envelope.Source.System, SourceInstance: envelope.Source.Instance,
		EntityFamily: envelope.Source.EntityFamily, Producer: envelope.Source.Producer,
		ProducerVersion: envelope.Source.ProducerVersion, SchemaVersion: envelope.SchemaVersion,
		WindowStartedAt: windowStart(envelope.Window), WindowEndedAt: windowEnd(envelope.Window),
		ItemsReceived: len(envelope.Records),
	})
	if errors.Is(err, errIngestTemporarilyUnavailable) {
		return idempotencyOutcome{}, nil, newIngestError(http.StatusServiceUnavailable, "ingest_temporarily_unavailable",
			"A concurrent request for the same idempotency key is in progress. Retry.")
	}
	if err != nil {
		return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve idempotency")
	}
	if outcome.Kind == outcomeConflict {
		if err := tx.Commit(ctx); err != nil {
			return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to commit")
		}
		return outcome, &outcome.Batch, nil
	}

	batch := outcome.Batch
	if outcome.Kind == outcomeRetry {
		won, err := resetForRetryTx(ctx, tx, orgID, batch.IngestionID, batch.Status)
		if err != nil {
			return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to reset for retry")
		}
		if !won {
			if err := tx.Commit(ctx); err != nil {
				return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to commit")
			}
			return idempotencyOutcome{Kind: outcomeReplay, Batch: batch}, &batch, nil
		}
	}
	if outcome.Kind == outcomeReplay {
		if err := tx.Commit(ctx); err != nil {
			return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to commit")
		}
		return outcome, &batch, nil
	}

	if err := upsertPayloadTx(ctx, tx, batch.IngestionID, orgID, envelope.SchemaVersion, raw); err != nil {
		return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to persist payload")
	}
	if err := tx.Commit(ctx); err != nil {
		return idempotencyOutcome{}, nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to commit accept transaction")
	}
	return idempotencyOutcome{Kind: outcomeNew, Batch: batch}, &batch, nil
}

func windowStart(w *IngestWindow) *time.Time {
	if w == nil {
		return nil
	}
	t := w.StartedAt
	return &t
}

func windowEnd(w *IngestWindow) *time.Time {
	if w == nil {
		return nil
	}
	t := w.EndedAt
	return &t
}

// writeReplayStatus is router.py's _replay_status_response: a REPLAY
// resolves to 200 with the full current-status envelope, not the narrow
// 202 shape.
func (d Deps) writeReplayStatus(w http.ResponseWriter, ctx context.Context, orgID string, batch *BatchRow) {
	rejections, total, err := listRejections(ctx, d.Pool, orgID, batch.IngestionID, 50, 0)
	if err != nil {
		writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to load rejections"))
		return
	}
	writeJSON(w, http.StatusOK, batchStatusResponse(batch, rejections, total, 50, 0))
}

// handleListBatches is status.py's list_batch_statuses (GET /batches).
func (d Deps) handleListBatches() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, authErr := d.requireIngestScope(r.Context(), r, "ingest:status", true)
		if authErr != nil {
			writeIngestError(w, authErr)
			return
		}
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.listBatches, ingestTokenRateLimitKey(authCtx.TokenID)); err != nil {
			writeIngestError(w, err)
			return
		}
		limit, offset := pageParams(r, "limit", "offset")
		createdAfter, createdBefore := timeRangeParams(r)
		batches, total, err := listBatches(r.Context(), d.Pool, authCtx.OrgID,
			r.URL.Query().Get("status"), r.URL.Query().Get("sourceSystem"), r.URL.Query().Get("sourceInstance"),
			createdAfter, createdBefore, limit, offset)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to list batches"))
			return
		}
		items := make([]map[string]any, len(batches))
		for i := range batches {
			items[i] = batchListItem(&batches[i])
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"items": items, "total": total, "limit": limit, "offset": offset,
		})
	}
}

// handleGetBatch is status.py's get_batch_status (GET /batches/{id}).
func (d Deps) handleGetBatch() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, authErr := d.requireIngestScope(r.Context(), r, "ingest:status", true)
		if authErr != nil {
			writeIngestError(w, authErr)
			return
		}
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.getBatch, ingestTokenRateLimitKey(authCtx.TokenID)); err != nil {
			writeIngestError(w, err)
			return
		}
		ingestionID, err := uuid.Parse(r.PathValue("ingestion_id"))
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusNotFound, "not_found", "ingestion batch not found"))
			return
		}
		batch, err := getBatch(r.Context(), d.Pool, authCtx.OrgID, ingestionID)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to load batch"))
			return
		}
		if batch == nil {
			writeIngestError(w, newIngestError(http.StatusNotFound, "not_found", "ingestion batch not found"))
			return
		}
		limit, offset := pageParams(r, "errorLimit", "errorOffset")
		rejections, total, err := listRejections(r.Context(), d.Pool, authCtx.OrgID, ingestionID, limit, offset)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to load rejections"))
			return
		}
		writeJSON(w, http.StatusOK, batchStatusResponse(batch, rejections, total, limit, offset))
	}
}

// pageParams reads limitParam/offsetParam, clamped to Python's Query(ge=1,
// le=200) / Query(ge=0) bounds -- status.py's list_batch_statuses uses
// "limit"/"offset", get_batch_status uses "errorLimit"/"errorOffset"
// (status.py:1123-1124), never the same pair.
func pageParams(r *http.Request, limitParam, offsetParam string) (limit, offset int) {
	limit, offset = 50, 0
	if v, err := strconv.Atoi(r.URL.Query().Get(limitParam)); err == nil {
		limit = v
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}
	if v, err := strconv.Atoi(r.URL.Query().Get(offsetParam)); err == nil && v >= 0 {
		offset = v
	}
	return limit, offset
}

// timeRangeParams reads createdAfter/createdBefore as RFC3339 timestamps
// (status.py's list_batch_statuses, Query(alias="createdAfter"/"createdBefore")).
// An absent or unparseable value is nil, matching FastAPI's optional
// datetime query params -- this is a narrowing from FastAPI's exact 422 on
// a malformed value to "silently not filtered"; see deps.go's package doc
// for this package's convention of stating such narrowings explicitly.
func timeRangeParams(r *http.Request) (after, before *time.Time) {
	if raw := r.URL.Query().Get("createdAfter"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			after = &t
		}
	}
	if raw := r.URL.Query().Get("createdBefore"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			before = &t
		}
	}
	return after, before
}

func batchListItem(row *BatchRow) map[string]any {
	return map[string]any{
		"ingestionId":   row.IngestionID.String(),
		"status":        row.Status,
		"itemsReceived": row.ItemsReceived,
		"itemsAccepted": row.ItemsAccepted,
		"itemsRejected": row.ItemsRejected,
		"source":        map[string]any{"system": row.SourceSystem, "instance": row.SourceInstance},
		"window":        map[string]any{"startedAt": formatOptionalRFC3339(row.WindowStartedAt), "endedAt": formatOptionalRFC3339(row.WindowEndedAt)},
		"producer":      optionalStringValue(row.Producer),
		"createdAt":     row.CreatedAt.UTC().Format(time.RFC3339Nano),
		"completedAt":   formatOptionalRFC3339(row.CompletedAt),
	}
}

func batchStatusResponse(row *BatchRow, rejections []RejectionRow, total, limit, offset int) map[string]any {
	errs := make([]map[string]any, len(rejections))
	for i, rej := range rejections {
		errs[i] = map[string]any{
			"index": rej.Index, "kind": rej.Kind, "externalId": optionalStringValue(rej.ExternalID),
			"code": rej.Code, "message": rej.Message, "path": optionalStringValue(rej.Path),
		}
	}
	return map[string]any{
		"ingestionId":     row.IngestionID.String(),
		"status":          row.Status,
		"attempts":        row.Attempts,
		"itemsReceived":   row.ItemsReceived,
		"itemsAccepted":   row.ItemsAccepted,
		"itemsRejected":   row.ItemsRejected,
		"source":          map[string]any{"system": row.SourceSystem, "instance": row.SourceInstance},
		"window":          map[string]any{"startedAt": formatOptionalRFC3339(row.WindowStartedAt), "endedAt": formatOptionalRFC3339(row.WindowEndedAt)},
		"producer":        optionalStringValue(row.Producer),
		"producerVersion": optionalStringValue(row.ProducerVersion),
		"createdAt":       row.CreatedAt.UTC().Format(time.RFC3339Nano),
		"updatedAt":       row.UpdatedAt.UTC().Format(time.RFC3339Nano),
		"completedAt":     formatOptionalRFC3339(row.CompletedAt),
		"errorSummary":    row.ErrorSummary,
		"errors":          errs,
		"errorsTotal":     total,
		"errorsLimit":     limit,
		"errorsOffset":    offset,
		"recompute": map[string]any{
			"status":       row.RecomputeStatus,
			"dispatchedAt": formatOptionalRFC3339(row.RecomputeDispatchedAt),
			"completedAt":  formatOptionalRFC3339(row.RecomputeCompletedAt),
			"error":        optionalStringValue(row.RecomputeError),
			"jobs":         []any{},
		},
	}
}

func formatOptionalRFC3339(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func optionalStringValue(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
