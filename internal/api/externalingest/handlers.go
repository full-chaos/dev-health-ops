package externalingest

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/google/uuid"
)

// handleListSchemas is router.py's list_schemas (GET /schemas).
func (d Deps) handleListSchemas() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.schemasList, forwardedIP(r)); err != nil {
			writeIngestError(w, err)
			return
		}
		body := pyjson.NewObject()
		body.Set("schemaVersions", []string{schemaVersion})
		body.Set("recordKinds", recordvalidation.RecordKinds())
		body.Set("limits", limitsPayload(d.limits()))
		policy.WriteJSON(w, http.StatusOK, body, nil)
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
				"Unknown schema version: "+pythonparity.StrRepr(version)))
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
		policy.WriteJSON(w, http.StatusOK, document, http.Header{
			"Etag":          {etag},
			"Cache-Control": {"public, max-age=3600, must-revalidate"},
		})
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
			for _, kind := range recordvalidation.RecordKinds() {
				unavailable[kind] = true
			}
		case !canonicalIncident:
			for _, kind := range recordvalidation.RecordKinds() {
				if recordvalidation.OperationalKind(kind) {
					unavailable[kind] = true
				} else {
					available[kind] = true
				}
			}
		default:
			for _, kind := range recordvalidation.RecordKinds() {
				available[kind] = true
			}
		}

		features := pyjson.NewObject()
		features.Set("customerPushIngest", customerPush)
		features.Set("canonicalIncidentIngestion", canonicalIncident)
		body := pyjson.NewObject()
		body.Set("schemaVersion", schemaVersion)
		body.Set("features", features)
		body.Set("availableRecordKinds", sortedSet(available))
		body.Set("unavailableRecordKinds", sortedSet(unavailable))
		policy.WriteJSON(w, http.StatusOK, body, nil)
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
			writeIngestError(w, err.(*ingestError))
			return
		}
		if envelope.SchemaVersion != schemaVersion {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "unsupported_schema_version",
				"Unsupported schemaVersion: "+pythonparity.StrRepr(envelope.SchemaVersion)))
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
		errItems := nonNilErrors(errs)
		errorValues := make([]pyjson.Value, len(errItems))
		for i, item := range errItems {
			errorValues[i] = item.ToPyJSON()
		}
		body := pyjson.NewObject()
		body.Set("valid", len(errs) == 0)
		body.Set("itemsAccepted", len(envelope.Records)-len(rejectedIndices))
		body.Set("itemsRejected", len(rejectedIndices))
		body.Set("errors", errorValues)
		policy.WriteJSON(w, http.StatusOK, body, nil)
	}
}

func nonNilErrors(errs []recordvalidation.ValidationErrorItem) []recordvalidation.ValidationErrorItem {
	if errs == nil {
		return []recordvalidation.ValidationErrorItem{}
	}
	return errs
}

func requiresCanonicalIncidentIngestion(records []Record) bool {
	for _, r := range records {
		if recordvalidation.OperationalKind(r.Kind) {
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
			writeIngestError(w, err.(*ingestError))
			return
		}
		if idempotencyHeader := r.Header.Get("Idempotency-Key"); idempotencyHeader != "" && idempotencyHeader != envelope.IdempotencyKey {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "idempotency_key_mismatch",
				"Idempotency-Key header does not match body idempotencyKey"))
			return
		}
		if envelope.SchemaVersion != schemaVersion {
			writeIngestError(w, newIngestError(http.StatusBadRequest, "unsupported_schema_version",
				"Unsupported schemaVersion: "+pythonparity.StrRepr(envelope.SchemaVersion)))
			return
		}
		kinds := make([]string, len(envelope.Records))
		for i, rec := range envelope.Records {
			kinds[i] = rec.Kind
			if !recordvalidation.KnownKind(rec.Kind) {
				writeIngestError(w, newIngestError(http.StatusBadRequest, "unknown_record_kind",
					fmt.Sprintf("Unknown record kind at index %d: ", i)+pythonparity.StrRepr(rec.Kind)))
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
				"Idempotency key "+pythonparity.StrRepr(envelope.IdempotencyKey)+
					fmt.Sprintf(" was already used for source '%s:%s' with a different payload. "+
						"Use a new idempotencyKey, or retry with the exact original payload to get the cached status.",
						envelope.Source.System, envelope.Source.Instance)))
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
				"The durable ingest stream is temporarily unavailable. The batch was recorded as "+
					pythonparity.StrRepr(batch.IngestionID.String())+"; retry with the same idempotencyKey once available."))
			return
		}

		body := pyjson.NewObject()
		body.Set("ingestionId", batch.IngestionID.String())
		body.Set("status", "accepted")
		body.Set("itemsReceived", len(envelope.Records))
		body.Set("stream", stream)
		policy.WriteJSON(w, http.StatusAccepted, body, nil)
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
	jobs, err := listRecomputeJobs(ctx, d.Pool, batch.OrgID, batch.SourceSystem, batch.SourceInstance, batch.RecomputeDispatchedAt)
	if err != nil {
		writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to load recompute jobs"))
		return
	}
	policy.WriteJSON(w, http.StatusOK, batchStatusResponse(batch, rejections, jobs, total, 50, 0), nil)
}

func int64Pointer(value int64) *int64 { return &value }

// handleListBatches is status.py's list_batch_statuses (GET /batches).
func (d Deps) handleListBatches() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authCtx, authErr := d.requireIngestScope(r.Context(), r, "ingest:status", true)
		if authErr != nil {
			writeIngestError(w, authErr)
			return
		}
		values := r.URL.Query()
		var problems pybody.Errors
		sourceSystem, sourceInstance, statusFilter := pybody.LastQuery(values, "sourceSystem"), pybody.LastQuery(values, "sourceInstance"), pybody.LastQuery(values, "status")
		createdAfter, _ := problems.QueryDatetime("createdAfter", pybody.LastQuery(values, "createdAfter"))
		createdBefore, _ := problems.QueryDatetime("createdBefore", pybody.LastQuery(values, "createdBefore"))
		limit, _ := problems.QueryInt("limit", pybody.LastQuery(values, "limit"), 50, int64Pointer(1), int64Pointer(200))
		offset, _ := problems.QueryInt("offset", pybody.LastQuery(values, "offset"), 0, int64Pointer(0), nil)
		if len(problems) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
			return
		}
		// pydantic's int is unbounded; asyncpg refuses an offset past int64
		// when it binds the page query, Python's unhandled 500.
		if !offset.IsInt64() {
			writeIngestError(w, unhandledError())
			return
		}
		if err := rateLimitedOrTooManyRequests(d.routeLimiters.listBatches, ingestTokenRateLimitKey(authCtx.TokenID)); err != nil {
			writeIngestError(w, err)
			return
		}
		batches, total, err := ListBatches(r.Context(), d.Pool, authCtx.OrgID, BatchQuery{
			SourceSystem: sourceSystem, SourceInstance: sourceInstance, Status: statusFilter,
			CreatedAfter: pybody.Instant(createdAfter), CreatedBefore: pybody.Instant(createdBefore),
			Limit: int(limit.Int64()), Offset: int(offset.Int64()),
		})
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to list batches"))
			return
		}
		items := make([]pyjson.Value, len(batches))
		for i := range batches {
			items[i] = batchListItem(&batches[i])
		}
		body := pyjson.NewObject()
		body.Set("items", items)
		body.Set("total", total)
		body.Set("limit", limit.Int64())
		body.Set("offset", offset.Int64())
		policy.WriteJSON(w, http.StatusOK, body, nil)
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
		jobs, err := listRecomputeJobs(r.Context(), d.Pool, batch.OrgID, batch.SourceSystem, batch.SourceInstance, batch.RecomputeDispatchedAt)
		if err != nil {
			writeIngestError(w, newIngestError(http.StatusInternalServerError, "internal_error", "failed to load recompute jobs"))
			return
		}
		policy.WriteJSON(w, http.StatusOK, batchStatusResponse(batch, rejections, jobs, total, limit, offset), nil)
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

// recomputeScopePyJSON builds status.py's RecomputeScopeResponse -- a real
// Pydantic submodel, so Python serializes it in the MODEL's declared field
// order (repoIds, teamIds, windowStartedAt, windowEndedAt, cappedDays,
// cappedRepos) regardless of what order the stored recompute_scope JSONB
// happens to hold, with the same defaulting _recompute_scope_response
// applies (missing/falsy list -> [], missing bool -> false). nil in, nil
// out (recompute.scope is null until a bounded recompute has scoped one).
func recomputeScopePyJSON(scope map[string]any) pyjson.Value {
	if scope == nil {
		return nil
	}
	object := pyjson.NewObject()
	object.Set("repoIds", stringListOrEmpty(scope["repoIds"]))
	object.Set("teamIds", stringListOrEmpty(scope["teamIds"]))
	object.Set("windowStartedAt", scopeDatetimeValue(scope["windowStartedAt"]))
	object.Set("windowEndedAt", scopeDatetimeValue(scope["windowEndedAt"]))
	object.Set("cappedDays", boolOrFalse(scope["cappedDays"]))
	object.Set("cappedRepos", boolOrFalse(scope["cappedRepos"]))
	return object
}

// stringListOrEmpty ports `list(scope.get(key) or [])`: a missing or falsy
// (nil, empty list) value becomes [], never null -- RecomputeScopeResponse's
// repo_ids/team_ids fields are plain lists, not Optional.
func stringListOrEmpty(raw any) []pyjson.Value {
	items, _ := raw.([]any)
	out := make([]pyjson.Value, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// boolOrFalse ports `bool(scope.get(key, False))`.
func boolOrFalse(raw any) bool {
	b, _ := raw.(bool)
	return b
}

// scopeDatetimeValue ports `_parse_dt(scope.get(key))`: an ISO8601 string
// stored in the scope JSONB, re-serialized through pytime -- NOT
// formatOptionalRFC3339, which forces UTC and lets Go's RFC3339Nano trim
// trailing zero microseconds. The stored string can carry a non-UTC offset
// (the producer writes `scope.window_start.isoformat()`, which preserves
// whatever aware offset it had) and Pydantic's own datetime serializer
// never trims a non-zero microsecond field to fewer than six digits --
// round 2 review reproduced both divergences live (a +05:30 offset forced
// to Z, and .123400 trimmed to .1234). pytime.FromISOFormat/Pydantic is the
// same datetime.fromisoformat()/pydantic-JSON pair telemetry.go and
// customerpush/reads.go already use for this exact contract. A missing,
// non-string, or unparseable value is null, matching _parse_dt_required's
// own contract of only ever being called on a value the writer produced.
func scopeDatetimeValue(raw any) any {
	s, ok := raw.(string)
	if !ok || s == "" {
		return nil
	}
	parsed, ok := pytime.FromISOFormat(s)
	if !ok {
		return nil
	}
	return pytime.Pydantic(parsed)
}

// recomputeJobPyJSON builds status.py's RecomputeJobResponse field order:
// task, taskId, queue, repoId.
func recomputeJobPyJSON(job RecomputeJobRow) *pyjson.Object {
	object := pyjson.NewObject()
	object.Set("task", job.Task)
	object.Set("taskId", optionalStringValue(job.TaskID))
	object.Set("queue", job.Queue)
	object.Set("repoId", optionalStringValue(job.RepoID))
	return object
}

// sourceRef and windowRef build status.py's SourceRef/WindowRef shapes,
// shared by batchListItem and batchStatusResponse.
func sourceRef(system, instance string) *pyjson.Object {
	object := pyjson.NewObject()
	object.Set("system", system)
	object.Set("instance", instance)
	return object
}

func windowRef(startedAt, endedAt *time.Time) *pyjson.Object {
	object := pyjson.NewObject()
	object.Set("startedAt", formatOptionalRFC3339(startedAt))
	object.Set("endedAt", formatOptionalRFC3339(endedAt))
	return object
}

// batchListItem builds status.py's BatchListItemResponse field order:
// ingestionId, status, itemsReceived, itemsAccepted, itemsRejected, source,
// window, producer, createdAt, completedAt.
func batchListItem(row *BatchRow) *pyjson.Object {
	object := pyjson.NewObject()
	object.Set("ingestionId", row.IngestionID.String())
	object.Set("status", row.Status)
	object.Set("itemsReceived", row.ItemsReceived)
	object.Set("itemsAccepted", row.ItemsAccepted)
	object.Set("itemsRejected", row.ItemsRejected)
	object.Set("source", sourceRef(row.SourceSystem, row.SourceInstance))
	object.Set("window", windowRef(row.WindowStartedAt, row.WindowEndedAt))
	object.Set("producer", optionalStringValue(row.Producer))
	object.Set("createdAt", pytime.Pydantic(pytime.UTC(row.CreatedAt)))
	object.Set("completedAt", formatOptionalRFC3339(row.CompletedAt))
	return object
}

// batchStatusResponse builds status.py's BatchStatusResponse field order,
// through an ordered *pyjson.Object instead of a map[string]any. jobs is
// the recompute_jobs log fetched separately (listRecomputeJobs) since it
// has no FK to this batch row -- a same-dispatched_at match across the
// caller's own source_system/source_instance, exactly like Python's
// _batch_to_recompute_response(row, jobs) split.
func batchStatusResponse(row *BatchRow, rejections []RejectionRow, jobs []RecomputeJobRow, total, limit, offset int) *pyjson.Object {
	errs := make([]pyjson.Value, len(rejections))
	for i, rej := range rejections {
		errItem := pyjson.NewObject()
		errItem.Set("index", rej.Index)
		errItem.Set("kind", rej.Kind)
		errItem.Set("externalId", optionalStringValue(rej.ExternalID))
		errItem.Set("code", rej.Code)
		errItem.Set("message", rej.Message)
		errItem.Set("path", optionalStringValue(rej.Path))
		errs[i] = errItem
	}
	jobItems := make([]pyjson.Value, len(jobs))
	for i, job := range jobs {
		jobItems[i] = recomputeJobPyJSON(job)
	}
	recompute := pyjson.NewObject()
	recompute.Set("status", row.RecomputeStatus)
	recompute.Set("scope", recomputeScopePyJSON(row.RecomputeScope))
	recompute.Set("dispatchedAt", formatOptionalRFC3339(row.RecomputeDispatchedAt))
	recompute.Set("completedAt", formatOptionalRFC3339(row.RecomputeCompletedAt))
	recompute.Set("error", optionalStringValue(row.RecomputeError))
	recompute.Set("jobs", jobItems)

	object := pyjson.NewObject()
	object.Set("ingestionId", row.IngestionID.String())
	object.Set("status", row.Status)
	object.Set("attempts", row.Attempts)
	object.Set("itemsReceived", row.ItemsReceived)
	object.Set("itemsAccepted", row.ItemsAccepted)
	object.Set("itemsRejected", row.ItemsRejected)
	object.Set("source", sourceRef(row.SourceSystem, row.SourceInstance))
	object.Set("window", windowRef(row.WindowStartedAt, row.WindowEndedAt))
	object.Set("producer", optionalStringValue(row.Producer))
	object.Set("producerVersion", optionalStringValue(row.ProducerVersion))
	object.Set("createdAt", pytime.Pydantic(pytime.UTC(row.CreatedAt)))
	object.Set("updatedAt", pytime.Pydantic(pytime.UTC(row.UpdatedAt)))
	object.Set("completedAt", formatOptionalRFC3339(row.CompletedAt))
	object.Set("errorSummary", row.ErrorSummary)
	object.Set("errors", errs)
	object.Set("errorsTotal", total)
	object.Set("errorsLimit", limit)
	object.Set("errorsOffset", offset)
	object.Set("recompute", recompute)
	return object
}

// formatOptionalRFC3339 formats a real Postgres timestamptz-sourced instant
// the way Pydantic's JSON datetime serializer does: exactly six fraction
// digits when non-zero (never trimmed), "Z" for UTC. NOT
// t.UTC().Format(time.RFC3339Nano): Go's %.9f-style fractional-second
// formatting TRIMS trailing zeros, so a genuinely-random microsecond value
// like 52254860 (".254860") renders as ".25486" -- a real, intermittent
// mismatch (roughly 1 run in 10, whenever the low-order digit of a
// timestamp happens to be zero), not a hypothetical one: it flaked
// TestExternalIngestVenueOracle's required venue-oracles check on main.
// pytime.Pydantic/pytime.UTC is the same pair scopeDatetimeValue already
// uses for this exact contract.
func formatOptionalRFC3339(t *time.Time) any {
	if t == nil {
		return nil
	}
	return pytime.Pydantic(pytime.UTC(*t))
}

func optionalStringValue(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
