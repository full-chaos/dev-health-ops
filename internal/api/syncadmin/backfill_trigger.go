package syncadmin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// maxErrorTextLength is error_sanitize.DEFAULT_MAX_ERROR_TEXT_LENGTH.
const maxErrorTextLength = 4000

// taskQueueUnavailable is the 503 the backfill route answers for any failure
// past its guards that is not an HTTPException: "Task queue unavailable: " and
// the sanitized error. Named divergence: only an infrastructure failure gets
// here, and its text is Go's error, where Python's is the SQLAlchemy or driver
// exception's class name and message.
func taskQueueUnavailable(err error) error {
	return refuse(http.StatusServiceUnavailable, "Task queue unavailable: "+pythonparity.SanitizeErrorTextHardened(err.Error(), maxErrorTextLength))
}

// backfillSyncConfig is sync.py's trigger_sync_config_backfill, in its order:
// the body (422, before the guard), the config (404), the canonical incident
// gate (403), paused (409), the tier's backfill_days limit (403), the credential
// preflight, then the hand-off to the scheduler and the backfill_jobs history
// row in one transaction. It does not wait for the plan: 202 at once.
func (h *handlers) backfillSyncConfig(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	window, problems := parseBackfillRequest(body)
	if len(problems) > 0 {
		writeQueryErrors(w, problems)
		return
	}
	key, keyOK := parseIdempotencyKey(r.Header.Get("Idempotency-Key"))
	if !keyOK {
		policy.WriteDetail(w, http.StatusUnprocessableEntity, "Invalid Idempotency-Key header", nil)
		return
	}
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	ctx, org := r.Context(), orgID(r)
	targets, err := storedTargets(config)
	if err != nil {
		h.fail(w, r, "sync_targets", err)
		return
	}
	if err := h.requireCanonicalIncident(ctx, org, targets); err != nil {
		h.answerOrFail(w, r, "canonical_incident_feature", err)
		return
	}
	if !config.IsActive {
		policy.WriteDetail(w, http.StatusConflict, "Sync configuration is paused and cannot be backfilled", nil)
		return
	}
	if err := h.checkBackfillLimit(ctx, org, window.days()); err != nil {
		h.answerOrFail(w, r, "backfill_limit", err)
		return
	}
	if err := h.preflightPlannerCredential(ctx, org, config, targets); err != nil {
		h.answerOrFail(w, r, "preflight_credential", err)
		return
	}
	submission, err := h.submitBackfill(ctx, org, config.ID, window, key)
	if err != nil {
		h.answerBackfillFailure(w, r, "hand_off_backfill", err)
		return
	}
	policy.WriteModel(w, http.StatusAccepted, acceptedBackfillBody(config.ID, submission.JobID, submission.OccurrenceID, window), nil)
}

// answerBackfillFailure is the route's `except Exception` around the hand-off:
// an HTTPException the body raised passes through, any other failure is 503.
func (h *handlers) answerBackfillFailure(w http.ResponseWriter, r *http.Request, step string, err error) {
	var raised *answer
	if errors.As(err, &raised) {
		policy.WriteDetail(w, raised.status, raised.detail, nil)
		return
	}
	h.logger.ErrorContext(r.Context(), "sync admin: backfill failed past its guards", "step", step, "error", err.Error())
	h.answerOrFail(w, r, step, taskQueueUnavailable(err))
}

// checkBackfillLimit is TierLimitService.check_backfill_limit for the window's
// day count: 403 with the limit's text, or "Backfill not allowed".
func (h *handlers) checkBackfillLimit(ctx context.Context, org string, requestedDays int64) error {
	orgUUID, err := pythonparity.ParseUUID(org)
	if err != nil {
		return fmt.Errorf("org id is not a UUID: %w", err)
	}
	inputs, err := licensing.LoadTierLimitInputs(ctx, h.pool, orgUUID)
	if err != nil {
		return err
	}
	allowed, reason, err := licensing.CheckBackfillLimitFrom(inputs, big.NewInt(requestedDays))
	if err != nil {
		return err
	}
	if !allowed {
		if reason == "" {
			reason = "Backfill not allowed"
		}
		return refuse(http.StatusForbidden, reason)
	}
	return nil
}

// backfillSubmission is what one accepted submit names: the occurrence the
// scheduler plans and the history row the UI reads progress from.
type backfillSubmission struct {
	OccurrenceID string
	JobID        uuid.UUID
}

// backfillMaxIdempotencyKey bounds the Idempotency-Key header.
const backfillMaxIdempotencyKey = 128

// parseIdempotencyKey is the optional Idempotency-Key header: empty, or 1 to
// 128 characters of letters, digits and ".-_:".
func parseIdempotencyKey(raw string) (string, bool) {
	if len(raw) > backfillMaxIdempotencyKey {
		return "", false
	}
	for _, character := range raw {
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z', character >= '0' && character <= '9',
			character == '.', character == '-', character == '_', character == ':':
		default:
			return "", false
		}
	}
	return raw, true
}

// idempotencyHash scopes a caller's key to its org and configuration: 32 hex
// characters, so a key never matches another org's or configuration's job.
func idempotencyHash(org string, configID uuid.UUID, key string) string {
	sum := sha256.Sum256([]byte(org + "\x00" + configID.String() + "\x00" + key))
	return hex.EncodeToString(sum[:16])
}

// backfillTaskMarker is the job's celery_task_id until the scheduler plans the
// occurrence: the read path resolves "occurrence:" to the run (backfillRunCounts).
// A keyed submit also carries its key hash, which is how a repeat finds the job.
func backfillTaskMarker(occurrenceID, keyHash string) string {
	if keyHash == "" {
		return backfillOccurrenceMarker + occurrenceID
	}
	return "idem:" + keyHash + ";" + backfillOccurrenceMarker + occurrenceID
}

// backfillOccurrenceID is the text after the "occurrence:" marker in
// celery_task_id up to ";", "" when none.
func backfillOccurrenceID(job *backfillJob) string {
	if job.CeleryTaskID == nil {
		return ""
	}
	index := strings.LastIndex(*job.CeleryTaskID, backfillOccurrenceMarker)
	if index < 0 {
		return ""
	}
	rest := (*job.CeleryTaskID)[index+len(backfillOccurrenceMarker):]
	if end := strings.IndexByte(rest, ';'); end >= 0 {
		rest = rest[:end]
	}
	return rest
}

const backfillOccurrenceMarker = "occurrence:"

// checkBackfillScale is the shared scale check answered as this route's 422.
func checkBackfillScale(days, sources, unitCap int64) error {
	if err := synchandoff.CheckBackfillScale(days, sources, unitCap); err != nil {
		return refuse(http.StatusUnprocessableEntity, err.Error())
	}
	return nil
}

// acceptedBackfillBody is the 202 body: the job and the occurrence, and no run
// (the submit does not wait for the plan).
func acceptedBackfillBody(configID, jobID uuid.UUID, occurrenceID string, window backfillWindow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("status", "accepted")
	out.Set("config_id", configID.String())
	out.Set("task_id", backfillOccurrenceMarker+occurrenceID)
	out.Set("backfill_job_id", jobID.String())
	out.Set("occurrence_id", occurrenceID)
	out.Set("mode", "fanout")
	out.Set("since", window.sinceISO())
	out.Set("before", window.beforeISO())
	return out
}

// submitBackfill writes the occurrence, its manual trigger and the
// backfill_jobs row in ONE transaction and waits for nothing: every accepted
// submit has a job row. A keyed repeat of a submit that already committed
// returns that job and writes nothing; the key's advisory lock serialises
// concurrent repeats.
func (h *handlers) submitBackfill(ctx context.Context, org string, configID uuid.UUID, window backfillWindow, key string) (backfillSubmission, error) {
	var out backfillSubmission
	keyHash := ""
	if key != "" {
		keyHash = idempotencyHash(org, configID, key)
	}
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		if keyHash != "" {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "backfill-idempotency:"+keyHash); err != nil {
				return fmt.Errorf("idempotency lock: %w", err)
			}
			prefix := "idem:" + keyHash + ";" + backfillOccurrenceMarker
			var existing uuid.UUID
			var task string
			err := tx.QueryRow(ctx, `
SELECT id, celery_task_id FROM public.backfill_jobs
WHERE org_id = $1 AND sync_config_id = $2 AND celery_task_id LIKE $3
ORDER BY created_at, id LIMIT 1`, org, configID, prefix+"%").Scan(&existing, &task)
			if err == nil {
				out = backfillSubmission{OccurrenceID: strings.TrimPrefix(task, prefix), JobID: existing}
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("idempotency lookup: %w", err)
			}
		}
		trigger, err := h.handOffManualTx(ctx, tx, org, configID, manualSpec{mode: "backfill", triggeredBy: "backfill", window: &window, checkScale: true})
		if err != nil {
			return err
		}
		id := uuid.New()
		if _, err := tx.Exec(ctx, `
INSERT INTO public.backfill_jobs
	(id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks, completed_chunks, failed_chunks)
VALUES ($1, $2, $3, $4, 'pending', $5, $6, 0, 0, 0)`,
			id, org, configID, backfillTaskMarker(trigger.OccurrenceID, keyHash), window.historySince(), window.historyBefore()); err != nil {
			return fmt.Errorf("backfill job: %w", err)
		}
		out = backfillSubmission{OccurrenceID: trigger.OccurrenceID, JobID: id}
		return nil
	})
	return out, err
}
