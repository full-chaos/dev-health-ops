package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// maxErrorTextLength is error_sanitize.DEFAULT_MAX_ERROR_TEXT_LENGTH.
const maxErrorTextLength = 4000

// taskQueueUnavailable is the 503 the backfill route answers for any failure
// past its guards that is not an HTTPException: "Task queue unavailable: " and
// the sanitized error. Named divergence: only an infrastructure failure gets
// here, and its text is Go's error, where Python's is the SQLAlchemy or driver
// exception's class name and message.
func taskQueueUnavailable(err error) error {
	return refuse(http.StatusServiceUnavailable, "Task queue unavailable: "+pythonparity.SanitizeErrorText(err.Error(), maxErrorTextLength))
}

// backfillSyncConfig is sync.py's trigger_sync_config_backfill, in its order:
// the body (422, before the guard), the config (404), the canonical incident
// gate (403), paused (409), the tier's backfill_days limit (403), the credential
// preflight, then the hand-off to the scheduler, the bounded wait for its plan,
// and the backfill_jobs history row.
func (h *handlers) backfillSyncConfig(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	window, problems := parseBackfillRequest(body)
	if len(problems) > 0 {
		writeQueryErrors(w, problems)
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
	trigger, err := h.handOffManual(ctx, org, config.ID, manualSpec{mode: "backfill", triggeredBy: "backfill", window: &window})
	if err != nil {
		h.answerBackfillFailure(w, r, "hand_off_backfill", err)
		return
	}
	outcome, answered, err := h.manualOutcome(w, r, config.ID.String(), trigger)
	if err != nil {
		h.answerBackfillFailure(w, r, "wait for the scheduler", err)
		return
	}
	if answered {
		return
	}
	jobID, err := h.recordBackfillJob(ctx, org, config.ID, window, outcome.SyncRunID)
	if err != nil {
		h.answerBackfillFailure(w, r, "backfill_job", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("status", "accepted")
	out.Set("config_id", config.ID.String())
	out.Set("task_id", "sync_run:"+outcome.SyncRunID)
	out.Set("backfill_job_id", jobID.String())
	out.Set("sync_run_id", outcome.SyncRunID)
	out.Set("mode", "fanout")
	out.Set("since", window.sinceISO())
	out.Set("before", window.beforeISO())
	policy.WriteModel(w, http.StatusAccepted, out, nil)
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

// recordBackfillJob is the route's BackfillJob row: pending, the inclusive
// calendar-date history anchor, and the run it fans out as (celery_task_id
// "sync_run:<id>"). Every NOT NULL column the model fills from Python-side
// defaults is written as the ORM writes it.
func (h *handlers) recordBackfillJob(ctx context.Context, org string, configID uuid.UUID, window backfillWindow, syncRunID string) (uuid.UUID, error) {
	id := uuid.New()
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
INSERT INTO public.backfill_jobs
	(id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks, completed_chunks, failed_chunks)
VALUES ($1, $2, $3, $4, 'pending', $5, $6, 0, 0, 0)`,
			id, org, configID, "sync_run:"+syncRunID, window.historySince(), window.historyBefore())
		return err
	})
	return id, err
}
