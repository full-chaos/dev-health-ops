package reports

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// ErrWriterUnavailable reports a write attempted without the dependency it
// needs (no Postgres pool, or no outbox publisher for triggerReport).
var ErrWriterUnavailable = errors.New("reports: saved report writer unavailable")

// TxBeginner opens the transaction one mutation runs in; *pgxpool.Pool
// satisfies it.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// OutboxPublisher stages a job envelope in the caller's transaction;
// *joboutbox.Producer satisfies it.
type OutboxPublisher interface {
	Publish(ctx context.Context, tx pgx.Tx, kind string, envelope jobcontract.Envelope) error
}

// CronEvaluator resolves the next cron occurrence strictly after base,
// evaluated as wall-clock time in the named zone and returned in UTC;
// internal/scheduler/sync.NextOccurrence is the reviewed port of
// workers.task_utils.cron_next_run.
type CronEvaluator func(expression string, base time.Time, timezoneName string) (time.Time, bool, error)

// Writer is the Go form of the saved-report mutations in
// api/graphql/resolvers/reports.py. Every mutation runs in ONE transaction, as
// the Python resolver's session does, so a validation failure after the row was
// written leaves nothing behind.
type Writer struct {
	Pool           TxBeginner
	Outbox         OutboxPublisher
	NextOccurrence CronEvaluator

	// Now and NewID are the clock and the uuid4 source; nil means the real ones.
	Now   func() time.Time
	NewID func() string
	// TraceParent is the W3C traceparent of the request's active span, "" when
	// there is none; nil means the OpenTelemetry global propagator.
	TraceParent func(ctx context.Context) string
}

// CreateInput is CreateSavedReportInput. ReportPlan and Parameters are the
// client's JSON as bytes (nil or null when absent).
type CreateInput struct {
	Name             string
	Description      *string
	ReportPlan       []byte
	IsTemplate       bool
	Parameters       []byte
	ScheduleCron     *string
	ScheduleTimezone string
}

// UpdateInput is UpdateSavedReportInput: a nil pointer or a null JSON leaves
// the column alone (Python's `is not None`).
type UpdateInput struct {
	Name             *string
	Description      *string
	ReportPlan       []byte
	IsTemplate       *bool
	Parameters       []byte
	IsActive         *bool
	ScheduleCron     *string
	ScheduleTimezone *string
}

// CloneInput is CloneSavedReportInput.
type CloneInput struct {
	SourceReportID     string
	NewName            *string
	ParameterOverrides []byte
}

func (w *Writer) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC().Truncate(time.Microsecond)
	}
	return time.Now().UTC().Truncate(time.Microsecond)
}

func (w *Writer) newID() string {
	if w.NewID != nil {
		return w.NewID()
	}
	return uuid.NewString()
}

func (w *Writer) begin(ctx context.Context, operation string) (pgx.Tx, error) {
	if w == nil || w.Pool == nil {
		logFailure(ctx, operation, "writer", ErrWriterUnavailable)
		return nil, ErrWriterUnavailable
	}
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		logFailure(ctx, operation, "begin", err)
		return nil, fmt.Errorf("%s: begin transaction: %w", operation, err)
	}
	return tx, nil
}

// finish commits, or rolls back when the mutation failed. A rollback that
// itself fails is logged, never returned: the mutation's own error is the one
// the caller needs.
func finish(ctx context.Context, tx pgx.Tx, operation string, opErr error) error {
	if opErr != nil {
		if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			logFailure(ctx, operation, "rollback", err)
		}
		return opErr
	}
	if err := tx.Commit(ctx); err != nil {
		logFailure(ctx, operation, "commit", err)
		return fmt.Errorf("%s: commit: %w", operation, err)
	}
	return nil
}

const insertSavedReportSQL = `
INSERT INTO saved_reports (id, org_id, name, description, report_plan, is_template,
                           template_source_id, parameters, is_active, created_by,
                           created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, $5::json, $6, $7::uuid, $8::json, $9, $10, $11, $12)`

// Create is resolve_create_saved_report.
func (w *Writer) Create(ctx context.Context, orgID string, in CreateInput) (*model.SavedReportType, error) {
	tx, err := w.begin(ctx, "createSavedReport")
	if err != nil {
		return nil, err
	}
	report, err := w.create(ctx, tx, orgID, in)
	if err := finish(ctx, tx, "createSavedReport", err); err != nil {
		return nil, err
	}
	return report, nil
}

func (w *Writer) create(ctx context.Context, tx pgx.Tx, orgID string, in CreateInput) (*model.SavedReportType, error) {
	plan, err := PythonDumpsObject(in.ReportPlan)
	if err != nil {
		return nil, fmt.Errorf("reportPlan: %w", err)
	}
	parameters, err := PythonDumpsObject(in.Parameters)
	if err != nil {
		return nil, fmt.Errorf("parameters: %w", err)
	}
	id := w.newID()
	created := w.now()
	if _, err := tx.Exec(ctx, insertSavedReportSQL, id, orgID, in.Name, in.Description, plan,
		in.IsTemplate, nil, parameters, true, nil, created, created); err != nil {
		logFailure(ctx, "createSavedReport", "insert", err)
		return nil, fmt.Errorf("createSavedReport: insert: %w", err)
	}
	if in.ScheduleCron != nil {
		state := reportState{ID: id, OrgID: orgID, Name: in.Name, CreatedAt: created}
		if err := w.ensureSchedule(ctx, tx, state, *in.ScheduleCron, in.ScheduleTimezone, false); err != nil {
			return nil, err
		}
	}
	report, err := (&Reader{Postgres: tx}).Get(ctx, orgID, id)
	if err != nil {
		return nil, err
	}
	if report == nil {
		return nil, fmt.Errorf("Saved report not found after create: %s", id)
	}
	return report, nil
}

// reportState is what the schedule step reads of the report it schedules.
type reportState struct {
	ID         string
	OrgID      string
	Name       string
	ScheduleID *string
	LastRunAt  *time.Time
	CreatedAt  time.Time
}

// ensureSchedule is _ensure_or_update_schedule. The report row already exists;
// a new job is linked to it with one more UPDATE. explicitUpdatedAt is true
// when the caller set updated_at itself in this transaction (an update), false
// when the row's own onupdate default stamps it (a create).
func (w *Writer) ensureSchedule(ctx context.Context, tx pgx.Tx, report reportState, cron, tz string, explicitUpdatedAt bool) error {
	if err := validateTimezone(tz); err != nil {
		return err
	}
	if w.NextOccurrence == nil {
		return ErrWriterUnavailable
	}
	if err := validateCron(cron, w.NextOccurrence, w.now()); err != nil {
		return err
	}
	base := report.CreatedAt
	if report.LastRunAt != nil {
		base = *report.LastRunAt
	}
	nextRun, _, err := w.NextOccurrence(cron, base, tz)
	if err != nil {
		return err
	}
	nextRun = nextRun.UTC()

	if report.ScheduleID != nil {
		updated, err := tx.Exec(ctx, `
UPDATE scheduled_jobs SET schedule_cron = $2, timezone = $3, next_run_at = $4, updated_at = $5
WHERE id = $1::uuid`, *report.ScheduleID, cron, tz, nextRun, w.now())
		if err != nil {
			logFailure(ctx, "saveReportSchedule", "update job", err)
			return fmt.Errorf("saved report schedule: update job: %w", err)
		}
		if updated.RowsAffected() > 0 {
			return nil
		}
		// The linked job is gone: a new one replaces it, as the Python resolver's
		// fall-through does.
	}

	jobID := w.newID()
	stamp := w.now()
	if _, err := tx.Exec(ctx, `
INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone,
                            job_config, sync_config_id, status, is_running, next_run_at,
                            run_count, failure_count, created_at, updated_at)
VALUES ($1::uuid, $2, $3, 'report', '', $4, $5, $6::json, NULL, 0, false, $7, 0, 0, $8, $8)`,
		jobID, report.OrgID, "report:"+report.Name, cron, tz,
		`{"report_id": "`+report.ID+`"}`, nextRun, stamp); err != nil {
		logFailure(ctx, "saveReportSchedule", "insert job", err)
		return fmt.Errorf("saved report schedule: insert job: %w", err)
	}
	var linkErr error
	if explicitUpdatedAt {
		_, linkErr = tx.Exec(ctx, `UPDATE saved_reports SET schedule_id = $2::uuid WHERE id = $1::uuid`, report.ID, jobID)
	} else {
		_, linkErr = tx.Exec(ctx, `UPDATE saved_reports SET schedule_id = $2::uuid, updated_at = $3 WHERE id = $1::uuid`,
			report.ID, jobID, w.now())
	}
	if err := linkErr; err != nil {
		logFailure(ctx, "saveReportSchedule", "link job", err)
		return fmt.Errorf("saved report schedule: link job: %w", err)
	}
	return nil
}

// validateTimezone is validate_timezone_name: empty is allowed (callers
// default it to UTC), anything else must build a zoneinfo.ZoneInfo.
func validateTimezone(tz string) error {
	if tz == "" || pythonparity.ZoneInfoKeyValid(tz) {
		return nil
	}
	return fmt.Errorf("Invalid timezone: %s", pythonparity.StrRepr(tz))
}

// validateCron is _validate_report_schedule_cron: exactly five
// whitespace-separated fields, and an expression the cron dialect can evaluate.
func validateCron(cron string, next CronEvaluator, now time.Time) error {
	if len(pythonparity.SplitWhitespace(cron)) != 5 {
		return fmt.Errorf("Invalid report schedule cron expression %s: expected exactly "+
			"five fields (minute hour day-of-month month day-of-week)", pythonparity.StrRepr(cron))
	}
	if _, _, err := next(cron, now, "UTC"); err != nil {
		return fmt.Errorf("Invalid report schedule cron expression %s: %v", pythonparity.StrRepr(cron), err)
	}
	return nil
}

// Update is resolve_update_saved_report: nil when the report is not the org's.
func (w *Writer) Update(ctx context.Context, orgID, reportID string, in UpdateInput) (*model.SavedReportType, error) {
	id, err := ParseReportID(reportID)
	if err != nil {
		return nil, err
	}
	tx, err := w.begin(ctx, "updateSavedReport")
	if err != nil {
		return nil, err
	}
	report, err := w.update(ctx, tx, orgID, id, in)
	if err := finish(ctx, tx, "updateSavedReport", err); err != nil {
		return nil, err
	}
	return report, nil
}

func (w *Writer) update(ctx context.Context, tx pgx.Tx, orgID, id string, in UpdateInput) (*model.SavedReportType, error) {
	var state reportState
	var name string
	err := tx.QueryRow(ctx, `
SELECT id::text, org_id, name, schedule_id::text, last_run_at, created_at
FROM saved_reports WHERE id = $1::uuid AND org_id = $2 FOR UPDATE`, id, orgID).
		Scan(&state.ID, &state.OrgID, &name, &state.ScheduleID, &state.LastRunAt, &state.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logFailure(ctx, "updateSavedReport", "read", err)
		return nil, fmt.Errorf("updateSavedReport: read: %w", err)
	}
	state.Name = name
	sets := []string{}
	args := []any{id}
	set := func(column, cast string, value any) {
		args = append(args, value)
		sets = append(sets, fmt.Sprintf("%s = $%d%s", column, len(args), cast))
	}
	if in.Name != nil {
		set("name", "", *in.Name)
		state.Name = *in.Name
	}
	if in.Description != nil {
		set("description", "", *in.Description)
	}
	if !isNullJSON(in.ReportPlan) {
		plan, err := PythonDumpsObject(in.ReportPlan)
		if err != nil {
			return nil, fmt.Errorf("reportPlan: %w", err)
		}
		set("report_plan", "::json", plan)
	}
	if in.IsTemplate != nil {
		set("is_template", "", *in.IsTemplate)
	}
	if !isNullJSON(in.Parameters) {
		parameters, err := PythonDumpsObject(in.Parameters)
		if err != nil {
			return nil, fmt.Errorf("parameters: %w", err)
		}
		set("parameters", "::json", parameters)
	}
	if in.IsActive != nil {
		set("is_active", "", *in.IsActive)
	}
	set("updated_at", "", w.now())
	if _, err := tx.Exec(ctx, "UPDATE saved_reports SET "+strings.Join(sets, ", ")+" WHERE id = $1::uuid", args...); err != nil {
		logFailure(ctx, "updateSavedReport", "update", err)
		return nil, fmt.Errorf("updateSavedReport: update: %w", err)
	}
	if in.ScheduleCron != nil {
		tz := "UTC"
		if in.ScheduleTimezone != nil && *in.ScheduleTimezone != "" {
			tz = *in.ScheduleTimezone
		}
		if err := w.ensureSchedule(ctx, tx, state, *in.ScheduleCron, tz, true); err != nil {
			return nil, err
		}
	}
	return (&Reader{Postgres: tx}).Get(ctx, orgID, id)
}

func isNullJSON(raw []byte) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed == "" || trimmed == "null"
}

// Delete is resolve_delete_saved_report: false when the report is not the
// org's. The report's runs and occurrences go with it by the foreign keys'
// ON DELETE CASCADE, the schedule's job row stays, as in Python.
func (w *Writer) Delete(ctx context.Context, orgID, reportID string) (bool, error) {
	id, err := ParseReportID(reportID)
	if err != nil {
		return false, err
	}
	tx, err := w.begin(ctx, "deleteSavedReport")
	if err != nil {
		return false, err
	}
	deleted, opErr := func() (bool, error) {
		tag, err := tx.Exec(ctx, `DELETE FROM saved_reports WHERE id = $1::uuid AND org_id = $2`, id, orgID)
		if err != nil {
			logFailure(ctx, "deleteSavedReport", "delete", err)
			return false, fmt.Errorf("deleteSavedReport: delete: %w", err)
		}
		return tag.RowsAffected() > 0, nil
	}()
	if err := finish(ctx, tx, "deleteSavedReport", opErr); err != nil {
		return false, err
	}
	return deleted, nil
}

// Clone is resolve_clone_saved_report: nil when the source is not the org's.
func (w *Writer) Clone(ctx context.Context, orgID string, in CloneInput) (*model.SavedReportType, error) {
	sourceID, err := ParseReportID(in.SourceReportID)
	if err != nil {
		return nil, err
	}
	tx, err := w.begin(ctx, "cloneSavedReport")
	if err != nil {
		return nil, err
	}
	report, err := w.clone(ctx, tx, orgID, sourceID, in)
	if err := finish(ctx, tx, "cloneSavedReport", err); err != nil {
		return nil, err
	}
	return report, nil
}

func (w *Writer) clone(ctx context.Context, tx pgx.Tx, orgID, sourceID string, in CloneInput) (*model.SavedReportType, error) {
	var name string
	var description, plan, parameters, createdBy *string
	err := tx.QueryRow(ctx, `
SELECT name, description, report_plan::text, parameters::text, created_by
FROM saved_reports WHERE id = $1::uuid AND org_id = $2`, sourceID, orgID).
		Scan(&name, &description, &plan, &parameters, &createdBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		logFailure(ctx, "cloneSavedReport", "read", err)
		return nil, fmt.Errorf("cloneSavedReport: read: %w", err)
	}
	cloneName := name + " (Copy)"
	if in.NewName != nil && *in.NewName != "" {
		cloneName = *in.NewName
	}
	planText := "null"
	if plan != nil {
		if planText, err = PythonDumps([]byte(*plan)); err != nil {
			return nil, fmt.Errorf("report_plan: %w", err)
		}
	}
	parametersText, err := CloneParameters(parameters, in.ParameterOverrides)
	if err != nil {
		return nil, err
	}
	id := w.newID()
	created := w.now()
	if _, err := tx.Exec(ctx, insertSavedReportSQL, id, orgID, cloneName, description, planText,
		false, sourceID, parametersText, true, createdBy, created, created); err != nil {
		logFailure(ctx, "cloneSavedReport", "insert", err)
		return nil, fmt.Errorf("cloneSavedReport: insert: %w", err)
	}
	return (&Reader{Postgres: tx}).Get(ctx, orgID, id)
}

// Trigger is resolve_trigger_report: nil when the report is not the org's, is
// inactive, or its id is malformed. The pending run and its outbox handoff
// commit together, or neither does.
func (w *Writer) Trigger(ctx context.Context, orgID, reportID string) (*model.ReportRunType, error) {
	id, err := ParseReportID(reportID)
	if err != nil {
		return nil, nil // Python catches the ValueError and answers null
	}
	if w == nil || w.Outbox == nil {
		logFailure(ctx, "triggerReport", "writer", ErrWriterUnavailable)
		return nil, ErrWriterUnavailable
	}
	tx, err := w.begin(ctx, "triggerReport")
	if err != nil {
		return nil, err
	}
	run, opErr := w.trigger(ctx, tx, orgID, id)
	if err := finish(ctx, tx, "triggerReport", opErr); err != nil {
		return nil, err
	}
	return run, nil
}

func (w *Writer) trigger(ctx context.Context, tx pgx.Tx, orgID, reportID string) (*model.ReportRunType, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT is_active FROM saved_reports WHERE id = $1::uuid AND org_id = $2 FOR UPDATE`,
		reportID, orgID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !active) {
		return nil, nil
	}
	if err != nil {
		logFailure(ctx, "triggerReport", "lock", err)
		return nil, fmt.Errorf("triggerReport: lock report: %w", err)
	}
	runID := w.newID()
	created := w.now()
	if _, err := tx.Exec(ctx, `
INSERT INTO report_runs (id, report_id, status, provenance_records, attempt_count,
                         execution_reclaim_count, notification_status, triggered_by, created_at)
VALUES ($1::uuid, $2::uuid, 'pending', '[]'::json, 0, 0, 'pending', 'api', $3)`,
		runID, reportID, created); err != nil {
		logFailure(ctx, "triggerReport", "insert run", err)
		return nil, fmt.Errorf("triggerReport: insert run: %w", err)
	}
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		CorrelationID:   "report-run:" + runID,
		IdempotencyKey:  "report.run:" + runID,
		TraceParent:     w.traceParent(ctx),
		Domain:          jobcontract.DomainLink{Type: "report_run", ID: runID},
		Payload:         jobcontract.OnDemandReportExecutionPayload{ReportID: reportID},
	}
	if err := w.Outbox.Publish(ctx, tx, jobcontract.KindReportExecuteOnDemand, envelope); err != nil {
		logFailure(ctx, "triggerReport", "publish", err)
		return nil, fmt.Errorf("triggerReport: stage the execution: %w", err)
	}
	return &model.ReportRunType{
		ID:          runID,
		ReportID:    reportID,
		Status:      "pending",
		TriggeredBy: "api",
		CreatedAt:   created,
	}, nil
}

func (w *Writer) traceParent(ctx context.Context) string {
	if w.TraceParent != nil {
		return w.TraceParent(ctx)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}
