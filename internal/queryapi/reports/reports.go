// Package reports reads saved reports and their runs from Postgres for the
// savedReports, savedReport and reportRuns GraphQL fields. The reads are
// scoped to one org on every statement; a report id is read the way Python's
// uuid.UUID reads it (see ParseReportID).
package reports

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// PGQuerier is the read-only Postgres surface shared with the data-health
// reads; *pgxpool.Pool satisfies it.
type PGQuerier = datahealth.PGQuerier

// ErrUnavailable reports a read attempted without a Postgres reader.
var ErrUnavailable = errors.New("reports: postgres reader unavailable")

const savedReportColumns = `s.id::text, s.org_id, s.name, s.description, s.report_plan::text,
       s.is_template, s.template_source_id::text, s.parameters::text, s.schedule_id::text,
       s.is_active, s.last_run_at, s.last_run_status, s.created_at, s.updated_at, s.created_by`

const savedReportsCountSQL = `SELECT count(*) FROM saved_reports s WHERE s.org_id = $1`

const savedReportsPageSQL = `SELECT ` + savedReportColumns + `
FROM saved_reports s
WHERE s.org_id = $1
ORDER BY s.updated_at DESC
OFFSET $2::bigint LIMIT $3::bigint`

const savedReportSQL = `SELECT ` + savedReportColumns + `
FROM saved_reports s
WHERE s.id = $1::uuid AND s.org_id = $2`

// report_runs has no org column: the org predicate rides on the report the
// runs belong to, in the same statement.
const reportRunsCountSQL = `SELECT count(*)
FROM report_runs r
JOIN saved_reports s ON s.id = r.report_id
WHERE s.org_id = $1 AND r.report_id = $2::uuid`

const reportRunsPageSQL = `SELECT r.id::text, r.report_id::text, r.status, r.started_at, r.completed_at,
       r.duration_seconds, r.rendered_markdown, r.artifact_url, r.provenance_records::text,
       r.error, r.triggered_by, r.created_at
FROM report_runs r
JOIN saved_reports s ON s.id = r.report_id
WHERE s.org_id = $1 AND r.report_id = $2::uuid
ORDER BY r.created_at DESC
LIMIT $3::bigint`

// Reader reads saved reports for one authorized org.
type Reader struct {
	Postgres PGQuerier
}

// checkInt32 mirrors the GraphQL Int range the Python schema enforces on
// limit and offset.
func checkInt32(name string, value int) error {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return fmt.Errorf("Int cannot represent non 32-bit signed integer value: %d (%s)", value, name)
	}
	return nil
}

func (r *Reader) query() (PGQuerier, error) {
	if r == nil || r.Postgres == nil {
		return nil, ErrUnavailable
	}
	return r.Postgres, nil
}

func logFailure(ctx context.Context, operation, step string, err error) {
	var pgErr *pgconn.PgError
	cause := "query"
	if errors.As(err, &pgErr) {
		cause = "sqlstate_" + pgErr.Code
	}
	slog.ErrorContext(ctx, "query-api: saved report read failed",
		"operation", operation, "step", step, "cause", cause, "error", err)
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// List reads one page of the org's saved reports, newest update first, and the
// org's total.
func (r *Reader) List(ctx context.Context, orgID string, limit, offset int) (*model.SavedReportConnection, error) {
	if err := checkInt32("limit", limit); err != nil {
		return nil, err
	}
	if err := checkInt32("offset", offset); err != nil {
		return nil, err
	}
	pg, err := r.query()
	if err != nil {
		logFailure(ctx, "savedReports", "reader", err)
		return nil, err
	}
	total, err := scanCount(ctx, pg, savedReportsCountSQL, orgID)
	if err != nil {
		logFailure(ctx, "savedReports", "count", err)
		return nil, fmt.Errorf("saved reports count: %w", err)
	}
	rows, err := pg.Query(ctx, savedReportsPageSQL, orgID, offset, limit)
	if err != nil {
		logFailure(ctx, "savedReports", "page", err)
		return nil, fmt.Errorf("saved reports page: %w", err)
	}
	defer rows.Close()
	items := []model.SavedReportType{}
	for rows.Next() {
		item, err := scanSavedReport(rows)
		if err != nil {
			logFailure(ctx, "savedReports", "scan", err)
			return nil, fmt.Errorf("saved reports scan: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		logFailure(ctx, "savedReports", "rows", err)
		return nil, fmt.Errorf("saved reports rows: %w", err)
	}
	return &model.SavedReportConnection{Items: items, Total: total}, nil
}

// Get reads one saved report of the org; a report of another org or an
// unknown id is nil.
func (r *Reader) Get(ctx context.Context, orgID, reportID string) (*model.SavedReportType, error) {
	id, err := ParseReportID(reportID)
	if err != nil {
		return nil, err
	}
	pg, err := r.query()
	if err != nil {
		logFailure(ctx, "savedReport", "reader", err)
		return nil, err
	}
	rows, err := pg.Query(ctx, savedReportSQL, id, orgID)
	if err != nil {
		logFailure(ctx, "savedReport", "read", err)
		return nil, fmt.Errorf("saved report read: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			logFailure(ctx, "savedReport", "rows", err)
			return nil, fmt.Errorf("saved report rows: %w", err)
		}
		return nil, nil
	}
	item, err := scanSavedReport(rows)
	if err != nil {
		logFailure(ctx, "savedReport", "scan", err)
		return nil, fmt.Errorf("saved report scan: %w", err)
	}
	return &item, nil
}

// Runs reads one page of a report's runs, newest first, and the report's run
// total. A report of another org or an unknown id has no runs.
func (r *Reader) Runs(ctx context.Context, orgID, reportID string, limit int) (*model.ReportRunConnection, error) {
	id, err := ParseReportID(reportID)
	if err != nil {
		return nil, err
	}
	if err := checkInt32("limit", limit); err != nil {
		return nil, err
	}
	pg, err := r.query()
	if err != nil {
		logFailure(ctx, "reportRuns", "reader", err)
		return nil, err
	}
	total, err := scanCount(ctx, pg, reportRunsCountSQL, orgID, id)
	if err != nil {
		logFailure(ctx, "reportRuns", "count", err)
		return nil, fmt.Errorf("report runs count: %w", err)
	}
	rows, err := pg.Query(ctx, reportRunsPageSQL, orgID, id, limit)
	if err != nil {
		logFailure(ctx, "reportRuns", "page", err)
		return nil, fmt.Errorf("report runs page: %w", err)
	}
	defer rows.Close()
	items := []model.ReportRunType{}
	for rows.Next() {
		item, err := scanReportRun(rows)
		if err != nil {
			logFailure(ctx, "reportRuns", "scan", err)
			return nil, fmt.Errorf("report runs scan: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		logFailure(ctx, "reportRuns", "rows", err)
		return nil, fmt.Errorf("report runs rows: %w", err)
	}
	return &model.ReportRunConnection{Items: items, Total: total}, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanCount(ctx context.Context, pg PGQuerier, sql string, args ...any) (int, error) {
	rows, err := pg.Query(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total int64
	if rows.Next() {
		if err := rows.Scan(&total); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return int(total), nil
}

func scanSavedReport(row scanner) (model.SavedReportType, error) {
	var (
		id, orgID, name              string
		description, lastRunStatus   *string
		createdBy                    *string
		plan, params                 *string
		templateSourceID, scheduleID *string
		isTemplate, isActive         bool
		lastRunAt                    *time.Time
		createdAt, updatedAt         time.Time
	)
	if err := row.Scan(&id, &orgID, &name, &description, &plan,
		&isTemplate, &templateSourceID, &params, &scheduleID,
		&isActive, &lastRunAt, &lastRunStatus, &createdAt, &updatedAt, &createdBy); err != nil {
		return model.SavedReportType{}, err
	}
	reportPlan, err := jsonValue(plan)
	if err != nil {
		return model.SavedReportType{}, fmt.Errorf("report_plan: %w", err)
	}
	if reportPlan.IsNull() {
		// reportPlan is a non-null field; Python cannot answer a null one.
		return model.SavedReportType{}, errors.New("Cannot return null for non-nullable field SavedReportType.reportPlan")
	}
	parameters, err := jsonValue(params)
	if err != nil {
		return model.SavedReportType{}, fmt.Errorf("parameters: %w", err)
	}
	return model.SavedReportType{
		ID:               id,
		OrgID:            orgID,
		Name:             name,
		Description:      description,
		ReportPlan:       reportPlan,
		IsTemplate:       isTemplate,
		TemplateSourceID: templateSourceID,
		Parameters:       parameters,
		ScheduleID:       scheduleID,
		IsActive:         isActive,
		LastRunAt:        utc(lastRunAt),
		LastRunStatus:    lastRunStatus,
		CreatedAt:        createdAt.UTC(),
		UpdatedAt:        updatedAt.UTC(),
		CreatedBy:        createdBy,
	}, nil
}

func scanReportRun(row scanner) (model.ReportRunType, error) {
	var (
		id, reportID, status, triggeredBy string
		startedAt, completedAt            *time.Time
		durationSeconds                   *float64
		renderedMarkdown, artifactURL     *string
		provenance, runError              *string
		createdAt                         time.Time
	)
	if err := row.Scan(&id, &reportID, &status, &startedAt, &completedAt,
		&durationSeconds, &renderedMarkdown, &artifactURL, &provenance,
		&runError, &triggeredBy, &createdAt); err != nil {
		return model.ReportRunType{}, err
	}
	records, err := jsonValue(provenance)
	if err != nil {
		return model.ReportRunType{}, fmt.Errorf("provenance_records: %w", err)
	}
	return model.ReportRunType{
		ID:                id,
		ReportID:          reportID,
		Status:            status,
		StartedAt:         utc(startedAt),
		CompletedAt:       utc(completedAt),
		DurationSeconds:   durationSeconds,
		RenderedMarkdown:  renderedMarkdown,
		ArtifactURL:       artifactURL,
		ProvenanceRecords: records,
		Error:             runError,
		TriggeredBy:       triggeredBy,
		CreatedAt:         createdAt.UTC(),
	}, nil
}
