package providersync

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const linearProjectEnsureName = "dev_health_linear_project_catalog_ensure_total"

const linearProjectEnsureStoredSelect = `SELECT id, project_key, name, is_active, state, state_source, updated_at ` +
	`FROM projects FINAL WHERE org_id = ? AND provider = 'linear' AND id IN ?`

type linearProjectEnsureQuerier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

type linearStoredProject struct {
	key         string
	name        string
	isActive    uint8
	state       string
	stateSource string
	updatedAt   time.Time
}

func recordLinearProjectEnsure(ctx context.Context, outcome string, rows int) {
	if rows == 0 {
		return
	}
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		linearProjectEnsureName,
		metric.WithDescription("Linear project ensure rows by outcome: written, skipped (cannot change the newest stored row) or read_error (stored rows unreadable, whole batch written)."))
	if err != nil {
		return
	}
	counter.Add(ctx, int64(rows), metric.WithAttributes(attribute.String("outcome", outcome)))
}

// linearProjectEnsureKeep says, per row, whether the ensure row must be written. A row that cannot
// change the newest stored row is dropped: a stored row of a newer version always wins, and a stored
// row of the same version with the same content is replaced by an identical one. Anything missing,
// older or changed is kept. A failed read keeps every row: the skip is an optimisation, never a
// reason to fail the unit.
func linearProjectEnsureKeep(
	ctx context.Context, conn linearProjectEnsureQuerier, orgID string, rows []projectmembership.CatalogRow,
) []bool {
	keep := make([]bool, len(rows))
	for index := range keep {
		keep[index] = true
	}
	if len(rows) == 0 {
		return keep
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	stored, err := readLinearStoredProjects(ctx, conn, orgID, ids)
	if err != nil {
		slog.Default().WarnContext(ctx, "linear_project_catalog_ensure_read_failed",
			"org_id", orgID, "rows", len(rows), "error", err.Error())
		recordLinearProjectEnsure(ctx, "read_error", len(rows))
		return keep
	}
	skipped := 0
	for index, row := range rows {
		if existing, ok := stored[row.ID]; ok && !linearProjectEnsureChangesRow(row, existing) {
			keep[index] = false
			skipped++
		}
	}
	recordLinearProjectEnsure(ctx, "written", len(rows)-skipped)
	recordLinearProjectEnsure(ctx, "skipped", skipped)
	if skipped > 0 {
		slog.Default().InfoContext(ctx, "linear_project_catalog_ensure_skipped",
			"org_id", orgID, "requested", len(rows), "skipped", skipped, "written", len(rows)-skipped)
	}
	return keep
}

func linearProjectEnsureChangesRow(row projectmembership.CatalogRow, stored linearStoredProject) bool {
	version := clickHouseMillis(row.UpdatedAt)
	switch {
	case stored.updatedAt.After(version):
		return false
	case stored.updatedAt.Before(version):
		return true
	}
	return stored.key != row.ProjectKey || stored.name != row.Name || stored.isActive != row.IsActive ||
		stored.state != row.State || stored.stateSource != row.StateSource
}

func readLinearStoredProjects(
	ctx context.Context, conn linearProjectEnsureQuerier, orgID string, ids []string,
) (map[string]linearStoredProject, error) {
	result, err := conn.Query(ctx, linearProjectEnsureStoredSelect, orgID, ids)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	stored := make(map[string]linearStoredProject, len(ids))
	for result.Next() {
		var id string
		var key *string
		var row linearStoredProject
		if err := result.Scan(&id, &key, &row.name, &row.isActive, &row.state, &row.stateSource, &row.updatedAt); err != nil {
			return nil, err
		}
		if key != nil {
			row.key = *key
		}
		row.updatedAt = row.updatedAt.UTC()
		stored[id] = row
	}
	return stored, result.Err()
}
