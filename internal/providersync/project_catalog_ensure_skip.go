package providersync

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const projectEnsureName = "dev_health_project_catalog_ensure_total"

const projectEnsureStoredSelect = `SELECT id, project_key, name, is_active, state, state_source, target_state, updated_at ` +
	`FROM projects FINAL WHERE org_id = ? AND provider = ? AND id IN ?`

type projectEnsureQuerier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

type storedEnsureProject struct {
	key         string
	name        string
	isActive    uint8
	state       string
	stateSource string
	targetState string
	updatedAt   time.Time
}

func recordProjectEnsure(ctx context.Context, provider, outcome string, rows int) {
	if rows == 0 {
		return
	}
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		projectEnsureName,
		metric.WithDescription("Project catalog ensure rows by provider and outcome: written, skipped (cannot change the newest stored row) or read_error (stored rows unreadable, whole batch written)."))
	if err != nil {
		return
	}
	counter.Add(ctx, int64(rows), metric.WithAttributes(attribute.String("provider", provider), attribute.String("outcome", outcome)))
}

// projectEnsureKeep says, per row, whether the ensure row must be written. A row that cannot
// change the newest stored row is dropped: a stored row of a newer version always wins, and a stored
// row with the same content is kept at whatever version it has (Jira stamps each window's wall clock
// as the version, so a later window would only rewrite identical content). Anything missing or
// changed under an older stored version is kept. A failed read keeps every row: the skip is an optimisation, never a
// reason to fail the unit.
func projectEnsureKeep(
	ctx context.Context, conn projectEnsureQuerier, provider, orgID string, rows []projectmembership.CatalogRow,
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
	stored, err := readStoredEnsureProjects(ctx, conn, provider, orgID, ids)
	if err != nil {
		errorClass, errorType := logging.ErrorClass(err), logging.ErrorType(err) // never the error text (CHAOS-7933)
		slog.Default().WarnContext(ctx, "project_catalog_ensure_read_failed",
			"provider", provider, "org_id", orgID, "rows", len(rows), "error_class", errorClass, "error_type", errorType)
		recordProjectEnsure(ctx, provider, "read_error", len(rows))
		return keep
	}
	skipped := 0
	for index, row := range rows {
		if existing, ok := stored[row.ID]; ok && !projectEnsureChangesRow(row, existing) {
			keep[index] = false
			skipped++
		}
	}
	recordProjectEnsure(ctx, provider, "written", len(rows)-skipped)
	recordProjectEnsure(ctx, provider, "skipped", skipped)
	if skipped > 0 {
		slog.Default().InfoContext(ctx, "project_catalog_ensure_skipped",
			"provider", provider, "org_id", orgID, "requested", len(rows), "skipped", skipped, "written", len(rows)-skipped)
	}
	return keep
}

func projectEnsureChangesRow(row projectmembership.CatalogRow, stored storedEnsureProject) bool {
	// An ensure row reads no target: written over a read target it flips that target to held.
	flipsTarget := (stored.targetState == projectTargetStated || stored.targetState == projectTargetNoneDate) &&
		!stored.updatedAt.After(clickHouseMillis(row.UpdatedAt))
	if !flipsTarget && stored.key == row.ProjectKey && stored.name == row.Name && stored.isActive == row.IsActive &&
		stored.state == row.State && stored.stateSource == row.StateSource {
		return false
	}
	return !stored.updatedAt.After(clickHouseMillis(row.UpdatedAt))
}

func readStoredEnsureProjects(
	ctx context.Context, conn projectEnsureQuerier, provider, orgID string, ids []string,
) (map[string]storedEnsureProject, error) {
	result, err := conn.Query(ctx, projectEnsureStoredSelect, orgID, provider, ids)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	stored := make(map[string]storedEnsureProject, len(ids))
	for result.Next() {
		var id string
		var key *string
		var row storedEnsureProject
		if err := result.Scan(&id, &key, &row.name, &row.isActive, &row.state, &row.stateSource, &row.targetState, &row.updatedAt); err != nil {
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

// narrowProjectEnsureEffect drops the rows of a projects effect that cannot change the newest stored
// row. The returned batch is the effect itself when nothing is dropped; empty reports that every row
// was dropped.
func narrowProjectEnsureEffect(
	ctx context.Context, conn projectEnsureQuerier, provider, orgID string,
	effect EffectBatch, rows []projectmembership.CatalogRow,
) (narrowed EffectBatch, empty bool, err error) {
	keep := projectEnsureKeep(ctx, conn, provider, orgID, rows)
	kept := make([]json.RawMessage, 0, len(effect.Rows))
	for index, raw := range effect.Rows {
		if keep[index] {
			kept = append(kept, raw)
		}
	}
	if len(kept) == len(effect.Rows) {
		return effect, false, nil
	}
	if len(kept) == 0 {
		return EffectBatch{}, true, nil
	}
	narrowed, err = BuildEffectBatch(effect.Destination, effect.Recovery, kept)
	return narrowed, false, err
}
