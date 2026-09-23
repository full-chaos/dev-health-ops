package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

const (
	lastReportDescription = "Timestamp of the last voluntary telemetry report."
	// sendTimeout is httpx.AsyncClient(timeout=10.0).
	sendTimeout = 10 * time.Second
)

// requirePlatformRole is require_platform_role: a superuser who is not
// impersonating. It runs before the org is resolved, as its dependency
// order does.
func (h handlers) requirePlatformRole(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := policy.UserFrom(r.Context())
		if policy.ImpersonationFrom(r.Context()) != nil || !user.IsSuperuser {
			rejected.Add(r.Context(), 1, metric.WithAttributes(attribute.String("reason", "report_not_platform_role")))
			h.logger.WarnContext(r.Context(), "telemetry report refused", slog.String("reason", "report_not_platform_role"))
			policy.WriteDetail(w, http.StatusForbidden, "Instance-wide telemetry statistics require a platform role", nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// report is telemetry_report: opted-in org only; instance-wide usage
// counts; the report sent to TELEMETRY_ENDPOINT when set; the org's
// last-report time stored; one audit row recorded.
//
// The counts are per table. Python's collect_usage_stats selects every
// count over organizations, users, repos, memberships and
// sync_configurations with no join, so each of its totals is the product
// of the table sizes; the per-table count is the intended value (ruled).
// Python's query also names a Postgres repos table the Alembic schema does
// not create (repositories live in ClickHouse), so on a real database it
// fails; total_repos is 0 here.
func (h handlers) report(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID := ctx.Value(orgKey{}).(string)
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		h.internal(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	value, found, err := setting(ctx, tx, orgID, optInKey)
	if err != nil {
		h.internal(w, r, "read opt-in", err)
		return
	}
	if !optedIn(value, found) {
		policy.WriteDetail(w, http.StatusForbidden, "Telemetry is not enabled for this org", nil)
		return
	}
	collectedAt := h.now().UTC()
	report, err := usageStats(ctx, tx, collectedAt)
	if err != nil {
		h.internal(w, r, "usage stats", err)
		return
	}
	report.Set("version", version.Current(config.APIServiceName).Version)
	report.Set("collected_at", pytime.Pydantic(pytime.UTC(collectedAt)))
	payload, err := pyjson.Marshal(report)
	if err != nil {
		h.internal(w, r, "render", err)
		return
	}
	statusCode := h.send(ctx, payload)
	if err := h.setLastReportAt(ctx, tx, orgID, collectedAt); err != nil {
		h.internal(w, r, "store last report", err)
		return
	}
	if err := h.recordHeartbeat(ctx, tx, orgID, statusCode, collectedAt); err != nil {
		h.internal(w, r, "record heartbeat", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internal(w, r, "commit", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func usageStats(ctx context.Context, q rowQuerier, now time.Time) (*pyjson.Object, error) {
	var orgs, activeOrgs, users, activeUsers, repos, syncs, activeSyncs int64
	err := q.QueryRow(ctx, `SELECT
		(SELECT count(id) FROM organizations),
		(SELECT count(id) FROM organizations WHERE is_active IS TRUE),
		(SELECT count(id) FROM users),
		(SELECT count(id) FROM users WHERE is_active IS TRUE),
		0,
		(SELECT count(id) FROM sync_configurations),
		(SELECT count(id) FROM sync_configurations WHERE is_active IS TRUE AND last_sync_at >= $1)`,
		now.Add(-24*time.Hour)).Scan(&orgs, &activeOrgs, &users, &activeUsers, &repos, &syncs, &activeSyncs)
	if err != nil {
		return nil, err
	}
	tiers := pyjson.NewObject()
	rows, err := q.Query(ctx, `SELECT tier, count(id) FROM organizations GROUP BY tier`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tier *string
		var count int64
		if err := rows.Scan(&tier, &count); err != nil {
			rows.Close()
			return nil, err
		}
		key := "unknown"
		if tier != nil && *tier != "" {
			key = *tier
		}
		tiers.Set(key, count)
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	features := pyjson.NewObject()
	rows, err = q.Query(ctx, `SELECT f.key, count(o.id) FROM feature_flags f
		JOIN org_feature_overrides o ON o.feature_id = f.id
		WHERE o.is_enabled IS TRUE AND (o.expires_at IS NULL OR o.expires_at >= $1)
		GROUP BY f.key`, now)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			rows.Close()
			return nil, err
		}
		if key != "" {
			features.Set(key, count)
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	out := pyjson.NewObject()
	for _, pair := range []struct {
		key   string
		value int64
	}{{"total_organizations", orgs}, {"active_organizations", activeOrgs}, {"total_users", users},
		{"active_users", activeUsers}, {"total_repos", repos}, {"total_sync_configs", syncs},
		{"active_syncs_24h", activeSyncs}} {
		out.Set(pair.key, pair.value)
	}
	out.Set("tier_distribution", tiers)
	out.Set("feature_usage", features)
	return out, nil
}

// send is send_report: POST the report JSON to TELEMETRY_ENDPOINT; the
// status code, or nil when unset or on any error (logged).
func (h handlers) send(ctx context.Context, payload []byte) pyjson.Value {
	if h.endpoint == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, bytes.NewReader(payload))
	if err != nil {
		h.logger.WarnContext(ctx, "telemetry report send failed", slog.String("error", err.Error()))
		return nil
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := h.client.Do(request)
	if err != nil {
		h.logger.WarnContext(ctx, "telemetry report send failed", slog.String("error", err.Error()))
		return nil
	}
	_ = response.Body.Close()
	return int64(response.StatusCode)
}

func (h handlers) setLastReportAt(ctx context.Context, tx pgx.Tx, orgID string, at time.Time) error {
	value := pytime.ISOFormat(at)
	now := h.now().UTC()
	var existing uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM settings WHERE org_id = $1 AND category = $2 AND key = $3 FOR UPDATE`,
		orgID, category, lastReportAtKey).Scan(&existing)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, false, $6, $7, $7)`, uuid.New(), orgID, category, lastReportAtKey, value, lastReportDescription, now)
		return err
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE settings SET value = $2, is_encrypted = false, description = $3, updated_at = $4 WHERE id = $1`,
		existing, value, lastReportDescription, now)
	return err
}

// recordHeartbeat is record_heartbeat: the org is the id, or the org whose
// slug it is; with neither, no row (the ids are never empty here).
func (h handlers) recordHeartbeat(ctx context.Context, tx pgx.Tx, orgID string, statusCode pyjson.Value, at time.Time) error {
	resolved, ok := policy.ParsePyUUID(orgID)
	if !ok {
		err := tx.QueryRow(ctx, `SELECT id FROM organizations WHERE slug = $1`, orgID).Scan(&resolved)
		if errors.Is(err, pgx.ErrNoRows) {
			h.logger.DebugContext(ctx, "skipping telemetry audit record; org not found")
			return nil
		}
		if err != nil {
			return err
		}
	}
	changes := pyjson.NewObject()
	changes.Set("event", "telemetry_report")
	changes.Set("status_code", statusCode)
	changes.Set("collected_at", pytime.ISOFormat(at))
	changes.Set("org_id", orgID)
	changesJSON, err := pythonDumps(changes)
	if err != nil {
		return err
	}
	now := h.now().UTC()
	_, err = tx.Exec(ctx, `INSERT INTO audit_logs (id, org_id, user_id, action, resource_type, resource_id, description,
		changes, request_metadata, status, error_message, created_at)
		VALUES ($1, $2, NULL, 'other', 'other', 'telemetry', 'Telemetry heartbeat/report recorded', $3::json,
		'{"source": "telemetry"}'::json, 'success', NULL, $4)`, uuid.New(), resolved, changesJSON, now)
	return err
}

// pythonDumps is json.dumps with its defaults, as SQLAlchemy's JSON type
// serializes a column value.
func pythonDumps(value *pyjson.Object) (string, error) { return pyjson.Dumps(value) }
