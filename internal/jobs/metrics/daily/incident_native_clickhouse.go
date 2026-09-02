package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// IncidentRow is one deduplicated repository-scoped incident, as read by
// LoadIncidentsStarted -- the Go counterpart of active_incidents.py's
// IncidentRow TypedDict (metrics/schemas.py:144) and the shape
// deduplicate_active_incidents/compute_incident_metrics_daily consume.
type IncidentRow struct {
	RepoID     uuid.UUID
	IncidentID string
	Status     string
	StartedAt  time.Time
	ResolvedAt *time.Time
}

// LoadIncidentsStarted ports active_incidents_query(window=STARTED, ...)
// (src/dev_health_ops/metrics/active_incidents.py:21) plus
// loaders/clickhouse.py:1234 load_incidents -- the SAME join shape, scoped
// to started_at within [dayStart, dayEnd) and to the partition's repo_ids in
// one call (job_daily.py calls the Python equivalent once per repo_id; this
// executor computes every repo in the partition in one round trip, exactly
// like RepoUserCommitExecutor/TeamWellbeingExecutor -- compute_incident_metrics_daily's
// per-(repo_id) bucketing is unaffected by widening the query, only by the
// per-row started_at/resolved_at day filters, which stay row-local either
// way).
//
// # THE FIX (CHAOS-4269/CHAOS-4295)
//
// active_incidents_query's mapping-join predicate is
// `valid_from <= {as_of}` with NO NULL-OK guard -- unlike the symmetric
// `valid_to IS NULL OR valid_to > {as_of}` clause two lines below it.
// map_issue_incidents (providers/operational_migration.py:196-210), the
// ONLY writer of mapping_kind="repository_derived" mappings, never sets
// valid_from; the dataclass default (models/operational.py:277) is NULL.
// ClickHouse's three-valued logic makes `NULL <= x` evaluate to NULL
// (falsy), so every repository-derived mapping -- and therefore every
// incident joined through it -- is silently dropped from the WHERE/INNER
// JOIN. Confirmed with executed evidence on the shared local stack
// 2026-09-01 (CHAOS-4269 comment): `incident_metrics_daily` computes ZERO
// rows, unfiltered, across every org/day; a predicate swap on a seeded org
// recovered 36/36 previously-dropped rows.
//
// This loader adds the missing guard: `(valid_from IS NULL OR
// valid_from <= {as_of})`, mirroring valid_to's existing shape exactly --
// port-with-fix (chris/team-lead standing order: no Python patch, fix lands
// only in the native Go port). See ordering-contract note below for the
// other faithfully-ported half of this query (current_operational_rows_sql's
// LEGACY/FINAL branch, the live default).
//
// # Ordering contract
//
// current_operational_rows_sql (storage/operational_current.py:25) branches
// on configured_operational_ordering_contract(): LEGACY (`... FINAL WHERE
// org_id = ...`) when OPERATIONAL_ORDERING_CONTRACT is unset, CURRENT (an
// explicit `ORDER BY ... LIMIT 1 BY org_id, id` dedup) otherwise.
// parse_operational_ordering_contract defaults to LEGACY on an unset env var
// (storage/operational_ordering_guard.py:62-69), and the local/prod compose
// manifests do not set it (verified: `docker compose config --no-interpolate
// | rg OPERATIONAL_ORDERING_CONTRACT` finds nothing) -- LEGACY/FINAL is the
// live default this loader ports. Both `operational_incidents` and
// `operational_service_repository_mappings` are ReplacingMergeTree, so FINAL
// is a correct, if potentially stale-between-merges, dedup for that branch,
// exactly as it is for the Python query.
//
// observer, when non-nil, records how many matched mapping rows had
// valid_from set versus NULL (CHAOS-4269 telemetry) -- see
// jobruntime.IncidentValidFromGuardObserver's doc comment for why this
// matters as an ongoing signal, not just a one-time proof.
func LoadIncidentsStarted(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, dayStart, dayEnd time.Time, asOf time.Time,
	observer jobruntime.IncidentValidFromGuardObserver,
) ([]IncidentRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !dayStart.Before(dayEnd) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT repo_id, incident_id, status, started_at, resolved_at, mapping_valid_from
FROM (
    SELECT
        mapping.repo_id AS repo_id,
        incident.id AS incident_id,
        incident.normalized_status AS status,
        incident.started_at AS started_at,
        incident.resolved_at AS resolved_at,
        incident.last_synced AS last_synced,
        mapping.valid_from AS mapping_valid_from
    FROM (
        SELECT * FROM operational_incidents FINAL
        WHERE org_id = ?
          AND is_deleted = 0
          AND started_at >= ? AND started_at < ?
    ) AS incident
    INNER JOIN (
        SELECT * FROM operational_service_repository_mappings FINAL
        WHERE org_id = ?
          AND repo_id IS NOT NULL
          AND is_active = 1
          AND (valid_from IS NULL OR valid_from <= ?)
          AND (valid_to IS NULL OR valid_to > ?)
    ) AS mapping
        ON incident.org_id = mapping.org_id AND incident.service_id = mapping.service_id
    INNER JOIN repos AS repo FINAL
        ON mapping.org_id = repo.org_id AND mapping.repo_id = repo.id
    WHERE mapping.repo_id IS NOT NULL AND mapping.repo_id IN ?
    ORDER BY mapping.repo_id, incident.id, incident.last_synced DESC
    LIMIT 1 BY mapping.repo_id, incident.id
)
ORDER BY repo_id, incident_id`,
		organizationID, dayStart.UTC(), dayEnd.UTC(),
		organizationID, asOf.UTC(), asOf.UTC(),
		repositoryUUIDStrings(repoIDs),
	)
	if err != nil {
		return nil, fmt.Errorf("load incidents started: %w", err)
	}
	defer rows.Close()

	type seenKey struct {
		repoID     uuid.UUID
		incidentID string
	}
	seen := make(map[seenKey]struct{})
	var incidents []IncidentRow
	setCount, nullRecoveredCount := 0, 0
	for rows.Next() {
		var (
			repoID           uuid.UUID
			incidentID       string
			status           *string
			startedAt        time.Time
			resolvedAt       *time.Time
			mappingValidFrom *time.Time
		)
		if err := rows.Scan(&repoID, &incidentID, &status, &startedAt, &resolvedAt, &mappingValidFrom); err != nil {
			return nil, fmt.Errorf("scan incident row: %w", err)
		}
		if mappingValidFrom == nil {
			nullRecoveredCount++
		} else {
			setCount++
		}
		key := seenKey{repoID: repoID, incidentID: incidentID}
		if _, dup := seen[key]; dup {
			// Defensive parity with deduplicate_active_incidents: the
			// LIMIT 1 BY above already enforces uniqueness per
			// (mapping.repo_id, incident.id), so this branch is expected to
			// never fire -- kept only because the Python original has the
			// identical redundant-looking second dedup pass and a native
			// port that silently dropped it would be a behavioral
			// assumption, not a proven equivalence.
			continue
		}
		seen[key] = struct{}{}
		incidents = append(incidents, IncidentRow{
			RepoID:     repoID,
			IncidentID: incidentID,
			Status:     derefWellbeingString(status),
			StartedAt:  startedAt,
			ResolvedAt: resolvedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incident rows: %w", err)
	}

	if observer != nil {
		if setCount > 0 {
			_ = observer.ObserveIncidentValidFromGuardRows(jobruntime.IncidentValidFromGuardReasonSet, setCount)
		}
		if nullRecoveredCount > 0 {
			_ = observer.ObserveIncidentValidFromGuardRows(jobruntime.IncidentValidFromGuardReasonNullRecovered, nullRecoveredCount)
		}
	}

	return incidents, nil
}

// incidentBatchConn is the narrow write capability
// WriteIncidentMetricsDaily needs.
type incidentBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteIncidentMetricsDaily ports the write side of write_incident_metrics
// (job_daily.py:1837, sinks/clickhouse) -- the same table and column order.
// incident_metrics_daily is a legacy append-only MergeTree (not
// ReplacingMergeTree); readers dedup to the latest computed_at per
// (org_id, repo_id, day) (clickhouse_dedup.py:74), so one shared
// computed_at for the whole partition write is sufficient -- unlike
// team_metrics_daily, no row in this table shares its (org_id, repo_id, day)
// key with any other row this same call writes.
func WriteIncidentMetricsDaily(
	ctx context.Context, conn incidentBatchConn, organizationID string,
	records []IncidentMetricsDailyRecord, computedAt time.Time,
) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO incident_metrics_daily (
		repo_id, day, incidents_count, mttr_p50_hours, mttr_p90_hours, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare incident_metrics_daily batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, record := range records {
		if err := batch.Append(
			record.RepoID, record.Day, record.IncidentsCount,
			record.MTTRP50Hours, record.MTTRP90Hours,
			computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append incident_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send incident_metrics_daily batch: %w", err)
	}
	return len(records), nil
}
