package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type LaunchDarklyClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink LaunchDarklyClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil ||
		claim.Validate() != nil || claim.Provider != "launchdarkly" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	switch effect.Destination {
	case "feature_flag":
		rows, err := decodeEffectRows[launchDarklyFlagRow](effect)
		if err != nil {
			return err
		}
		if err := validateLaunchDarklyFlagScope(claim, rows); err != nil {
			return err
		}
		if sink.Conn == nil {
			return ErrInvalidConfiguration
		}
		return sink.writeFlags(ctx, rows)
	case "feature_flag_event":
		rows, err := decodeEffectRows[launchDarklyEventRow](effect)
		if err != nil {
			return err
		}
		if err := validateLaunchDarklyEventScope(claim, rows); err != nil {
			return err
		}
		if sink.Conn == nil {
			return ErrInvalidConfiguration
		}
		return sink.writeEvents(ctx, rows)
	case "feature_flag_link":
		rows, err := decodeEffectRows[launchDarklyLinkRow](effect)
		if err != nil {
			return err
		}
		if err := validateLaunchDarklyLinkScope(claim, rows); err != nil {
			return err
		}
		if sink.Conn == nil {
			return ErrInvalidConfiguration
		}
		return sink.writeLinks(ctx, rows)
	case "work_graph_edges":
		rows, err := decodeEffectRows[launchDarklyEdgeRow](effect)
		if err != nil {
			return err
		}
		if err := validateLaunchDarklyEdgeScope(claim, rows); err != nil {
			return err
		}
		if sink.Conn == nil {
			return ErrInvalidConfiguration
		}
		return sink.writeEdges(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink LaunchDarklyClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil ||
		claim.Validate() != nil || effect.Destination != "feature_flag_event" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[launchDarklyEventRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validateLaunchDarklyEventScope(claim, expected); err != nil {
		return EffectConflict, err
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, event := range expected {
		inspection, err := sink.inspectEvent(ctx, event)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	switch {
	case exact == len(expected):
		return EffectExact, nil
	case absent == len(expected):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

func validateLaunchDarklyFlagScope(
	claim Claim,
	rows []launchDarklyFlagRow,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != claim.Provider {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateLaunchDarklyEventScope(
	claim Claim,
	rows []launchDarklyEventRow,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateLaunchDarklyLinkScope(
	claim Claim,
	rows []launchDarklyLinkRow,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != claim.Provider {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateLaunchDarklyEdgeScope(
	claim Claim,
	rows []launchDarklyEdgeRow,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != claim.Provider {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func (sink LaunchDarklyClickHouseEffects) writeFlags(
	ctx context.Context,
	rows []launchDarklyFlagRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO feature_flag (
  org_id, provider, flag_key, project_key, repo_id, environment, flag_type,
  created_at, archived_at, last_synced
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.Provider, row.FlagKey, row.ProjectKey, row.RepoID,
			row.Environment, row.FlagType, row.CreatedAt, row.ArchivedAt,
			row.LastSynced,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LaunchDarklyClickHouseEffects) writeEvents(
	ctx context.Context,
	rows []launchDarklyEventRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO feature_flag_event (
  org_id, event_type, flag_key, environment, repo_id, actor_type, prev_state,
  next_state, event_ts, ingested_at, source_event_id, dedupe_key
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.EventType, row.FlagKey, row.Environment, row.RepoID,
			row.ActorType, row.PrevState, row.NextState, row.EventAt,
			row.IngestedAt, row.SourceEventID, row.DedupeKey,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LaunchDarklyClickHouseEffects) writeLinks(
	ctx context.Context,
	rows []launchDarklyLinkRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO feature_flag_link (
  org_id, flag_key, target_type, target_id, provider, link_source, link_type,
  evidence_type, confidence, valid_from, valid_to, last_synced
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.FlagKey, row.TargetType, row.TargetID, row.Provider,
			row.LinkSource, row.LinkType, row.EvidenceType, float32(row.Confidence),
			row.ValidFrom, row.ValidTo, row.LastSynced,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LaunchDarklyClickHouseEffects) writeEdges(
	ctx context.Context,
	rows []launchDarklyEdgeRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO work_graph_edges (
  edge_id, source_type, source_id, target_type, target_id, edge_type, repo_id,
  provider, provenance, confidence, evidence, discovered_at, last_synced,
  event_ts, day, org_id
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		var repoID any
		if row.RepoID != "" {
			repoID = row.RepoID
		}
		if err := batch.Append(
			row.EdgeID, row.SourceType, row.SourceID, row.TargetType, row.TargetID,
			row.EdgeType, repoID, row.Provider, row.Provenance,
			float32(row.Confidence), row.Evidence, row.DiscoveredAt,
			row.LastSynced, row.EventAt, row.Day, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LaunchDarklyClickHouseEffects) inspectEvent(
	ctx context.Context,
	expected launchDarklyEventRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT
  org_id, event_type, flag_key, environment, repo_id, actor_type, prev_state,
  next_state, event_ts, ingested_at, source_event_id, dedupe_key
FROM feature_flag_event
WHERE org_id = ? AND dedupe_key = ?`,
		expected.OrgID, expected.DedupeKey,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	found, matched := 0, 0
	for rows.Next() {
		var actual launchDarklyEventRow
		if err := rows.Scan(
			&actual.OrgID, &actual.EventType, &actual.FlagKey, &actual.Environment,
			&actual.RepoID, &actual.ActorType, &actual.PrevState,
			&actual.NextState, &actual.EventAt, &actual.IngestedAt,
			&actual.SourceEventID, &actual.DedupeKey,
		); err != nil {
			return EffectConflict, err
		}
		found++
		actual.EventAt = actual.EventAt.UTC()
		actual.IngestedAt = actual.IngestedAt.UTC()
		expected.EventAt = expected.EventAt.UTC()
		expected.IngestedAt = expected.IngestedAt.UTC()
		if actual == expected {
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	switch {
	case found == 0:
		return EffectAbsent, nil
	case found == 1 && matched == 1:
		return EffectExact, nil
	default:
		return EffectConflict, nil
	}
}

func decodeEffectRows[T any](effect EffectBatch) ([]T, error) {
	rows := make([]T, 0, len(effect.Rows))
	total := 0
	for _, raw := range effect.Rows {
		total += len(raw)
		if total > maxEffectPayloadBytes {
			return nil, ErrEffectRecoveryUnsafe
		}
		var row T
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("%w: effect row", ErrEffectRecoveryUnsafe)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

var _ EffectSink = LaunchDarklyClickHouseEffects{}
var _ EffectReadback = LaunchDarklyClickHouseEffects{}
