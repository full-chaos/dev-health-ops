package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type LaunchDarklyClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// GitLabFeatureFlagsClickHouseEffects applies the feature-flag persistence
// contract to GitLab rows. The provider is fixed by the wrapper so a caller
// cannot use a compatible row shape to write a different provider's data.
type GitLabFeatureFlagsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type featureFlagsClickHouseEffects struct {
	Conn     driver.Conn
	Lease    providerfoundation.LeaseGuard
	Provider string
}

func (sink LaunchDarklyClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return sink.shared().WriteEffect(ctx, claim, effect)
}

func (sink LaunchDarklyClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	return sink.shared().InspectEffect(ctx, claim, effect)
}

func (sink LaunchDarklyClickHouseEffects) shared() featureFlagsClickHouseEffects {
	return featureFlagsClickHouseEffects{
		Conn: sink.Conn, Lease: sink.Lease, Provider: "launchdarkly",
	}
}

func (sink GitLabFeatureFlagsClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return sink.shared().WriteEffect(ctx, claim, effect)
}

func (sink GitLabFeatureFlagsClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	return sink.shared().InspectEffect(ctx, claim, effect)
}

func (sink GitLabFeatureFlagsClickHouseEffects) shared() featureFlagsClickHouseEffects {
	return featureFlagsClickHouseEffects{
		Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab",
	}
}

func (sink featureFlagsClickHouseEffects) acceptsDestination(destination string) bool {
	switch destination {
	case "feature_flag", "feature_flag_event", "work_graph_edges":
		return true
	case "feature_flag_link":
		return sink.Provider == "launchdarkly"
	default:
		return false
	}
}

func (sink featureFlagsClickHouseEffects) supportsReadback(destination string) bool {
	if sink.Provider == "launchdarkly" {
		return destination == "feature_flag_event"
	}
	return sink.Provider == "gitlab" && sink.acceptsDestination(destination)
}

func (sink featureFlagsClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		(sink.Provider != "launchdarkly" && sink.Provider != "gitlab") ||
		claim.Provider != sink.Provider || claim.Dataset != "feature-flags" ||
		!sink.acceptsDestination(effect.Destination) {
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
		if err := validateFeatureFlagScope(claim, rows, sink.Provider); err != nil {
			return err
		}
		if len(rows) == 0 && sink.Provider == "gitlab" {
			return nil
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
		if err := validateFeatureFlagEventScope(claim, rows); err != nil {
			return err
		}
		if len(rows) == 0 && sink.Provider == "gitlab" {
			return nil
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
		if err := validateFeatureFlagLinkScope(claim, rows, sink.Provider); err != nil {
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
		if err := validateFeatureFlagEdgeScope(claim, rows, sink.Provider); err != nil {
			return err
		}
		if len(rows) == 0 && sink.Provider == "gitlab" {
			return nil
		}
		if sink.Conn == nil {
			return ErrInvalidConfiguration
		}
		return sink.writeEdges(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink featureFlagsClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		(sink.Provider != "launchdarkly" && sink.Provider != "gitlab") ||
		claim.Provider != sink.Provider || claim.Dataset != "feature-flags" ||
		!sink.supportsReadback(effect.Destination) {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case "feature_flag":
		return sink.inspectFlags(ctx, claim, effect)
	case "feature_flag_event":
		return sink.inspectEvents(ctx, claim, effect)
	case "work_graph_edges":
		return sink.inspectEdges(ctx, claim, effect)
	default:
		// supportsReadback above makes this unreachable, but keep a defensive
		// guard so a future destination cannot accidentally fall through.
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink featureFlagsClickHouseEffects) inspectEvents(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	expected, err := decodeEffectRows[launchDarklyEventRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validateFeatureFlagEventScope(claim, expected); err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 && sink.Provider == "gitlab" {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if len(expected) == 0 {
		// LaunchDarkly historically classified an empty event inspection as
		// exact; retain that behavior for recovery compatibility.
		return EffectExact, nil
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

func (sink featureFlagsClickHouseEffects) inspectFlags(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	expected, err := decodeEffectRows[launchDarklyFlagRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validateFeatureFlagScope(claim, expected, sink.Provider); err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, flag := range expected {
		inspection, err := sink.inspectFlag(ctx, flag)
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

func (sink featureFlagsClickHouseEffects) inspectFlag(
	ctx context.Context,
	expected launchDarklyFlagRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT org_id, provider, flag_key, project_key, repo_id, environment, flag_type,
       created_at, archived_at, last_synced
FROM feature_flag FINAL
WHERE org_id = ? AND provider = ? AND project_key = ? AND flag_key = ? AND environment = ?`,
		expected.OrgID, expected.Provider, expected.ProjectKey, expected.FlagKey,
		expected.Environment,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual launchDarklyFlagRow
	found := false
	for rows.Next() {
		if err := rows.Scan(
			&actual.OrgID, &actual.Provider, &actual.FlagKey, &actual.ProjectKey,
			&actual.RepoID, &actual.Environment, &actual.FlagType, &actual.CreatedAt,
			&actual.ArchivedAt, &actual.LastSynced,
		); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareFeatureFlagVersion(expected, actual, found), nil
}

func compareFeatureFlagVersion(
	expected, actual launchDarklyFlagRow,
	found bool,
) EffectInspection {
	actualVersion := featureFlagsStoredTime(actual.LastSynced)
	expectedVersion := featureFlagsStoredTime(expected.LastSynced)
	if !found || actualVersion.IsZero() {
		return EffectAbsent
	}
	if actualVersion.Before(expectedVersion) {
		return EffectAbsent
	}
	if actualVersion.After(expectedVersion) {
		return EffectConflict
	}
	if actual.OrgID != expected.OrgID || actual.Provider != expected.Provider ||
		actual.FlagKey != expected.FlagKey || actual.ProjectKey != expected.ProjectKey ||
		actual.RepoID != expected.RepoID || actual.Environment != expected.Environment ||
		actual.FlagType != expected.FlagType ||
		!featureFlagsStoredTimePointersEqual(actual.CreatedAt, expected.CreatedAt) ||
		!featureFlagsStoredTimePointersEqual(actual.ArchivedAt, expected.ArchivedAt) {
		return EffectConflict
	}
	return EffectExact
}

func (sink featureFlagsClickHouseEffects) inspectEdges(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	expected, err := decodeEffectRows[launchDarklyEdgeRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := validateFeatureFlagEdgeScope(claim, expected, sink.Provider); err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, edge := range expected {
		inspection, err := sink.inspectEdge(ctx, edge)
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

func (sink featureFlagsClickHouseEffects) inspectEdge(
	ctx context.Context,
	expected launchDarklyEdgeRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT edge_id, source_type, source_id, target_type, target_id, edge_type,
       ifNull(toString(repo_id), ''), provider, provenance, confidence, evidence,
       discovered_at, last_synced, event_ts, toString(day), org_id
FROM work_graph_edges FINAL
WHERE org_id = ? AND source_type = ? AND source_id = ? AND edge_type = ?
  AND target_type = ? AND target_id = ?`,
		expected.OrgID, expected.SourceType, expected.SourceID, expected.EdgeType,
		expected.TargetType, expected.TargetID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual launchDarklyEdgeRow
	var repoID string
	var confidence float32
	found := false
	for rows.Next() {
		if err := rows.Scan(
			&actual.EdgeID, &actual.SourceType, &actual.SourceID, &actual.TargetType,
			&actual.TargetID, &actual.EdgeType, &repoID, &actual.Provider,
			&actual.Provenance, &confidence, &actual.Evidence, &actual.DiscoveredAt,
			&actual.LastSynced, &actual.EventAt, &actual.Day, &actual.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		actual.RepoID = repoID
		actual.Confidence = float64(confidence)
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareFeatureFlagEdgeVersion(expected, actual, found), nil
}

func compareFeatureFlagEdgeVersion(
	expected, actual launchDarklyEdgeRow,
	found bool,
) EffectInspection {
	actualVersion := featureFlagsStoredTime(actual.LastSynced)
	expectedVersion := featureFlagsStoredTime(expected.LastSynced)
	if !found || actualVersion.IsZero() {
		return EffectAbsent
	}
	if actualVersion.Before(expectedVersion) {
		return EffectAbsent
	}
	if actualVersion.After(expectedVersion) {
		return EffectConflict
	}
	if actual.EdgeID != expected.EdgeID || actual.SourceType != expected.SourceType ||
		actual.SourceID != expected.SourceID || actual.TargetType != expected.TargetType ||
		actual.TargetID != expected.TargetID || actual.EdgeType != expected.EdgeType ||
		actual.RepoID != expected.RepoID || actual.Provider != expected.Provider ||
		actual.Provenance != expected.Provenance ||
		float32(actual.Confidence) != float32(expected.Confidence) ||
		actual.Evidence != expected.Evidence ||
		!featureFlagsStoredTime(actual.DiscoveredAt).Equal(featureFlagsStoredTime(expected.DiscoveredAt)) ||
		!featureFlagsStoredTime(actual.EventAt).Equal(featureFlagsStoredTime(expected.EventAt)) || actual.Day != expected.Day ||
		actual.OrgID != expected.OrgID {
		return EffectConflict
	}
	return EffectExact
}

// All feature-flag destinations use DateTime64(3). Compare the value as it
// exists in ClickHouse, not as it appeared in a provider payload with finer
// precision. Recovery receives the original effect snapshot, while the
// server has already quantized the inserted timestamp to milliseconds.
func featureFlagsStoredTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Millisecond)
}

func featureFlagsStoredTimePointersEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return featureFlagsStoredTime(*left).Equal(featureFlagsStoredTime(*right))
}

func validateFeatureFlagScope(
	claim Claim,
	rows []launchDarklyFlagRow,
	provider string,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != provider {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateFeatureFlagEventScope(
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

func validateFeatureFlagLinkScope(
	claim Claim,
	rows []launchDarklyLinkRow,
	provider string,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != provider {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

func validateFeatureFlagEdgeScope(
	claim Claim,
	rows []launchDarklyEdgeRow,
	provider string,
) error {
	for _, row := range rows {
		if row.OrgID != claim.OrgID || row.Provider != provider {
			return providerfoundation.ErrInvalidScope
		}
	}
	return nil
}

// Keep the original LaunchDarkly-scoped helpers as narrow compatibility
// shims for package-local callers while the shared implementation serves both
// fixed-provider wrappers.
func validateLaunchDarklyFlagScope(claim Claim, rows []launchDarklyFlagRow) error {
	return validateFeatureFlagScope(claim, rows, "launchdarkly")
}

func validateLaunchDarklyEventScope(claim Claim, rows []launchDarklyEventRow) error {
	return validateFeatureFlagEventScope(claim, rows)
}

func validateLaunchDarklyLinkScope(claim Claim, rows []launchDarklyLinkRow) error {
	return validateFeatureFlagLinkScope(claim, rows, "launchdarkly")
}

func validateLaunchDarklyEdgeScope(claim Claim, rows []launchDarklyEdgeRow) error {
	return validateFeatureFlagEdgeScope(claim, rows, "launchdarkly")
}

func (sink LaunchDarklyClickHouseEffects) writeFlags(
	ctx context.Context,
	rows []launchDarklyFlagRow,
) error {
	return sink.shared().writeFlags(ctx, rows)
}

func (sink LaunchDarklyClickHouseEffects) writeEvents(
	ctx context.Context,
	rows []launchDarklyEventRow,
) error {
	return sink.shared().writeEvents(ctx, rows)
}

func (sink LaunchDarklyClickHouseEffects) writeLinks(
	ctx context.Context,
	rows []launchDarklyLinkRow,
) error {
	return sink.shared().writeLinks(ctx, rows)
}

func (sink LaunchDarklyClickHouseEffects) writeEdges(
	ctx context.Context,
	rows []launchDarklyEdgeRow,
) error {
	return sink.shared().writeEdges(ctx, rows)
}

func (sink LaunchDarklyClickHouseEffects) inspectEvent(
	ctx context.Context,
	expected launchDarklyEventRow,
) (EffectInspection, error) {
	return sink.shared().inspectEvent(ctx, expected)
}

func (sink featureFlagsClickHouseEffects) writeFlags(
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

func (sink featureFlagsClickHouseEffects) writeEvents(
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

func (sink featureFlagsClickHouseEffects) writeLinks(
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

func (sink featureFlagsClickHouseEffects) writeEdges(
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

func (sink featureFlagsClickHouseEffects) inspectEvent(
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
	storedExpected := expected
	storedExpected.EventAt = featureFlagsStoredTime(expected.EventAt)
	storedExpected.IngestedAt = featureFlagsStoredTime(expected.IngestedAt)
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
		actual.EventAt = featureFlagsStoredTime(actual.EventAt)
		actual.IngestedAt = featureFlagsStoredTime(actual.IngestedAt)
		if actual == storedExpected {
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	switch {
	case found == 0:
		return EffectAbsent, nil
	case found > 0 && matched == found:
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
var _ EffectSink = GitLabFeatureFlagsClickHouseEffects{}
var _ EffectReadback = GitLabFeatureFlagsClickHouseEffects{}
