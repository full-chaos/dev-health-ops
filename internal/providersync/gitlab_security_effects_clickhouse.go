package providersync

import (
	"context"
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabSecurityClickHouseEffects is the tenant-fenced persistence boundary
// for security_alerts. The schema is supplied by the real migrated
// ClickHouse chain; this file intentionally contains no local DDL.
type GitLabSecurityClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabSecurityClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" || claim.Dataset != "security" ||
		effect.Destination != "security_alerts" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeGitLabSecurityRows(effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO security_alerts (org_id, repo_id, alert_id, source, severity, state, package_name, cve_id, url, title, description, created_at, fixed_at, dismissed_at, last_synced)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.RepoID, row.AlertID, row.Source, row.Severity,
			row.State, row.PackageName, row.CVEID, row.URL, row.Title,
			row.Description, row.CreatedAt, row.FixedAt, row.DismissedAt,
			row.LastSynced,
		); err != nil {
			return err
		}
	}
	// A lease can expire while PrepareBatch is building a large effect. Do not
	// send a batch after that boundary; the caller can retry the same frozen
	// manifest under a fresh lease.
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitLabSecurityClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" || claim.Dataset != "security" ||
		effect.Destination != "security_alerts" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeGitLabSecurityRows(effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := sink.inspectGitLabSecurityAlert(ctx, row)
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
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func decodeGitLabSecurityRows(effect EffectBatch) ([]gitLabSecurityAlertRow, error) {
	rows, err := decodeEffectRows[gitLabSecurityAlertRow](effect)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := row.OrgID + "\x00" + row.RepoID + "\x00" + row.AlertID
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.Join(providerfoundation.ErrSinkDuplicate, ErrEffectRecoveryUnsafe)
		}
		seen[key] = struct{}{}
	}
	return rows, nil
}

func (sink GitLabSecurityClickHouseEffects) inspectGitLabSecurityAlert(
	ctx context.Context,
	expected gitLabSecurityAlertRow,
) (EffectInspection, error) {
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	rows, err := sink.Conn.Query(ctx, `SELECT org_id, repo_id, alert_id, source, severity, state, package_name, cve_id, url, title, description, created_at, fixed_at, dismissed_at, last_synced FROM security_alerts FINAL WHERE org_id = ? AND repo_id = ? AND alert_id = ?`, expected.OrgID, expected.RepoID, expected.AlertID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual gitLabSecurityAlertRow
	found := 0
	for rows.Next() {
		if err := rows.Scan(
			&actual.OrgID, &actual.RepoID, &actual.AlertID, &actual.Source,
			&actual.Severity, &actual.State, &actual.PackageName, &actual.CVEID,
			&actual.URL, &actual.Title, &actual.Description, &actual.CreatedAt,
			&actual.FixedAt, &actual.DismissedAt, &actual.LastSynced,
		); err != nil {
			return EffectConflict, err
		}
		found++
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return gitLabSecurityReadbackDecision(found, expected, actual), nil
}

func gitLabSecurityReadbackDecision(
	found int,
	expected, actual gitLabSecurityAlertRow,
) EffectInspection {
	if found > 1 {
		return EffectConflict
	}
	return compareGitLabSecurityAlertVersion(expected, actual, found == 1)
}

func compareGitLabSecurityAlertVersion(
	expected, actual gitLabSecurityAlertRow,
	found bool,
) EffectInspection {
	if !found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) {
		return EffectConflict
	}
	if actual.OrgID != expected.OrgID || actual.RepoID != expected.RepoID ||
		actual.AlertID != expected.AlertID || actual.Source != expected.Source ||
		!stringPointersEqual(actual.Severity, expected.Severity) ||
		!stringPointersEqual(actual.State, expected.State) ||
		!stringPointersEqual(actual.PackageName, expected.PackageName) ||
		!stringPointersEqual(actual.CVEID, expected.CVEID) ||
		!stringPointersEqual(actual.URL, expected.URL) ||
		!stringPointersEqual(actual.Title, expected.Title) ||
		!stringPointersEqual(actual.Description, expected.Description) ||
		!actual.CreatedAt.UTC().Equal(expected.CreatedAt.UTC()) ||
		!timePointersEqual(actual.FixedAt, expected.FixedAt) ||
		!timePointersEqual(actual.DismissedAt, expected.DismissedAt) {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitLabSecurityClickHouseEffects{}
var _ EffectReadback = GitLabSecurityClickHouseEffects{}
