package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubSecurityClickHouseEffects writes and tenant-fences security_alerts.
type GitHubSecurityClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubSecurityClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "security" || effect.Destination != "security_alerts" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[securityAlertRow](effect)
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
		if err := batch.Append(row.OrgID, row.RepoID, row.AlertID, row.Source, row.Severity, row.State, row.PackageName, row.CVEID, row.URL, row.Title, row.Description, row.CreatedAt, row.FixedAt, row.DismissedAt, row.LastSynced); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubSecurityClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "security" || effect.Destination != "security_alerts" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[securityAlertRow](effect)
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
		inspection, err := sink.inspectSecurityAlert(ctx, row)
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

func (sink GitHubSecurityClickHouseEffects) inspectSecurityAlert(ctx context.Context, expected securityAlertRow) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `SELECT org_id, repo_id, alert_id, source, severity, state, package_name, cve_id, url, title, description, created_at, fixed_at, dismissed_at, last_synced FROM security_alerts FINAL WHERE org_id = ? AND repo_id = ? AND alert_id = ?`, expected.OrgID, expected.RepoID, expected.AlertID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual securityAlertRow
	found := false
	for rows.Next() {
		if err := rows.Scan(&actual.OrgID, &actual.RepoID, &actual.AlertID, &actual.Source, &actual.Severity, &actual.State, &actual.PackageName, &actual.CVEID, &actual.URL, &actual.Title, &actual.Description, &actual.CreatedAt, &actual.FixedAt, &actual.DismissedAt, &actual.LastSynced); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareSecurityAlertVersion(expected, actual, found), nil
}

func compareSecurityAlertVersion(expected, actual securityAlertRow, found bool) EffectInspection {
	if !found || actual.LastSynced.IsZero() || actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) {
		return EffectConflict
	}
	if actual.OrgID != expected.OrgID || actual.RepoID != expected.RepoID || actual.AlertID != expected.AlertID || actual.Source != expected.Source || !stringPointersEqual(actual.Severity, expected.Severity) || !stringPointersEqual(actual.State, expected.State) || !stringPointersEqual(actual.PackageName, expected.PackageName) || !stringPointersEqual(actual.CVEID, expected.CVEID) || !stringPointersEqual(actual.URL, expected.URL) || !stringPointersEqual(actual.Title, expected.Title) || !stringPointersEqual(actual.Description, expected.Description) || !actual.CreatedAt.UTC().Equal(expected.CreatedAt.UTC()) || !timePointersEqual(actual.FixedAt, expected.FixedAt) || !timePointersEqual(actual.DismissedAt, expected.DismissedAt) {
		return EffectConflict
	}
	return EffectExact
}

var _ EffectSink = GitHubSecurityClickHouseEffects{}
var _ EffectReadback = GitHubSecurityClickHouseEffects{}
