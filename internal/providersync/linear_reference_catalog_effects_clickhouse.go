package providersync

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// LinearReferenceCatalogClickHouseEffects is the concrete bridge from the
// provider-owned typed rows to the five existing reference dimensions. It
// deliberately has no JSON metadata fallback: a destination only accepts its
// own row type and all rows carry the leased organization id.
type LinearReferenceCatalogClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

const linearReferenceTeamsInsert = `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id)`
const linearReferenceMembersInsert = `INSERT INTO members (org_id, member_id, name, email, provider_identities, is_active, updated_at)`
const linearReferenceMembershipsInsert = `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
const linearReferenceOwnershipInsert = `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`
const linearReferenceProjectsInsert = `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced)`

func (sink LinearReferenceCatalogClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	switch effect.Destination {
	case linearReferenceCatalogTeamsDestination:
		rows, err := decodeEffectRows[linearReferenceTeamRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateLinearReferenceTeamRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeTeams(ctx, rows)
	case linearReferenceCatalogMembersDestination:
		rows, err := decodeEffectRows[linearReferenceMemberRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateLinearReferenceMemberRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeMembers(ctx, rows)
	case linearReferenceCatalogMembershipsDestination:
		rows, err := decodeEffectRows[linearReferenceMembershipRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateLinearReferenceMembershipRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeMemberships(ctx, rows)
	case linearReferenceCatalogProjectsDestination:
		rows, err := decodeEffectRows[linearReferenceProjectRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := row.validate(claim); err != nil {
				return err
			}
		}
		return sink.writeProjects(ctx, rows)
	case linearReferenceCatalogOwnershipDestination:
		rows, err := decodeEffectRows[linearReferenceOwnershipRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateLinearReferenceOwnershipRow(claim, row); err != nil {
				return err
			}
		}
		return sink.writeOwnership(ctx, rows)
	case linearReferenceCatalogSprintsDestination:
		rows, err := decodeEffectRows[linearSprintRow](effect)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := validateLinearSprint(row, claim); err != nil {
				return err
			}
		}
		return sink.writeSprints(ctx, rows)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink LinearReferenceCatalogClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if err := sink.validateRequest(ctx, claim, effect); err != nil {
		return EffectConflict, err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	switch effect.Destination {
	case linearReferenceCatalogTeamsDestination:
		expected, err := decodeEffectRows[linearReferenceTeamRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		for _, row := range expected {
			if err := validateLinearReferenceTeamRow(claim, row); err != nil {
				return EffectConflict, err
			}
		}
		return sink.inspectTeams(ctx, claim, expected)
	case linearReferenceCatalogMembersDestination:
		expected, err := decodeEffectRows[linearReferenceMemberRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		for _, row := range expected {
			if err := validateLinearReferenceMemberRow(claim, row); err != nil {
				return EffectConflict, err
			}
		}
		return sink.inspectMembers(ctx, claim, expected)
	case linearReferenceCatalogMembershipsDestination:
		expected, err := decodeEffectRows[linearReferenceMembershipRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		for _, row := range expected {
			if err := validateLinearReferenceMembershipRow(claim, row); err != nil {
				return EffectConflict, err
			}
		}
		return sink.inspectMemberships(ctx, claim, expected)
	case linearReferenceCatalogProjectsDestination:
		expected, err := decodeEffectRows[linearReferenceProjectRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		for _, row := range expected {
			if err := row.validate(claim); err != nil {
				return EffectConflict, err
			}
		}
		return sink.inspectProjects(ctx, claim, expected)
	case linearReferenceCatalogOwnershipDestination:
		expected, err := decodeEffectRows[linearReferenceOwnershipRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		for _, row := range expected {
			if err := validateLinearReferenceOwnershipRow(claim, row); err != nil {
				return EffectConflict, err
			}
		}
		return sink.inspectOwnership(ctx, claim, expected)
	case linearReferenceCatalogSprintsDestination:
		expected, err := decodeEffectRows[linearSprintRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		for _, row := range expected {
			if err := validateLinearSprint(row, claim); err != nil {
				return EffectConflict, err
			}
		}
		return sink.inspectSprints(ctx, claim, expected)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

// validateRequest deliberately does NOT call claim.Validate(): CHAOS-4431
// (team-lead ruling, 2026-08-28, option (c)) made the caller claim-free --
// this sink is now written to from a once-per-sync-run reference-catalog
// walk with no lease or claimed provider-unit behind it, not from inside the
// work-items route's Collect(). claim.Validate() requires a live lease and
// claim.Dataset=="work-items" belonged to that retired call shape; the only
// properties this write path still needs are "this really is Linear" and
// "this really is this org" (every row below also re-checks OrgID itself).
func (sink LinearReferenceCatalogClickHouseEffects) validateRequest(ctx context.Context, claim Claim, effect EffectBatch) error {
	if ctx == nil || sink.Lease == nil || sink.Conn == nil ||
		claim.Provider != "linear" || strings.TrimSpace(claim.OrgID) == "" ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) ||
		effect.PayloadBytes < 0 || !linearReferenceCatalogDestination(effect.Destination) {
		return ErrInvalidConfiguration
	}
	return nil
}

func linearReferenceCatalogDestination(destination string) bool {
	switch destination {
	case linearReferenceCatalogTeamsDestination, linearReferenceCatalogMembersDestination,
		linearReferenceCatalogMembershipsDestination, linearReferenceCatalogProjectsDestination,
		linearReferenceCatalogOwnershipDestination, linearReferenceCatalogSprintsDestination:
		return true
	default:
		return false
	}
}

func validateLinearReferenceTeamRow(claim Claim, row linearReferenceTeamRow) error {
	if claim.Provider != "linear" || row.Provider != "linear" || row.OrgID != claim.OrgID ||
		strings.TrimSpace(row.ID) == "" || strings.TrimSpace(row.TeamUUID) == "" || row.UpdatedAt.IsZero() ||
		!containsString(row.ProjectKeys, row.ID) || row.IsActive > 1 {
		return ErrInvalidConfiguration
	}
	if _, err := uuid.Parse(row.TeamUUID); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateLinearReferenceMemberRow(claim Claim, row linearReferenceMemberRow) error {
	if claim.Provider != "linear" || row.OrgID != claim.OrgID || strings.TrimSpace(row.MemberID) == "" ||
		!strings.HasPrefix(row.MemberID, "linear:") || strings.TrimSpace(row.ProviderIdentities) == "" ||
		row.UpdatedAt.IsZero() || row.IsActive > 1 {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateLinearReferenceMembershipRow(claim Claim, row linearReferenceMembershipRow) error {
	if claim.Provider != "linear" || row.Provider != "linear" || row.OrgID != claim.OrgID ||
		strings.TrimSpace(row.TeamID) == "" || strings.TrimSpace(row.MemberID) == "" ||
		row.Source != "native" || row.IsPrimary != 1 || row.Specificity != 100 || row.Priority != 10 ||
		row.ValidFrom.IsZero() || row.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateLinearReferenceOwnershipRow(claim Claim, row linearReferenceOwnershipRow) error {
	if claim.Provider != "linear" || row.Provider != "linear" || row.OrgID != claim.OrgID ||
		strings.TrimSpace(row.TeamID) == "" || strings.TrimSpace(row.ProjectID) == "" ||
		row.Source != "native" || row.IsPrimary != 1 || row.Specificity != 100 || row.Priority != 10 ||
		row.ValidFrom.IsZero() || row.UpdatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func (sink LinearReferenceCatalogClickHouseEffects) writeTeams(ctx context.Context, rows []linearReferenceTeamRow) error {
	if len(rows) == 0 {
		return nil
	}
	// CHAOS-4446: read every touched team's CURRENT manual_members before
	// preparing the insert, so this write carries it forward instead of
	// clobbering an admin's roster override with the schema default ([]).
	// A team with no existing row (genuinely new) defaults to [] below,
	// which is correct -- PreserveExistingTeamManualMembers's own doc
	// comment covers why a missing map entry is not an error.
	teamIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		teamIDs = append(teamIDs, row.ID)
	}
	existingManualMembers, err := PreserveExistingTeamManualMembers(ctx, sink.Conn, rows[0].OrgID, teamIDs)
	if err != nil {
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, linearReferenceTeamsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		teamUUID, err := uuid.Parse(row.TeamUUID)
		if err != nil {
			return ErrInvalidConfiguration
		}
		manualMembers := existingManualMembers[row.ID]
		if err := batch.Append(row.ID, teamUUID, row.Name, row.Description, row.Members, manualMembers, row.ProjectKeys, row.RepoPatterns, row.IsActive, row.UpdatedAt, row.OrgID, row.Provider, row.NativeTeamKey, row.ParentTeamID); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LinearReferenceCatalogClickHouseEffects) writeMembers(ctx context.Context, rows []linearReferenceMemberRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, linearReferenceMembersInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.MemberID, row.Name, row.Email, row.ProviderIdentities, row.IsActive, row.UpdatedAt); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LinearReferenceCatalogClickHouseEffects) writeMemberships(ctx context.Context, rows []linearReferenceMembershipRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, linearReferenceMembershipsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.Provider, row.TeamID, row.MemberID, row.RawProviderUserID, row.RawEmail, row.IdentityFacets, row.Source, row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LinearReferenceCatalogClickHouseEffects) writeProjects(ctx context.Context, rows []linearReferenceProjectRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, linearReferenceProjectsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.ID, row.OrgID, row.Provider, row.ProjectKey, row.Name, row.IsActive, row.State, linearReferenceTargetDate(row.TargetDate), row.URL, row.TeamIDs, row.TeamKeys, row.LeadID, row.LeadName, row.LeadEmail, row.UpdatedAt, row.LastSynced); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LinearReferenceCatalogClickHouseEffects) writeOwnership(ctx context.Context, rows []linearReferenceOwnershipRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, linearReferenceOwnershipInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.OrgID, row.Provider, row.TeamID, row.ProjectID, row.ProjectKey, row.Source, row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

// writeSprints shares the `sprints` table and its INSERT column list with
// GitHubSprintsClickHouseAdapter (gitHubSprintsInsert,
// github_work_items_direct_effects_clickhouse.go) rather than duplicating
// them: linearSprintRow is a type alias for githubSprintRow, so the same
// columns and row shape apply unchanged. A dedicated method (not a call
// through that adapter's own WriteGitHubWorkItemEffect) because this sink's
// WriteEffect takes a (Claim, EffectBatch) pair, not that adapter's
// (GitHubWorkItemEffectIdentity, EffectBatch) shape.
func (sink LinearReferenceCatalogClickHouseEffects) writeSprints(ctx context.Context, rows []linearSprintRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, gitHubSprintsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.Provider, row.SprintID, row.NativeTeamKey, row.Name, row.State, row.StartedAt, row.EndedAt, row.CompletedAt, row.LastSynced, row.OrgID); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink LinearReferenceCatalogClickHouseEffects) inspectSprints(ctx context.Context, claim Claim, expected []linearSprintRow) (EffectInspection, error) {
	return inspectLinearReferenceRows(expected, func(row linearSprintRow) (EffectInspection, error) {
		result, err := sink.Conn.Query(ctx, gitHubSprintsSelect, claim.OrgID, "linear", row.SprintID)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual linearSprintRow
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.Provider, &actual.SprintID, &actual.NativeTeamKey, &actual.Name, &actual.State, &actual.StartedAt, &actual.EndedAt, &actual.CompletedAt, &actual.LastSynced, &actual.OrgID); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if found == 0 {
			return EffectAbsent, nil
		}
		if found != 1 || !equalLinearReferenceSprint(row, actual) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func equalLinearReferenceSprint(expected, actual linearSprintRow) bool {
	return expected.Provider == actual.Provider && expected.SprintID == actual.SprintID &&
		reflect.DeepEqual(expected.NativeTeamKey, actual.NativeTeamKey) &&
		reflect.DeepEqual(expected.Name, actual.Name) &&
		reflect.DeepEqual(expected.State, actual.State) &&
		reflect.DeepEqual(expected.StartedAt, actual.StartedAt) &&
		reflect.DeepEqual(expected.EndedAt, actual.EndedAt) &&
		reflect.DeepEqual(expected.CompletedAt, actual.CompletedAt) &&
		expected.LastSynced.Equal(actual.LastSynced) && expected.OrgID == actual.OrgID
}

func (sink LinearReferenceCatalogClickHouseEffects) inspectTeams(ctx context.Context, claim Claim, expected []linearReferenceTeamRow) (EffectInspection, error) {
	return inspectLinearReferenceRows(expected, func(row linearReferenceTeamRow) (EffectInspection, error) {
		result, err := sink.Conn.Query(ctx, `SELECT id, team_uuid, name, description, members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id FROM teams FINAL WHERE org_id = ? AND provider = ? AND id = ?`, claim.OrgID, "linear", row.ID)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual linearReferenceTeamRow
		var teamUUID uuid.UUID
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.ID, &teamUUID, &actual.Name, &actual.Description, &actual.Members, &actual.ProjectKeys, &actual.RepoPatterns, &actual.IsActive, &actual.UpdatedAt, &actual.OrgID, &actual.Provider, &actual.NativeTeamKey, &actual.ParentTeamID); err != nil {
				return EffectConflict, err
			}
			actual.TeamUUID = teamUUID.String()
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if found == 0 {
			return EffectAbsent, nil
		}
		if found != 1 || !equalLinearReferenceTeam(row, actual) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func (sink LinearReferenceCatalogClickHouseEffects) inspectMembers(ctx context.Context, claim Claim, expected []linearReferenceMemberRow) (EffectInspection, error) {
	return inspectLinearReferenceRows(expected, func(row linearReferenceMemberRow) (EffectInspection, error) {
		result, err := sink.Conn.Query(ctx, `SELECT org_id, member_id, name, email, provider_identities, is_active, updated_at FROM members FINAL WHERE org_id = ? AND member_id = ?`, claim.OrgID, row.MemberID)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual linearReferenceMemberRow
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.OrgID, &actual.MemberID, &actual.Name, &actual.Email, &actual.ProviderIdentities, &actual.IsActive, &actual.UpdatedAt); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if found == 0 {
			return EffectAbsent, nil
		}
		if found != 1 || !equalLinearReferenceMember(row, actual) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func (sink LinearReferenceCatalogClickHouseEffects) inspectMemberships(ctx context.Context, claim Claim, expected []linearReferenceMembershipRow) (EffectInspection, error) {
	return inspectLinearReferenceRows(expected, func(row linearReferenceMembershipRow) (EffectInspection, error) {
		result, err := sink.Conn.Query(ctx, `SELECT org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND member_id = ? AND source = ?`, claim.OrgID, "linear", row.TeamID, row.MemberID, "native")
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual linearReferenceMembershipRow
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.OrgID, &actual.Provider, &actual.TeamID, &actual.MemberID, &actual.RawProviderUserID, &actual.RawEmail, &actual.IdentityFacets, &actual.Source, &actual.IsPrimary, &actual.Specificity, &actual.Priority, &actual.ValidFrom, &actual.ValidTo, &actual.UpdatedAt); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if found == 0 {
			return EffectAbsent, nil
		}
		if found != 1 || !equalLinearReferenceMembership(row, actual) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func (sink LinearReferenceCatalogClickHouseEffects) inspectProjects(ctx context.Context, claim Claim, expected []linearReferenceProjectRow) (EffectInspection, error) {
	return inspectLinearReferenceRows(expected, func(row linearReferenceProjectRow) (EffectInspection, error) {
		result, err := sink.Conn.Query(ctx, `SELECT id, org_id, provider, project_key, name, is_active, state, target_date, url, team_ids, team_keys, lead_id, lead_name, lead_email, updated_at, last_synced FROM projects FINAL WHERE org_id = ? AND provider = ? AND id = ?`, claim.OrgID, "linear", row.ID)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual linearReferenceProjectRow
		var storedTargetDate *time.Time
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.ID, &actual.OrgID, &actual.Provider, &actual.ProjectKey, &actual.Name, &actual.IsActive, &actual.State, &storedTargetDate, &actual.URL, &actual.TeamIDs, &actual.TeamKeys, &actual.LeadID, &actual.LeadName, &actual.LeadEmail, &actual.UpdatedAt, &actual.LastSynced); err != nil {
				return EffectConflict, err
			}
			if storedTargetDate != nil {
				value := linearReferenceDate(storedTargetDate.UTC().Format("2006-01-02"))
				actual.TargetDate = &value
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if found == 0 {
			return EffectAbsent, nil
		}
		if found != 1 || !equalLinearReferenceProject(row, actual) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func (sink LinearReferenceCatalogClickHouseEffects) inspectOwnership(ctx context.Context, claim Claim, expected []linearReferenceOwnershipRow) (EffectInspection, error) {
	return inspectLinearReferenceRows(expected, func(row linearReferenceOwnershipRow) (EffectInspection, error) {
		result, err := sink.Conn.Query(ctx, `SELECT org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at FROM team_project_ownership FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND project_id = ? AND source = ?`, claim.OrgID, "linear", row.TeamID, row.ProjectID, "native")
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual linearReferenceOwnershipRow
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.OrgID, &actual.Provider, &actual.TeamID, &actual.ProjectID, &actual.ProjectKey, &actual.Source, &actual.IsPrimary, &actual.Specificity, &actual.Priority, &actual.ValidFrom, &actual.ValidTo, &actual.UpdatedAt); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if found == 0 {
			return EffectAbsent, nil
		}
		if found != 1 || !equalLinearReferenceOwnership(row, actual) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func inspectLinearReferenceRows[T any](rows []T, inspect func(T) (EffectInspection, error)) (EffectInspection, error) {
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range rows {
		inspection, err := inspect(row)
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
	if exact == len(rows) {
		return EffectExact, nil
	}
	if absent == len(rows) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func equalLinearReferenceTeam(left, right linearReferenceTeamRow) bool {
	return left.ID == right.ID && left.TeamUUID == right.TeamUUID && left.Name == right.Name &&
		reflect.DeepEqual(left.Description, right.Description) && reflect.DeepEqual(left.Members, right.Members) &&
		reflect.DeepEqual(left.ProjectKeys, right.ProjectKeys) && reflect.DeepEqual(left.RepoPatterns, right.RepoPatterns) &&
		left.IsActive == right.IsActive && left.OrgID == right.OrgID && left.Provider == right.Provider &&
		reflect.DeepEqual(left.NativeTeamKey, right.NativeTeamKey) && reflect.DeepEqual(left.ParentTeamID, right.ParentTeamID) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalLinearReferenceMember(left, right linearReferenceMemberRow) bool {
	return left.OrgID == right.OrgID && left.MemberID == right.MemberID && left.Name == right.Name &&
		reflect.DeepEqual(left.Email, right.Email) && left.ProviderIdentities == right.ProviderIdentities &&
		left.IsActive == right.IsActive && left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalLinearReferenceMembership(left, right linearReferenceMembershipRow) bool {
	return left.OrgID == right.OrgID && left.Provider == right.Provider && left.TeamID == right.TeamID &&
		left.MemberID == right.MemberID && reflect.DeepEqual(left.RawProviderUserID, right.RawProviderUserID) &&
		reflect.DeepEqual(left.RawEmail, right.RawEmail) && reflect.DeepEqual(left.IdentityFacets, right.IdentityFacets) &&
		left.Source == right.Source && left.IsPrimary == right.IsPrimary && left.Specificity == right.Specificity &&
		left.Priority == right.Priority && left.ValidFrom.Equal(right.ValidFrom) &&
		reflect.DeepEqual(left.ValidTo, right.ValidTo) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func equalLinearReferenceProject(left, right linearReferenceProjectRow) bool {
	return left.ID == right.ID && left.OrgID == right.OrgID && left.Provider == right.Provider &&
		reflect.DeepEqual(left.ProjectKey, right.ProjectKey) && left.Name == right.Name && left.IsActive == right.IsActive &&
		left.State == right.State && linearReferenceDateEqual(left.TargetDate, right.TargetDate) && left.URL == right.URL &&
		reflect.DeepEqual(left.TeamIDs, right.TeamIDs) && reflect.DeepEqual(left.TeamKeys, right.TeamKeys) &&
		reflect.DeepEqual(left.LeadID, right.LeadID) && reflect.DeepEqual(left.LeadName, right.LeadName) &&
		reflect.DeepEqual(left.LeadEmail, right.LeadEmail) && left.UpdatedAt.Equal(right.UpdatedAt) &&
		left.LastSynced.Equal(right.LastSynced)
}

func equalLinearReferenceOwnership(left, right linearReferenceOwnershipRow) bool {
	return left.OrgID == right.OrgID && left.Provider == right.Provider && left.TeamID == right.TeamID &&
		left.ProjectID == right.ProjectID && reflect.DeepEqual(left.ProjectKey, right.ProjectKey) && left.Source == right.Source &&
		left.IsPrimary == right.IsPrimary && left.Specificity == right.Specificity && left.Priority == right.Priority &&
		left.ValidFrom.Equal(right.ValidFrom) && reflect.DeepEqual(left.ValidTo, right.ValidTo) && left.UpdatedAt.Equal(right.UpdatedAt)
}

func linearReferenceTargetDate(value *linearReferenceDate) *time.Time {
	if value == nil {
		return nil
	}
	parsed, err := time.Parse("2006-01-02", string(*value))
	if err != nil {
		return nil
	}
	result := parsed.UTC()
	return &result
}

func linearReferenceDateEqual(left, right *linearReferenceDate) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return string(*left) == string(*right)
}

var _ EffectSink = LinearReferenceCatalogClickHouseEffects{}
var _ EffectReadback = LinearReferenceCatalogClickHouseEffects{}
