// Package teamsidentity ports the ClickHouse-native team catalog and
// identity admin CRUD (dev_health_ops.api.admin.routers.teams /
// dev_health_ops.api.admin.routers.identities, and their
// ClickHouseTeamAdminService / ClickHouseIdentityStore backends) into Go.
//
// ClickHouse is the system of record for both `teams` and `identities`
// (CHAOS-2600 CS5): every write here is a ReplacingMergeTree row insert
// with a fresh updated_at (the latest version wins under FINAL), and every
// delete is a ClickHouse lightweight DELETE, never a Postgres write.
//
// This first PR covers only the pure-CRUD routes (list/get/create-or-
// update/delete teams, list/create-or-update identities) -- no external
// provider discovery, no team-drift review. Those are separate, later PRs
// under the same ticket (CHAOS-6251): discovery needs live GitHub/GitLab/
// Jira/Linear API calls through IntegrationCredentialsService, and drift
// review needs two more ClickHouse tables (team_drift_changes,
// team_provider_observations) this package does not touch.
package teamsidentity

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// teamNamespace/identityNamespace mirror Python's uuid.uuid5(NAMESPACE_URL,
// f"team:{org_id}:{team_id}") / f"identity:{org_id}:{canonical_id}" --
// uuid.NAMESPACE_URL is a fixed, well-known UUID (RFC 4122), so this must
// be that exact constant, not a locally-generated one, or the wire `id`
// would silently diverge from what Python assigns for the same team/
// identity today.
var (
	namespaceURL = uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
)

func teamUUID(orgID, teamID string) uuid.UUID {
	return uuid.NewSHA1(namespaceURL, []byte("team:"+orgID+":"+teamID))
}

func identityUUID(orgID, canonicalID string) uuid.UUID {
	return uuid.NewSHA1(namespaceURL, []byte("identity:"+orgID+":"+canonicalID))
}

// Team mirrors ClickHouseTeam's attribute surface the admin response mapper
// reads. Drift-only Postgres fields with no ClickHouse counterpart
// (ExtraData/ManagedFields/SyncPolicy/FlaggedChanges/LastDriftSyncAt) are
// stable defaults, not stored.
type Team struct {
	ID            string // the team_uuid, string form -- the wire "id"
	TeamUUID      uuid.UUID
	TeamID        string // the ClickHouse slug -- the wire "team_id"
	Name          string
	Description   *string
	Members       []string
	ManualMembers []string
	ProjectKeys   []string
	RepoPatterns  []string
	IsActive      bool
	UpdatedAt     time.Time
	OrgID         string
}

// Identity mirrors ClickHouseIdentity's attribute surface.
type Identity struct {
	ID                 string // the identity_uuid, string form -- the wire "id"
	IdentityUUID       uuid.UUID
	CanonicalID        string
	DisplayName        *string
	Email              *string
	ProviderIdentities *pybody.OrderedStringListDict
	TeamIDs            []string
	IsActive           bool
	UpdatedAt          time.Time
	OrgID              string
}

// Store is the ClickHouse-backed team/identity admin store.
type Store struct {
	Conn driver.Conn
}

const teamSelectColumns = "id, team_uuid, name, description, members, project_keys, repo_patterns, is_active, updated_at, org_id, manual_members"

// queryTeams is _query_teams: teamID nil lists every (optionally
// active-only) team; teamID non-nil scopes to one.
func (s Store) queryTeams(ctx context.Context, orgID string, teamID *string, activeOnly bool) ([]Team, error) {
	query := "SELECT " + teamSelectColumns + " FROM teams FINAL WHERE org_id = {org_id:String}"
	args := []any{clickhouse.Named("org_id", orgID)}
	if teamID != nil {
		query += " AND id = {team_id:String}"
		args = append(args, clickhouse.Named("team_id", *teamID))
	}
	if activeOnly {
		query += " AND is_active = 1"
	}
	// ORDER BY: without one, ClickHouse's merge order is whatever the engine
	// happens to return -- not a deterministic contract (CHAOS-6310 r1
	// finding #9). The Python producer's equivalent query
	// (ClickHouseTeamAdminService._query_teams) now carries the matching
	// `ORDER BY id`, so both planes agree, not just each individually
	// deterministic.
	query += " ORDER BY id"
	rows, err := s.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query teams: %w", err)
	}
	defer rows.Close()
	var teams []Team
	for rows.Next() {
		var (
			id, name, orgIDCol                         string
			teamUUIDCol                                uuid.UUID
			description                                *string
			members, projectKeys, repoPatterns, manual []string
			isActive                                   uint8
			updatedAt                                  time.Time
		)
		if err := rows.Scan(&id, &teamUUIDCol, &name, &description, &members, &projectKeys, &repoPatterns, &isActive, &updatedAt, &orgIDCol, &manual); err != nil {
			return nil, fmt.Errorf("scan team row: %w", err)
		}
		teams = append(teams, Team{
			ID: teamUUIDCol.String(), TeamUUID: teamUUIDCol, TeamID: id, Name: name, Description: description,
			Members: members, ManualMembers: manual, ProjectKeys: projectKeys, RepoPatterns: repoPatterns,
			IsActive: isActive != 0, UpdatedAt: updatedAt, OrgID: orgIDCol,
		})
	}
	return teams, rows.Err()
}

// ListTeams is ClickHouseTeamAdminService.list_all.
func (s Store) ListTeams(ctx context.Context, orgID string, activeOnly bool) ([]Team, error) {
	return s.queryTeams(ctx, orgID, nil, activeOnly)
}

// GetTeam is ClickHouseTeamAdminService.get: always active_only=false so a
// caller can tell "missing" from "inactive".
func (s Store) GetTeam(ctx context.Context, orgID, teamID string) (*Team, error) {
	teams, err := s.queryTeams(ctx, orgID, &teamID, false)
	if err != nil {
		return nil, err
	}
	if len(teams) == 0 {
		return nil, nil
	}
	return &teams[0], nil
}

// TeamWrite is create_or_update's parameter set. A nil pointer for
// RepoPatterns/ProjectKeys/Members/ManualMembers means "not provided" --
// _resolve_list_field's None-means-keep-existing-or-empty semantics.
// Description has NO such fallback in Python (used exactly as given, nil
// included) -- it is a plain *string here, always applied as given.
type TeamWrite struct {
	TeamID        string
	Name          string
	Description   *string
	RepoPatterns  *[]string
	ProjectKeys   *[]string
	Members       *[]string
	ManualMembers *[]string
}

func resolveListField(provided *[]string, existing []string) []string {
	if provided != nil {
		return append([]string(nil), *provided...)
	}
	if existing != nil {
		return append([]string(nil), existing...)
	}
	return []string{}
}

// CreateOrUpdateTeam is ClickHouseTeamAdminService.create_or_update.
func (s Store) CreateOrUpdateTeam(ctx context.Context, orgID string, write TeamWrite) (Team, error) {
	existing, err := s.GetTeam(ctx, orgID, write.TeamID)
	if err != nil {
		return Team{}, err
	}
	uuidValue := teamUUID(orgID, write.TeamID)
	if existing != nil {
		uuidValue = existing.TeamUUID
	}
	var existingMembers, existingManual []string
	if existing != nil {
		existingMembers, existingManual = existing.Members, existing.ManualMembers
	}
	resolvedMembers := resolveListField(write.Members, existingMembers)
	resolvedManual := resolveListField(write.ManualMembers, existingManual)
	resolvedProjects := resolveListField(write.ProjectKeys, teamListOrNil(existing, func(t Team) []string { return t.ProjectKeys }))
	resolvedRepos := resolveListField(write.RepoPatterns, teamListOrNil(existing, func(t Team) []string { return t.RepoPatterns }))

	now := time.Now().UTC()
	if err := s.insertTeamRow(ctx, teamInsertRow{
		ID: write.TeamID, TeamUUID: uuidValue, Name: write.Name, Description: write.Description,
		Members: resolvedMembers, ManualMembers: resolvedManual, ProjectKeys: resolvedProjects, RepoPatterns: resolvedRepos,
		IsActive: true, OrgID: orgID, Provider: "", NativeTeamKey: nil, ParentTeamID: nil, UpdatedAt: now,
	}); err != nil {
		return Team{}, err
	}
	return Team{
		ID: uuidValue.String(), TeamUUID: uuidValue, TeamID: write.TeamID, Name: write.Name, Description: write.Description,
		Members: resolvedMembers, ManualMembers: resolvedManual, ProjectKeys: resolvedProjects, RepoPatterns: resolvedRepos,
		IsActive: true, UpdatedAt: now, OrgID: orgID,
	}, nil
}

func teamListOrNil(existing *Team, field func(Team) []string) []string {
	if existing == nil {
		return nil
	}
	return field(*existing)
}

func sortedUnique(values ...[]string) []string {
	set := map[string]struct{}{}
	for _, list := range values {
		for _, v := range list {
			if v != "" {
				set[v] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// SetMembers is ClickHouseTeamAdminService.set_members.
func (s Store) SetMembers(ctx context.Context, orgID, teamID string, members []string, manualMembers *[]string) (*Team, error) {
	existing, err := s.GetTeam(ctx, orgID, teamID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	sortedMembers := sortedUnique(members)
	write := TeamWrite{
		TeamID: teamID, Name: existing.Name, Description: existing.Description,
		RepoPatterns: ptrSlice(existing.RepoPatterns), ProjectKeys: ptrSlice(existing.ProjectKeys),
		Members: &sortedMembers,
	}
	if manualMembers != nil {
		sortedManual := sortedUnique(*manualMembers)
		write.ManualMembers = &sortedManual
	}
	team, err := s.CreateOrUpdateTeam(ctx, orgID, write)
	if err != nil {
		return nil, err
	}
	return &team, nil
}

func ptrSlice(values []string) *[]string {
	copied := append([]string(nil), values...)
	return &copied
}

// AddMembers is ClickHouseTeamAdminService.add_members: unions new members
// into both Members and ManualMembers (CHAOS-4321 -- this method is only
// ever called from a genuine admin action).
func (s Store) AddMembers(ctx context.Context, orgID, teamID string, members []string) (*Team, error) {
	existing, err := s.GetTeam(ctx, orgID, teamID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	merged := sortedUnique(existing.Members, members)
	mergedManual := sortedUnique(existing.ManualMembers, members)
	return s.SetMembers(ctx, orgID, teamID, merged, &mergedManual)
}

// RemoveMembers is ClickHouseTeamAdminService.remove_members: surgically
// drops facets from Members AND ManualMembers, preserving every other
// member (never recomputed from scratch).
func (s Store) RemoveMembers(ctx context.Context, orgID, teamID string, facets map[string]bool) (*Team, error) {
	existing, err := s.GetTeam(ctx, orgID, teamID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	remaining := filterOut(existing.Members, facets)
	remainingManual := filterOut(existing.ManualMembers, facets)
	return s.SetMembers(ctx, orgID, teamID, remaining, &remainingManual)
}

func filterOut(values []string, drop map[string]bool) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if !drop[v] {
			out = append(out, v)
		}
	}
	return out
}

// DeleteTeam is ClickHouseTeamAdminService.delete: a ClickHouse lightweight
// DELETE, never an ALTER TABLE ... DELETE mutation.
func (s Store) DeleteTeam(ctx context.Context, orgID, teamID string) (bool, error) {
	existing, err := s.GetTeam(ctx, orgID, teamID)
	if err != nil {
		return false, err
	}
	if existing == nil {
		return false, nil
	}
	if err := s.Conn.Exec(ctx, "DELETE FROM teams WHERE org_id = {org_id:String} AND id = {team_id:String}",
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_id", teamID)); err != nil {
		return false, fmt.Errorf("delete team: %w", err)
	}
	return true, nil
}

type teamInsertRow struct {
	ID, Name                    string
	TeamUUID                    uuid.UUID
	Description                 *string
	Members, ManualMembers      []string
	ProjectKeys, RepoPatterns   []string
	IsActive                    bool
	OrgID, Provider             string
	NativeTeamKey, ParentTeamID *string
	UpdatedAt                   time.Time
}

// insertTeamRow is storage/clickhouse.py's insert_teams -- one row, the
// exact 16-column list that table's real writer uses (source_id is always
// NULL from this admin surface; no caller here ever knows a customer-push
// source).
func (s Store) insertTeamRow(ctx context.Context, row teamInsertRow) error {
	const insertSQL = "INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, parent_team_id, source_id)"
	batch, err := s.Conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare team insert: %w", err)
	}
	defer batch.Abort()
	isActive := uint8(0)
	if row.IsActive {
		isActive = 1
	}
	now := time.Now().UTC()
	if err := batch.Append(
		row.ID, row.TeamUUID, row.Name, row.Description, row.Members, row.ManualMembers,
		row.ProjectKeys, row.RepoPatterns, isActive, row.UpdatedAt, now, row.OrgID,
		row.Provider, row.NativeTeamKey, row.ParentTeamID, (*uuid.UUID)(nil),
	); err != nil {
		return fmt.Errorf("append team row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send team insert: %w", err)
	}
	return nil
}

const identitySelectColumns = "canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at, org_id"

func (s Store) queryIdentities(ctx context.Context, orgID string, canonicalID *string, activeOnly bool) ([]Identity, error) {
	query := "SELECT " + identitySelectColumns + " FROM identities FINAL WHERE org_id = {org_id:String}"
	args := []any{clickhouse.Named("org_id", orgID)}
	if canonicalID != nil {
		query += " AND canonical_id = {canonical_id:String}"
		args = append(args, clickhouse.Named("canonical_id", *canonicalID))
	}
	if activeOnly {
		query += " AND is_active = 1"
	}
	// ORDER BY: same rationale as queryTeams (CHAOS-6310 r1 finding #9).
	// canonical_id matches this table's own declared MergeTree sort key
	// (org_id, canonical_id), so it costs nothing extra. The Python
	// producer's equivalent query (ClickHouseIdentityStore._query) now
	// carries the matching `ORDER BY canonical_id`.
	query += " ORDER BY canonical_id"
	rows, err := s.Conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query identities: %w", err)
	}
	defer rows.Close()
	var identities []Identity
	for rows.Next() {
		var (
			canonicalIDCol, orgIDCol string
			identityUUIDCol          uuid.UUID
			displayName, email       *string
			providerIdentitiesJSON   string
			teamIDs                  []string
			isActive                 uint8
			updatedAt                time.Time
		)
		if err := rows.Scan(&canonicalIDCol, &identityUUIDCol, &displayName, &email, &providerIdentitiesJSON, &teamIDs, &isActive, &updatedAt, &orgIDCol); err != nil {
			return nil, fmt.Errorf("scan identity row: %w", err)
		}
		identities = append(identities, Identity{
			ID: identityUUIDCol.String(), IdentityUUID: identityUUIDCol, CanonicalID: canonicalIDCol,
			DisplayName: displayName, Email: email, ProviderIdentities: decodeProviderIdentities(providerIdentitiesJSON),
			TeamIDs: teamIDs, IsActive: isActive != 0, UpdatedAt: updatedAt, OrgID: orgIDCol,
		})
	}
	return identities, rows.Err()
}

// decodeProviderIdentities mirrors _decode_provider_identities: malformed
// or non-object JSON is a silent {}, never a read failure. Parses with
// pyjson (not encoding/json into a map[string]any, which is unordered) so
// the persisted text's own key order survives the round trip -- r2,
// CHAOS-6310 finding #3: a response GET-ing back what an earlier write
// stored must reproduce that write's provider order, not an alphabetized
// or randomized one.
func decodeProviderIdentities(raw string) *pybody.OrderedStringListDict {
	out := pybody.NewOrderedStringListDict()
	if raw == "" {
		return out
	}
	decoded, err := pyjson.DecodeString(raw)
	if err != nil {
		return out
	}
	object, isObject := decoded.(*pyjson.Object)
	if !isObject {
		return out
	}
	for _, key := range object.Keys() {
		value, _ := object.Get(key)
		list, isList := value.([]pyjson.Value)
		if !isList {
			continue
		}
		strs := make([]string, 0, len(list))
		for _, item := range list {
			if item != nil {
				strs = append(strs, fmt.Sprintf("%v", item))
			}
		}
		out.Set(key, strs)
	}
	return out
}

// ListIdentities is ClickHouseIdentityStore.list_all.
func (s Store) ListIdentities(ctx context.Context, orgID string, activeOnly bool) ([]Identity, error) {
	return s.queryIdentities(ctx, orgID, nil, activeOnly)
}

// GetIdentity is ClickHouseIdentityStore.get.
func (s Store) GetIdentity(ctx context.Context, orgID, canonicalID string) (*Identity, error) {
	identities, err := s.queryIdentities(ctx, orgID, &canonicalID, false)
	if err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return nil, nil
	}
	return &identities[0], nil
}

// FindIdentityByProviderIdentity is ClickHouseIdentityStore.find_by_provider_identity:
// a full-scan match, mirroring the Python service's own O(n) scan.
func (s Store) FindIdentityByProviderIdentity(ctx context.Context, orgID, provider, identityValue string) (*Identity, error) {
	identities, err := s.ListIdentities(ctx, orgID, false)
	if err != nil {
		return nil, err
	}
	for i := range identities {
		candidates, _ := identities[i].ProviderIdentities.Get(provider)
		for _, candidate := range candidates {
			if candidate == identityValue {
				return &identities[i], nil
			}
		}
	}
	return nil, nil
}

// IdentityWrite is create_or_update's parameter set: nil means "not
// provided", replacement semantics on provide (ProviderIdentities/TeamIDs
// replace wholesale, never merge).
type IdentityWrite struct {
	CanonicalID        string
	DisplayName        *string
	Email              *string
	ProviderIdentities *pybody.OrderedStringListDict
	TeamIDs            *[]string
}

// CreateOrUpdateIdentity is ClickHouseIdentityStore.create_or_update.
func (s Store) CreateOrUpdateIdentity(ctx context.Context, orgID string, write IdentityWrite) (Identity, error) {
	existing, err := s.GetIdentity(ctx, orgID, write.CanonicalID)
	if err != nil {
		return Identity{}, err
	}
	uuidValue := identityUUID(orgID, write.CanonicalID)
	if existing != nil {
		uuidValue = existing.IdentityUUID
	}
	resolvedDisplay := write.DisplayName
	if resolvedDisplay == nil && existing != nil {
		resolvedDisplay = existing.DisplayName
	}
	resolvedEmail := write.Email
	if resolvedEmail == nil && existing != nil {
		resolvedEmail = existing.Email
	}
	resolvedProviders := pybody.NewOrderedStringListDict()
	if write.ProviderIdentities != nil {
		resolvedProviders = write.ProviderIdentities
	} else if existing != nil && existing.ProviderIdentities != nil {
		resolvedProviders = existing.ProviderIdentities
	}
	var resolvedTeamIDs []string
	if write.TeamIDs != nil {
		resolvedTeamIDs = append([]string(nil), (*write.TeamIDs)...)
	} else if existing != nil {
		resolvedTeamIDs = append([]string(nil), existing.TeamIDs...)
	}
	if resolvedTeamIDs == nil {
		resolvedTeamIDs = []string{}
	}

	now := time.Now().UTC()
	providersJSON, err := pythonProviderIdentitiesJSON(resolvedProviders)
	if err != nil {
		return Identity{}, fmt.Errorf("encode provider_identities: %w", err)
	}
	if err := s.insertIdentityRow(ctx, identityInsertRow{
		OrgID: orgID, CanonicalID: write.CanonicalID, IdentityUUID: uuidValue,
		DisplayName: resolvedDisplay, Email: resolvedEmail, ProviderIdentitiesJSON: providersJSON,
		TeamIDs: resolvedTeamIDs, IsActive: true, UpdatedAt: now,
	}); err != nil {
		return Identity{}, err
	}
	return Identity{
		ID: uuidValue.String(), IdentityUUID: uuidValue, CanonicalID: write.CanonicalID,
		DisplayName: resolvedDisplay, Email: resolvedEmail, ProviderIdentities: resolvedProviders,
		TeamIDs: resolvedTeamIDs, IsActive: true, UpdatedAt: now, OrgID: orgID,
	}, nil
}

// DeleteIdentity is ClickHouseIdentityStore.delete: a ClickHouse lightweight
// DELETE.
func (s Store) DeleteIdentity(ctx context.Context, orgID, canonicalID string) (bool, error) {
	existing, err := s.GetIdentity(ctx, orgID, canonicalID)
	if err != nil {
		return false, err
	}
	if existing == nil {
		return false, nil
	}
	if err := s.Conn.Exec(ctx, "DELETE FROM identities WHERE org_id = {org_id:String} AND canonical_id = {canonical_id:String}",
		clickhouse.Named("org_id", orgID), clickhouse.Named("canonical_id", canonicalID)); err != nil {
		return false, fmt.Errorf("delete identity: %w", err)
	}
	return true, nil
}

type identityInsertRow struct {
	OrgID, CanonicalID     string
	IdentityUUID           uuid.UUID
	DisplayName, Email     *string
	ProviderIdentitiesJSON string
	TeamIDs                []string
	IsActive               bool
	UpdatedAt              time.Time
}

// pythonProviderIdentitiesJSON renders d the way `storage/clickhouse.py`'s
// bare `json.dumps(m)` call (no keyword arguments at all) renders a
// `dict[str, list[str]]` -- pyjson.Marshal's compact `{"a":["b"]}`
// (Starlette's own response separators) differs from that in both spacing
// AND non-ASCII escaping, but the ClickHouse `provider_identities` column
// is a plain String, so a row-for-row comparison against the Python
// writer's stored bytes sees both differences. pyjson.Dumps is the shared
// encoder for exactly this bare-`json.dumps()` shape (internal/api/pyjson,
// already used by internal/api/telemetry and internal/api/producttelemetry
// for the same reason), not a route-local one, so every ClickHouse
// JSON-column writer shares one implementation. Keys are written in d's OWN
// order (r2, CHAOS-6310 finding #3, fixed): sorting here (this package's
// prior behavior, matching encoding/json.Marshal's map-key sort) was a
// live, reproducible mismatch against Python's dict, which preserves the
// original request's insertion order -- d is an OrderedStringListDict for
// exactly this reason, carried from the request all the way to this write.
func pythonProviderIdentitiesJSON(d *pybody.OrderedStringListDict) (string, error) {
	object := pyjson.NewObject()
	if d != nil {
		for _, key := range d.Keys {
			values, _ := d.Get(key)
			object.Set(key, values)
		}
	}
	return pyjson.Dumps(object)
}

// insertIdentityRow is storage/clickhouse.py's insert_identities -- one row,
// the exact 10-column list that table's real writer uses.
func (s Store) insertIdentityRow(ctx context.Context, row identityInsertRow) error {
	const insertSQL = "INSERT INTO identities (org_id, canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at, source_id)"
	batch, err := s.Conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("prepare identity insert: %w", err)
	}
	defer batch.Abort()
	isActive := uint8(0)
	if row.IsActive {
		isActive = 1
	}
	if err := batch.Append(
		row.OrgID, row.CanonicalID, row.IdentityUUID, row.DisplayName, row.Email,
		row.ProviderIdentitiesJSON, row.TeamIDs, isActive, row.UpdatedAt, (*uuid.UUID)(nil),
	); err != nil {
		return fmt.Errorf("append identity row: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send identity insert: %w", err)
	}
	return nil
}

// ErrTeamNotFound / ErrIdentityNotFound are sentinel errors the HTTP layer
// maps to 404, matching FastAPI's HTTPException(404).
var (
	ErrTeamNotFound     = errors.New("team not found")
	ErrIdentityNotFound = errors.New("identity not found")
)
