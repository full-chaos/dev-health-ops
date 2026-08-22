package providersync

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// The reference catalog is an auxiliary projection of the Linear work-items
// unit.  It intentionally has no capability or route-registration entry of
// its own: the cutover lane that owns the work-items unit decides when this
// provider-only collector is constructed.  The types below are concrete at
// every provider boundary; EffectBatch is only the already-established
// durable effect-ledger representation used after these rows are validated.

type linearReferenceIdentityPayload struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type linearReferenceProjectStatusPayload struct {
	Type string `json:"type"`
}

type linearReferenceProjectTeamPayload struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type linearReferenceProjectTeamsPayload struct {
	Nodes []linearReferenceProjectTeamPayload `json:"nodes"`
}

type linearReferenceProjectPayload struct {
	ID          string                              `json:"id"`
	Name        string                              `json:"name"`
	Description string                              `json:"description"`
	Status      linearReferenceProjectStatusPayload `json:"status"`
	Trashed     bool                                `json:"trashed"`
	TargetDate  string                              `json:"targetDate"`
	ArchivedAt  *string                             `json:"archivedAt"`
	URL         string                              `json:"url"`
	Lead        *linearReferenceIdentityPayload     `json:"lead"`
	Teams       linearReferenceProjectTeamsPayload  `json:"teams"`
}

type linearReferenceCatalogTeamMembersPayload struct {
	Nodes    []linearReferenceCatalogMemberPayload `json:"nodes"`
	PageInfo linearPageInfoPayload                 `json:"pageInfo"`
}

type linearReferenceCatalogTeamPayload struct {
	ID          string                                   `json:"id"`
	Key         string                                   `json:"key"`
	Name        string                                   `json:"name"`
	Description *string                                  `json:"description"`
	Members     linearReferenceCatalogTeamMembersPayload `json:"members"`
}

type linearReferenceCatalogMemberPayload struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Active *bool  `json:"active"`
}

type linearReferenceCatalogTeamPagePayload struct {
	Nodes    []linearReferenceCatalogTeamPayload `json:"nodes"`
	PageInfo linearPageInfoPayload               `json:"pageInfo"`
}

type linearReferenceCatalogMemberPagePayload struct {
	Team struct {
		Members linearReferenceCatalogTeamMembersPayload `json:"members"`
	} `json:"team"`
}

// These rows are the normalized, exact inputs to the five ClickHouse sinks.
// No row trusts a provider-supplied org_id: the collector stamps OrgID from
// the leased Claim and every sink rechecks the same invariant before writing.
type linearReferenceTeamRow struct {
	ID            string    `json:"id"`
	TeamUUID      string    `json:"team_uuid"`
	Name          string    `json:"name"`
	Description   *string   `json:"description"`
	Members       []string  `json:"members"`
	ProjectKeys   []string  `json:"project_keys"`
	RepoPatterns  []string  `json:"repo_patterns"`
	IsActive      uint8     `json:"is_active"`
	UpdatedAt     time.Time `json:"updated_at"`
	OrgID         string    `json:"org_id"`
	Provider      string    `json:"provider"`
	NativeTeamKey *string   `json:"native_team_key"`
	ParentTeamID  *string   `json:"parent_team_id"`
}

type linearReferenceMemberRow struct {
	OrgID              string    `json:"org_id"`
	MemberID           string    `json:"member_id"`
	Name               string    `json:"name"`
	Email              *string   `json:"email"`
	ProviderIdentities string    `json:"provider_identities"`
	IsActive           uint8     `json:"is_active"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type linearReferenceMembershipRow struct {
	OrgID             string     `json:"org_id"`
	Provider          string     `json:"provider"`
	TeamID            string     `json:"team_id"`
	MemberID          string     `json:"member_id"`
	RawProviderUserID *string    `json:"raw_provider_user_id"`
	RawEmail          *string    `json:"raw_email"`
	IdentityFacets    []string   `json:"identity_facets"`
	Source            string     `json:"source"`
	IsPrimary         uint8      `json:"is_primary"`
	Specificity       uint16     `json:"specificity"`
	Priority          int32      `json:"priority"`
	ValidFrom         time.Time  `json:"valid_from"`
	ValidTo           *time.Time `json:"valid_to"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type linearReferenceOwnershipRow struct {
	OrgID       string     `json:"org_id"`
	Provider    string     `json:"provider"`
	TeamID      string     `json:"team_id"`
	ProjectID   string     `json:"project_id"`
	ProjectKey  *string    `json:"project_key"`
	Source      string     `json:"source"`
	IsPrimary   uint8      `json:"is_primary"`
	Specificity uint16     `json:"specificity"`
	Priority    int32      `json:"priority"`
	ValidFrom   time.Time  `json:"valid_from"`
	ValidTo     *time.Time `json:"valid_to"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// linearReferenceProjectRow includes the existing projects columns and the
// native Linear enrichment selected by PROJECTS_QUERY. The enrichment is
// intentionally typed instead of being hidden in a JSON metadata blob.
type linearReferenceProjectRow struct {
	ID         string               `json:"id"`
	OrgID      string               `json:"org_id"`
	Provider   string               `json:"provider"`
	ProjectKey *string              `json:"project_key"`
	Name       string               `json:"name"`
	IsActive   uint8                `json:"is_active"`
	State      string               `json:"state"`
	TargetDate *linearReferenceDate `json:"target_date"`
	URL        string               `json:"url"`
	TeamIDs    []string             `json:"team_ids"`
	TeamKeys   []string             `json:"team_keys"`
	LeadID     *string              `json:"lead_id"`
	LeadName   *string              `json:"lead_name"`
	LeadEmail  *string              `json:"lead_email"`
	UpdatedAt  time.Time            `json:"updated_at"`
	LastSynced time.Time            `json:"last_synced"`
}

// linearReferenceDate preserves Python's datetime.date contract while still
// giving the ClickHouse adapter a single canonical YYYY-MM-DD value.
type linearReferenceDate string

func (value linearReferenceDate) OracleDate() string { return string(value) }

func (value linearReferenceDate) Format(layout string) string {
	if layout == "2006-01-02" {
		return string(value)
	}
	return string(value)
}

type LinearReferenceCatalogRows struct {
	Teams       []linearReferenceTeamRow       `json:"teams"`
	Members     []linearReferenceMemberRow     `json:"members"`
	Memberships []linearReferenceMembershipRow `json:"memberships"`
	Projects    []linearReferenceProjectRow    `json:"projects"`
	Ownership   []linearReferenceOwnershipRow  `json:"ownership"`
}

const (
	linearReferenceCatalogDestinationCount       = 5
	linearReferenceCatalogTeamsDestination       = "linear_reference_teams"
	linearReferenceCatalogMembersDestination     = "linear_reference_members"
	linearReferenceCatalogMembershipsDestination = "linear_reference_memberships"
	linearReferenceCatalogProjectsDestination    = "linear_reference_projects"
	linearReferenceCatalogOwnershipDestination   = "linear_reference_ownership"
)

type LinearReferenceCatalogEffects struct {
	Teams       EffectBatch
	Members     EffectBatch
	Memberships EffectBatch
	Projects    EffectBatch
	Ownership   EffectBatch
}

func (effects LinearReferenceCatalogEffects) Batches() []EffectBatch {
	return []EffectBatch{effects.Teams, effects.Members, effects.Memberships, effects.Projects, effects.Ownership}
}

func BuildLinearReferenceCatalogEffects(rows LinearReferenceCatalogRows) (LinearReferenceCatalogEffects, error) {
	teams, err := effectBatchFromValues(linearReferenceCatalogTeamsDestination, EffectReadbackRequired, rows.Teams)
	if err != nil {
		return LinearReferenceCatalogEffects{}, err
	}
	members, err := effectBatchFromValues(linearReferenceCatalogMembersDestination, EffectReadbackRequired, rows.Members)
	if err != nil {
		return LinearReferenceCatalogEffects{}, err
	}
	memberships, err := effectBatchFromValues(linearReferenceCatalogMembershipsDestination, EffectReadbackRequired, rows.Memberships)
	if err != nil {
		return LinearReferenceCatalogEffects{}, err
	}
	projects, err := effectBatchFromValues(linearReferenceCatalogProjectsDestination, EffectReadbackRequired, rows.Projects)
	if err != nil {
		return LinearReferenceCatalogEffects{}, err
	}
	ownership, err := effectBatchFromValues(linearReferenceCatalogOwnershipDestination, EffectReadbackRequired, rows.Ownership)
	if err != nil {
		return LinearReferenceCatalogEffects{}, err
	}
	return LinearReferenceCatalogEffects{
		Teams: teams, Members: members, Memberships: memberships,
		Projects: projects, Ownership: ownership,
	}, nil
}

func normalizeLinearReferenceProject(
	claim Claim,
	payload linearReferenceProjectPayload,
	normalizedAt time.Time,
) (linearReferenceProjectRow, error) {
	if claim.Provider != "linear" || strings.TrimSpace(claim.OrgID) == "" ||
		strings.TrimSpace(payload.ID) == "" || normalizedAt.IsZero() {
		return linearReferenceProjectRow{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	var targetDate *linearReferenceDate
	if value := strings.TrimSpace(payload.TargetDate); value != "" {
		parsed, err := time.Parse("2006-01-02", value[:minInt(len(value), len("2006-01-02"))])
		if err == nil {
			parsed = parsed.UTC()
			canonical := linearReferenceDate(parsed.Format("2006-01-02"))
			targetDate = &canonical
		}
	}
	teamIDs := make([]string, 0, len(payload.Teams.Nodes))
	teamKeys := make([]string, 0, len(payload.Teams.Nodes))
	for _, team := range payload.Teams.Nodes {
		if value := strings.TrimSpace(team.ID); value != "" && !containsString(teamIDs, value) {
			teamIDs = append(teamIDs, value)
		}
		if value := strings.TrimSpace(team.Key); value != "" && !containsString(teamKeys, value) {
			teamKeys = append(teamKeys, value)
		}
	}
	var leadID, leadName, leadEmail *string
	if payload.Lead != nil {
		leadID = optionalLinearString(payload.Lead.ID)
		leadName = optionalLinearString(payload.Lead.Name)
		leadEmail = optionalLinearString(payload.Lead.Email)
	}
	isActive := uint8(1)
	if payload.Trashed || payload.ArchivedAt != nil && strings.TrimSpace(*payload.ArchivedAt) != "" {
		isActive = 0
	}
	return linearReferenceProjectRow{
		ID: payload.ID, OrgID: claim.OrgID, Provider: "linear", Name: linearFirstNonEmpty(payload.Name, payload.ID),
		IsActive: isActive, State: strings.TrimSpace(payload.Status.Type), TargetDate: targetDate,
		URL: strings.TrimSpace(payload.URL), TeamIDs: teamIDs, TeamKeys: teamKeys,
		LeadID: leadID, LeadName: leadName, LeadEmail: leadEmail,
		UpdatedAt: normalizedAt, LastSynced: normalizedAt,
	}, nil
}

func normalizeLinearReferenceTeam(
	claim Claim,
	payload linearReferenceCatalogTeamPayload,
	normalizedAt time.Time,
) (linearReferenceTeamRow, error) {
	if claim.Provider != "linear" || strings.TrimSpace(claim.OrgID) == "" ||
		normalizedAt.IsZero() || strings.TrimSpace(payload.Key) == "" {
		return linearReferenceTeamRow{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	teamKey := strings.TrimSpace(payload.Key)
	nativeTeamKey := teamKey
	members := make([]string, 0, len(payload.Members.Nodes))
	for _, member := range payload.Members.Nodes {
		if member.Active != nil && !*member.Active {
			continue
		}
		identity := linearFirstNonEmpty(member.Email, member.ID)
		if identity = strings.TrimSpace(identity); identity == "" {
			continue
		}
		for _, facet := range linearReferenceMembershipFacets(identity, member.Email) {
			if !containsString(members, facet) {
				members = append(members, facet)
			}
		}
	}
	return linearReferenceTeamRow{
		ID:       teamKey,
		TeamUUID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+teamKey)).String(),
		Name:     linearFirstNonEmpty(payload.Name, teamKey), Description: payload.Description,
		Members: members, ProjectKeys: []string{teamKey}, RepoPatterns: []string{}, IsActive: 1,
		UpdatedAt: normalizedAt, OrgID: claim.OrgID, Provider: "linear",
		NativeTeamKey: &nativeTeamKey,
	}, nil
}

func normalizeLinearReferenceMember(
	claim Claim,
	teamID string,
	payload linearReferenceCatalogMemberPayload,
	normalizedAt time.Time,
) (linearReferenceMemberRow, linearReferenceMembershipRow, string, error) {
	if claim.Provider != "linear" || strings.TrimSpace(claim.OrgID) == "" ||
		strings.TrimSpace(teamID) == "" || normalizedAt.IsZero() {
		return linearReferenceMemberRow{}, linearReferenceMembershipRow{}, "", ErrInvalidConfiguration
	}
	if payload.Active != nil && !*payload.Active {
		return linearReferenceMemberRow{}, linearReferenceMembershipRow{}, "", ErrInvalidConfiguration
	}
	identity := strings.TrimSpace(linearFirstNonEmpty(payload.Email, payload.ID))
	if identity == "" {
		return linearReferenceMemberRow{}, linearReferenceMembershipRow{}, "", ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	memberID := linearMemberID(identity)
	identityValue, err := json.Marshal(identity)
	if err != nil {
		return linearReferenceMemberRow{}, linearReferenceMembershipRow{}, "", err
	}
	// Python's _provider_identities uses json.dumps(..., sort_keys=True),
	// whose default separators retain one space after the object colon and
	// array comma. Keep the durable String column byte-for-byte compatible with
	// that producer rather than relying on Go's compact encoder.
	identityJSON := `{"linear": [` + string(identityValue) + `]}`
	name := linearFirstNonEmpty(payload.Name, identity)
	email := optionalLinearString(payload.Email)
	facets := linearReferenceMembershipFacets(identity, payload.Email)
	return linearReferenceMemberRow{
			OrgID: claim.OrgID, MemberID: memberID, Name: name, Email: email,
			ProviderIdentities: string(identityJSON), IsActive: 1, UpdatedAt: normalizedAt,
		}, linearReferenceMembershipRow{
			OrgID: claim.OrgID, Provider: "linear", TeamID: teamID, MemberID: memberID,
			RawProviderUserID: &facets[0], RawEmail: email, IdentityFacets: facets,
			Source: "native", IsPrimary: 1, Specificity: 100, Priority: 10,
			ValidFrom: normalizedAt, UpdatedAt: normalizedAt,
		}, memberID, nil
}

func linearMemberID(identity string) string {
	return "linear:" + strings.ToLower(strings.TrimSpace(identity))
}

func linearReferenceMembershipFacets(identity, email string) []string {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return nil
	}
	facets := []string{"linear:" + identity}
	if normalizedEmail := strings.ToLower(strings.TrimSpace(email)); normalizedEmail != "" &&
		!containsString(facets, normalizedEmail) {
		facets = append(facets, normalizedEmail)
	}
	return facets
}

func linearReferenceTeamPayloadForOrg(rows []LinearReferenceTeam, orgID, teamKey string) (linearTeamPayload, bool) {
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		return linearTeamPayload{}, false
	}
	for _, row := range rows {
		if strings.TrimSpace(row.OrgID) != orgID {
			continue
		}
		team, ok := linearReferenceTeamPayload([]LinearReferenceTeam{row}, teamKey)
		if ok {
			return team, true
		}
	}
	return linearTeamPayload{}, false
}

func linearFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func optionalLinearString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func (row linearReferenceProjectRow) validate(claim Claim) error {
	if claim.Provider != "linear" || row.Provider != "linear" ||
		row.OrgID != claim.OrgID || strings.TrimSpace(row.ID) == "" ||
		row.UpdatedAt.IsZero() || row.LastSynced.IsZero() {
		return fmt.Errorf("%w: invalid Linear reference project row", ErrInvalidConfiguration)
	}
	return nil
}
