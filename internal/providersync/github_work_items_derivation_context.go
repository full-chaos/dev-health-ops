package providersync

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const githubWorkItemDerivationContextLimit = 100_000

var githubWorkItemIssueKeyPattern = regexp.MustCompile(`^([A-Za-z]{2,})-\d+$`)

var githubWorkItemDerivationInheritableRelationships = map[string]bool{
	"relates_to": true, "relates": true, "duplicates": true, "external_issue_key": true,
}

type githubWorkItemDerivationCandidate struct {
	Source      string    `json:"source"`
	TeamID      *string   `json:"team_id"`
	TeamName    *string   `json:"team_name"`
	Confidence  string    `json:"confidence"`
	Evidence    string    `json:"evidence"`
	IsPrimary   int       `json:"is_primary"`
	Specificity int       `json:"specificity"`
	Priority    int       `json:"priority"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type githubWorkItemDerivationSubject struct {
	WorkItemID    string
	Provider      string
	RepoID        *string
	NativeTeamKey *string
	ProjectKey    *string
	ProjectID     *string
	ProjectName   *string
	Assignees     []string
	OrgID         string
}

type githubWorkItemDerivationTeamFact struct {
	Provider    string
	TeamID      string
	TeamName    string
	ProjectKeys []string
	UpdatedAt   time.Time
}

type githubWorkItemDerivationProjectFact struct {
	Provider    string
	TeamID      string
	TeamName    string
	ProjectID   string
	ProjectKey  *string
	IsPrimary   int
	Specificity int
	Priority    int
	UpdatedAt   time.Time
}

type githubWorkItemDerivationRepoFact struct {
	Provider     string
	TeamID       string
	TeamName     string
	RepoID       *string
	RepoFullName string
	IsPrimary    int
	Specificity  int
	Priority     int
	UpdatedAt    time.Time
}

type githubWorkItemDerivationMemberFact struct {
	Provider          string
	TeamID            string
	TeamName          string
	MemberID          string
	RawProviderUserID *string
	RawEmail          *string
	IdentityFacets    []string
	IsPrimary         int
	Specificity       int
	Priority          int
	UpdatedAt         time.Time
}

type githubWorkItemDerivationManualFallback struct {
	Provider  string
	ScopeType string
	ScopeID   string
	TeamID    string
	TeamName  string
	Reason    string
	Priority  int
}

type githubWorkItemDerivationFacts struct {
	Teams           []githubWorkItemDerivationTeamFact
	Projects        []githubWorkItemDerivationProjectFact
	Repos           []githubWorkItemDerivationRepoFact
	Members         []githubWorkItemDerivationMemberFact
	ManualFallbacks []githubWorkItemDerivationManualFallback
	DonorItems      []githubWorkItemDerivationSubject
}

type githubWorkItemDerivationLoadRequest struct {
	AsOf             time.Time
	DonorWorkItemIDs []string
	DonorIssueKeys   []string
}

type githubWorkItemDerivationContextSource interface {
	Load(
		context.Context,
		Claim,
		githubWorkItemDerivationLoadRequest,
	) (githubWorkItemDerivationFacts, error)
}

type githubWorkItemClickHouseDerivationContextSource struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type githubWorkItemDerivationContext struct {
	projectKeyTeams map[string]githubWorkItemDerivationTeamFact
	projectByID     map[string][]githubWorkItemDerivationCandidate
	projectByKey    map[string][]githubWorkItemDerivationCandidate
	repoByID        map[string][]githubWorkItemDerivationCandidate
	repoByName      map[string][]githubWorkItemDerivationCandidate
	memberByID      map[string][]githubWorkItemDerivationCandidate
	manualFallbacks []githubWorkItemDerivationManualFallback
	linkedIssue     map[string][2]string
}

func loadGitHubWorkItemDerivationContext(
	ctx context.Context,
	claim Claim,
	rows githubWorkItemRows,
	source githubWorkItemDerivationContextSource,
	asOf time.Time,
) (githubWorkItemDerivationContext, error) {
	return loadWorkItemDerivationContextForProvider(
		ctx, "github", claim, rows, source, asOf,
	)
}

// loadWorkItemDerivationContextForProvider is the provider-neutral context
// loader shared by the GitHub and GitLab work-item families. The source facts
// are already provider-keyed, so the resolver can apply the same canonical
// precedence without copying or weakening the team-attribution contract.
func loadWorkItemDerivationContextForProvider(
	ctx context.Context,
	provider string,
	claim Claim,
	rows githubWorkItemRows,
	source githubWorkItemDerivationContextSource,
	asOf time.Time,
) (githubWorkItemDerivationContext, error) {
	if ctx == nil || source == nil || claim.Validate() != nil ||
		claim.Provider != provider || claim.Dataset != "work-items" || asOf.IsZero() {
		return githubWorkItemDerivationContext{}, ErrInvalidConfiguration
	}
	for _, row := range rows.WorkItems {
		if row.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	for _, dependency := range rows.Dependencies {
		if dependency.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	// D17 (owner ruling, 2026-08-06) ratifies failing closed on BOTH branches
	// below, as a deliberate divergence from job_work_items.py:1196-1210, which
	// catches a donor-load failure, logs it, and continues with whatever
	// inheritance it has left.
	//
	// This is deliberately NOT the optional-data class the same ruling governs
	// for fetches. A missing donor does not omit a row: it writes a DIFFERENT
	// team onto work_item_team_attributions and every derived surface
	// downstream, with nothing in the row saying the attribution was computed
	// blind. There is no "land what you have and record what you lost" shape
	// available here, so the unit fails instead. The Python-side silent
	// degradation is tracked as CHAOS-3467.
	//
	// The limit is the same rail for the same reason: a truncated donor set
	// produces confidently wrong attribution, not absent attribution.
	donorIDs, donorKeys := githubWorkItemDerivationDonorTargets(rows.Dependencies)
	if len(donorIDs)+len(donorKeys) > githubWorkItemDerivationContextLimit {
		return githubWorkItemDerivationContext{}, ErrEffectRecoveryUnsafe
	}
	facts, err := source.Load(ctx, claim, githubWorkItemDerivationLoadRequest{
		AsOf: asOf, DonorWorkItemIDs: donorIDs, DonorIssueKeys: donorKeys,
	})
	if err != nil {
		return githubWorkItemDerivationContext{}, err
	}
	for _, donor := range facts.DonorItems {
		if donor.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
	}
	derived := newGitHubWorkItemDerivationContext(facts)
	subjects := make(map[string]githubWorkItemDerivationSubject, len(facts.DonorItems)+len(rows.WorkItems))
	for _, donor := range facts.DonorItems {
		subjects[donor.WorkItemID] = donor
	}
	for _, row := range rows.WorkItems {
		subject := githubWorkItemDerivationSubjectFromRow(row)
		if subject.OrgID != claim.OrgID {
			return githubWorkItemDerivationContext{}, providerfoundation.ErrInvalidScope
		}
		// Fresh rows are authoritative over a persisted donor version.
		subjects[subject.WorkItemID] = subject
	}
	derived.linkedIssue = derived.buildLinkedIssueIndex(subjects, rows.Dependencies)
	return derived, nil
}

func githubWorkItemDerivationDonorTargets(
	dependencies []githubWorkItemDependencyRow,
) ([]string, []string) {
	ids := map[string]struct{}{}
	keys := map[string]struct{}{}
	for _, dependency := range latestGitHubWorkItemDerivationDependencies(dependencies) {
		if !githubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
			continue
		}
		target := strings.TrimSpace(dependency.TargetWorkItemID)
		if strings.HasPrefix(target, "extkey:") {
			key := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(target, "extkey:")))
			if key != "" {
				keys[key] = struct{}{}
			}
		} else if target != "" {
			ids[target] = struct{}{}
		}
	}
	return sortedStringSet(ids), sortedStringSet(keys)
}

func latestGitHubWorkItemDerivationDependencies(
	dependencies []githubWorkItemDependencyRow,
) map[struct{ source, target string }]githubWorkItemDependencyRow {
	type edgeKey struct{ source, target string }
	latest := map[edgeKey]githubWorkItemDependencyRow{}
	for _, dependency := range dependencies {
		key := edgeKey{dependency.SourceWorkItemID, dependency.TargetWorkItemID}
		current, exists := latest[key]
		if !exists || dependency.LastSynced.After(current.LastSynced) ||
			(dependency.LastSynced.Equal(current.LastSynced) && dependency.RelationshipType < current.RelationshipType) {
			latest[key] = dependency
		}
	}
	result := make(map[struct{ source, target string }]githubWorkItemDependencyRow, len(latest))
	for key, dependency := range latest {
		result[struct{ source, target string }{key.source, key.target}] = dependency
	}
	return result
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func githubWorkItemDerivationSubjectFromRow(row githubWorkItemRow) githubWorkItemDerivationSubject {
	var repoID *string
	if row.RepoID != nil {
		value := row.RepoID.String()
		repoID = &value
	}
	return githubWorkItemDerivationSubject{
		WorkItemID: row.WorkItemID, Provider: row.Provider, RepoID: repoID,
		NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
		ProjectID: row.ProjectID, ProjectName: row.ProjectName,
		Assignees: append([]string(nil), row.Assignees...), OrgID: row.OrgID,
	}
}

func newGitHubWorkItemDerivationContext(
	facts githubWorkItemDerivationFacts,
) githubWorkItemDerivationContext {
	result := githubWorkItemDerivationContext{
		projectKeyTeams: map[string]githubWorkItemDerivationTeamFact{},
		projectByID:     map[string][]githubWorkItemDerivationCandidate{},
		projectByKey:    map[string][]githubWorkItemDerivationCandidate{},
		repoByID:        map[string][]githubWorkItemDerivationCandidate{},
		repoByName:      map[string][]githubWorkItemDerivationCandidate{},
		memberByID:      map[string][]githubWorkItemDerivationCandidate{},
		manualFallbacks: append([]githubWorkItemDerivationManualFallback(nil), facts.ManualFallbacks...),
		linkedIssue:     map[string][2]string{},
	}
	for _, team := range facts.Teams {
		for _, rawKey := range append(append([]string(nil), team.ProjectKeys...), team.TeamID) {
			key := strings.TrimSpace(rawKey)
			if key == "" {
				continue
			}
			if _, exists := result.projectKeyTeams[key]; !exists {
				result.projectKeyTeams[key] = team
			}
		}
	}
	for _, fact := range facts.Projects {
		candidate := githubWorkItemDerivationCandidateFromFact(
			"project_ownership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("project_ownership=%s", githubWorkItemDerivationFirstNonEmpty(fact.ProjectID, githubWorkItemDerivationStringValue(fact.ProjectKey))),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		if fact.ProjectID != "" {
			appendDerivationCandidate(result.projectByID, attributionMapKey(fact.Provider, fact.ProjectID), candidate)
		}
		if fact.ProjectKey != nil && strings.TrimSpace(*fact.ProjectKey) != "" {
			appendDerivationCandidate(result.projectByKey, attributionMapKey(fact.Provider, *fact.ProjectKey), candidate)
		}
	}
	for _, fact := range facts.Repos {
		candidate := githubWorkItemDerivationCandidateFromFact(
			"repo_ownership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("repo_ownership=%s", githubWorkItemDerivationFirstNonEmpty(githubWorkItemDerivationStringValue(fact.RepoID), fact.RepoFullName)),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		if fact.RepoID != nil && strings.TrimSpace(*fact.RepoID) != "" {
			appendDerivationCandidate(result.repoByID, attributionMapKey(fact.Provider, *fact.RepoID), candidate)
		}
		if fact.RepoFullName != "" {
			appendDerivationCandidate(result.repoByName, attributionMapKey(fact.Provider, fact.RepoFullName), candidate)
		}
	}
	for _, fact := range facts.Members {
		candidate := githubWorkItemDerivationCandidateFromFact(
			"assignee_membership", fact.TeamID, fact.TeamName,
			fmt.Sprintf("assignee_membership=%s", githubWorkItemDerivationFirstNonEmpty(fact.MemberID, githubWorkItemDerivationStringValue(fact.RawEmail))),
			fact.IsPrimary, fact.Specificity, fact.Priority, fact.UpdatedAt,
		)
		identities := []string{fact.MemberID, githubWorkItemDerivationStringValue(fact.RawProviderUserID), githubWorkItemDerivationStringValue(fact.RawEmail)}
		identities = append(identities, fact.IdentityFacets...)
		seen := map[string]struct{}{}
		for _, identity := range identities {
			key := normalizeDerivationIdentity(identity)
			if key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			appendDerivationCandidate(result.memberByID, attributionMapKey(fact.Provider, key), candidate)
		}
	}
	return result
}

func githubWorkItemDerivationCandidateFromFact(
	source, teamID, teamName, evidence string,
	isPrimary, specificity, priority int,
	updatedAt time.Time,
) githubWorkItemDerivationCandidate {
	if teamName == "" {
		teamName = teamID
	}
	// An ownership row's team_id is passed through RAW by Python, so a blank
	// team id persists as "" on work_item_team_attributions -- NOT as NULL.
	// githubWorkItemDerivationStringPointer maps "" to nil, which made Go write
	// NULL where Python writes "", and in a Nullable(String) column those are
	// different values. The two daily rollups normalise both to "unassigned",
	// so the attribution table is the only surface that can see it. Pinned by
	// the empty_string_team_id_is_not_null oracle case.
	//
	// Only this FACT-derived path is corrected, because only it is measured.
	// The native_team, issue_project, manual_fallback and linked_issue
	// candidates still route through the nil-on-empty helper: a blank team id
	// is not reachable there from any oracle case in this PR, and changing
	// them would be an unmeasured claim.
	return githubWorkItemDerivationCandidate{
		Source: source, TeamID: &teamID, TeamName: &teamName,
		Confidence: confidenceForPrimary(isPrimary), Evidence: evidence,
		IsPrimary: isPrimary, Specificity: specificity, Priority: priority,
		UpdatedAt: normalizedDerivationTime(updatedAt),
	}
}

func confidenceForPrimary(primary int) string {
	if primary != 0 {
		return "high"
	}
	return "medium"
}

func appendDerivationCandidate(
	target map[string][]githubWorkItemDerivationCandidate,
	key string,
	candidate githubWorkItemDerivationCandidate,
) {
	target[key] = append(target[key], candidate)
}

func attributionMapKey(provider, key string) string {
	return strings.TrimSpace(provider) + "\x00" + strings.TrimSpace(key)
}

func normalizeDerivationIdentity(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func normalizedDerivationTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return value.UTC()
}

func (derived githubWorkItemDerivationContext) resolve(
	subject githubWorkItemDerivationSubject,
) (*string, *string, []githubWorkItemDerivationCandidate) {
	bySource := map[string][]githubWorkItemDerivationCandidate{}
	if candidate := derived.nativeTeamCandidate(subject); candidate != nil {
		bySource[candidate.Source] = append(bySource[candidate.Source], *candidate)
	}
	if inherited, exists := derived.linkedIssue[subject.WorkItemID]; exists {
		bySource["linked_issue"] = append(bySource["linked_issue"], githubWorkItemDerivationCandidate{
			Source: "linked_issue", TeamID: githubWorkItemDerivationStringPointer(inherited[0]), TeamName: githubWorkItemDerivationStringPointer(inherited[1]),
			Confidence: "medium", Evidence: "linked_issue=" + subject.WorkItemID,
			IsPrimary: 1, Specificity: 90, UpdatedAt: normalizedDerivationTime(time.Time{}),
		})
	}
	issueProjectTeams := map[string]struct{}{}
	if candidate := derived.issueProjectCandidate(subject); candidate != nil {
		bySource[candidate.Source] = append(bySource[candidate.Source], *candidate)
		if candidate.TeamID != nil {
			issueProjectTeams[*candidate.TeamID] = struct{}{}
		}
	}
	bySource["project_ownership"] = append(
		bySource["project_ownership"],
		derived.projectByID[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.ProjectID))]...,
	)
	for _, candidate := range derived.projectByKey[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.ProjectKey))] {
		if candidate.TeamID != nil {
			if _, duplicate := issueProjectTeams[*candidate.TeamID]; duplicate {
				continue
			}
		}
		bySource["project_ownership"] = append(bySource["project_ownership"], candidate)
	}
	bySource["repo_ownership"] = append(
		bySource["repo_ownership"],
		derived.repoByID[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.RepoID))]...,
	)
	bySource["repo_ownership"] = append(
		bySource["repo_ownership"],
		derived.repoByName[attributionMapKey(subject.Provider, githubWorkItemDerivationStringValue(subject.ProjectID))]...,
	)
	for _, assignee := range subject.Assignees {
		bySource["assignee_membership"] = append(
			bySource["assignee_membership"],
			derived.memberByID[attributionMapKey(subject.Provider, normalizeDerivationIdentity(assignee))]...,
		)
	}
	bySource["manual_fallback"] = append(
		bySource["manual_fallback"], derived.manualCandidates(subject)...,
	)

	order := []string{
		"native_team", "issue_project", "project_ownership", "repo_ownership",
		"assignee_membership", "linked_issue", "manual_fallback", "unassigned",
	}
	var primary *githubWorkItemDerivationCandidate
	all := make([]githubWorkItemDerivationCandidate, 0)
	for _, source := range order {
		candidates := rankDerivationCandidates(dedupeDerivationCandidates(bySource[source]))
		if primary == nil && len(candidates) > 0 {
			value := candidates[0]
			primary = &value
		}
		all = append(all, candidates...)
	}
	if primary == nil {
		value := githubWorkItemDerivationCandidate{
			Source: "unassigned", Confidence: "none", Evidence: "no_candidate",
			IsPrimary: 1, UpdatedAt: normalizedDerivationTime(time.Time{}),
		}
		primary = &value
		all = append(all, value)
	}
	marked := make([]githubWorkItemDerivationCandidate, len(all))
	for index, candidate := range all {
		candidate.IsPrimary = 0
		if sameDerivationCandidate(candidate, *primary) {
			candidate.IsPrimary = 1
			primary = &candidate
		}
		marked[index] = candidate
	}
	return primary.TeamID, primary.TeamName, marked
}

func (derived githubWorkItemDerivationContext) nativeTeamCandidate(
	subject githubWorkItemDerivationSubject,
) *githubWorkItemDerivationCandidate {
	if subject.NativeTeamKey == nil {
		return nil
	}
	team, exists := derived.projectKeyTeams[strings.TrimSpace(*subject.NativeTeamKey)]
	if !exists {
		return nil
	}
	return &githubWorkItemDerivationCandidate{
		Source: "native_team", TeamID: githubWorkItemDerivationStringPointer(team.TeamID), TeamName: githubWorkItemDerivationStringPointer(githubWorkItemDerivationFirstNonEmpty(team.TeamName, team.TeamID)),
		Confidence: "high", Evidence: "native_team_key=" + *subject.NativeTeamKey,
		IsPrimary: 1, Specificity: 100, UpdatedAt: normalizedDerivationTime(time.Time{}),
	}
}

func (derived githubWorkItemDerivationContext) issueProjectCandidate(
	subject githubWorkItemDerivationSubject,
) *githubWorkItemDerivationCandidate {
	keys := []string{workItemDerivationScope(subject)}
	if subject.ProjectKey != nil && *subject.ProjectKey != keys[0] {
		keys = append(keys, *subject.ProjectKey)
	}
	for _, key := range keys {
		// Python looks the key up STRIPPED (providers/teams.py:88,
		// `work_scope_id.strip()`) but reports the RAW key in its evidence, so
		// the trim belongs on the lookup alone. Trimming the evidence too
		// would swap one divergence for another; both halves are pinned by the
		// issue_project_scope_needs_trimming oracle case.
		team, exists := derived.projectKeyTeams[strings.TrimSpace(key)]
		if !exists {
			continue
		}
		return &githubWorkItemDerivationCandidate{
			Source: "issue_project", TeamID: githubWorkItemDerivationStringPointer(team.TeamID), TeamName: githubWorkItemDerivationStringPointer(githubWorkItemDerivationFirstNonEmpty(team.TeamName, team.TeamID)),
			Confidence: "high", Evidence: "issue_project_key=" + key,
			IsPrimary: 1, Specificity: 50, UpdatedAt: normalizedDerivationTime(time.Time{}),
		}
	}
	return nil
}

func workItemDerivationScope(subject githubWorkItemDerivationSubject) string {
	if subject.Provider == "jira" && subject.ProjectKey != nil {
		return *subject.ProjectKey
	}
	for _, value := range []*string{subject.ProjectID, subject.ProjectName, subject.NativeTeamKey, subject.ProjectKey} {
		if value != nil && *value != "" {
			return *value
		}
	}
	return ""
}

func (derived githubWorkItemDerivationContext) manualCandidates(
	subject githubWorkItemDerivationSubject,
) []githubWorkItemDerivationCandidate {
	result := []githubWorkItemDerivationCandidate{}
	for _, rule := range derived.manualFallbacks {
		if rule.Provider != "" && rule.Provider != subject.Provider && rule.ScopeType != "issue_key_prefix" {
			continue
		}
		scopeID := strings.TrimSpace(rule.ScopeID)
		matched := false
		switch rule.ScopeType {
		case "repo":
			// Python compares against a SET built by dropping falsy values
			// (compute_work_items.py:304-309), so a blank scope_id matches
			// nothing. Comparing directly against stringValue(nil) == ""
			// instead made a blank rule match EVERY item with a null repo --
			// one empty config row silently attributing the whole tenant to
			// one team. githubWorkItemDerivationScopeMatch reproduces the set
			// semantics: empty candidates are dropped and an empty scope_id
			// can never match.
			matched = githubWorkItemDerivationScopeMatch(scopeID,
				githubWorkItemDerivationStringValue(subject.RepoID),
				githubWorkItemDerivationStringValue(subject.ProjectID))
		case "project":
			matched = githubWorkItemDerivationScopeMatch(scopeID,
				githubWorkItemDerivationStringValue(subject.ProjectID),
				githubWorkItemDerivationStringValue(subject.ProjectKey),
				workItemDerivationScope(subject))
		case "member":
			for _, assignee := range subject.Assignees {
				// Python's member_ids drops falsy assignees, so a blank
				// assignee cannot be matched by a blank rule either.
				if scopeID == "" || assignee == "" {
					continue
				}
				matched = matched || normalizeDerivationIdentity(scopeID) == normalizeDerivationIdentity(assignee)
			}
		case "issue_key_prefix":
			prefix := githubWorkItemIssueKeyPrefix(subject.WorkItemID)
			matched = prefix != "" && prefix == strings.ToUpper(scopeID)
		}
		if !matched {
			continue
		}
		evidence := fmt.Sprintf("manual_fallback:%s=%s", rule.ScopeType, rule.ScopeID)
		if rule.Reason != "" {
			evidence += " (" + rule.Reason + ")"
		}
		result = append(result, githubWorkItemDerivationCandidate{
			Source: "manual_fallback", TeamID: githubWorkItemDerivationStringPointer(rule.TeamID), TeamName: githubWorkItemDerivationStringPointer(githubWorkItemDerivationFirstNonEmpty(rule.TeamName, rule.TeamID)),
			Confidence: "manual", Evidence: evidence, IsPrimary: 1,
			Priority: rule.Priority, UpdatedAt: normalizedDerivationTime(time.Time{}),
		})
	}
	return result
}

// githubWorkItemDerivationScopeMatch mirrors Python's `scope_id in {...}`
// where the set is built by dropping falsy values: an empty scope_id matches
// nothing, and an empty candidate is not a member.
func githubWorkItemDerivationScopeMatch(scopeID string, candidates ...string) bool {
	if scopeID == "" {
		return false
	}
	for _, candidate := range candidates {
		if candidate != "" && strings.TrimSpace(candidate) == scopeID {
			return true
		}
	}
	return false
}

func githubWorkItemIssueKeyPrefix(workItemID string) string {
	_, suffix, found := strings.Cut(workItemID, ":")
	if !found {
		return ""
	}
	match := githubWorkItemIssueKeyPattern.FindStringSubmatch(strings.TrimSpace(suffix))
	if len(match) != 2 {
		return ""
	}
	return strings.ToUpper(match[1])
}

func rankDerivationCandidates(
	candidates []githubWorkItemDerivationCandidate,
) []githubWorkItemDerivationCandidate {
	sort.SliceStable(candidates, func(left, right int) bool {
		a, b := candidates[left], candidates[right]
		if a.IsPrimary != b.IsPrimary {
			return a.IsPrimary > b.IsPrimary
		}
		if a.Specificity != b.Specificity {
			return a.Specificity > b.Specificity
		}
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if !a.UpdatedAt.Equal(b.UpdatedAt) {
			return a.UpdatedAt.After(b.UpdatedAt)
		}
		if leftTeamID, rightTeamID := githubWorkItemDerivationStringValue(a.TeamID), githubWorkItemDerivationStringValue(b.TeamID); leftTeamID != rightTeamID {
			return leftTeamID < rightTeamID
		}
		if leftTeamName, rightTeamName := githubWorkItemDerivationStringValue(a.TeamName), githubWorkItemDerivationStringValue(b.TeamName); leftTeamName != rightTeamName {
			return leftTeamName < rightTeamName
		}
		if a.Confidence != b.Confidence {
			return a.Confidence < b.Confidence
		}
		if a.Evidence != b.Evidence {
			return a.Evidence < b.Evidence
		}
		return a.Source < b.Source
	})
	return candidates
}

func dedupeDerivationCandidates(
	candidates []githubWorkItemDerivationCandidate,
) []githubWorkItemDerivationCandidate {
	result := make([]githubWorkItemDerivationCandidate, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		key := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%d\x00%s",
			candidate.Source, githubWorkItemDerivationStringValue(candidate.TeamID), githubWorkItemDerivationStringValue(candidate.TeamName),
			candidate.Confidence, candidate.Evidence, candidate.IsPrimary,
			candidate.Specificity, candidate.Priority, candidate.UpdatedAt.UTC().Format(time.RFC3339Nano))
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, candidate)
	}
	return result
}

func sameDerivationCandidate(left, right githubWorkItemDerivationCandidate) bool {
	return left.Source == right.Source && githubWorkItemDerivationStringValue(left.TeamID) == githubWorkItemDerivationStringValue(right.TeamID) &&
		githubWorkItemDerivationStringValue(left.TeamName) == githubWorkItemDerivationStringValue(right.TeamName) && left.Confidence == right.Confidence &&
		left.Evidence == right.Evidence && left.Specificity == right.Specificity &&
		left.Priority == right.Priority && left.UpdatedAt.Equal(right.UpdatedAt)
}

func (derived githubWorkItemDerivationContext) buildLinkedIssueIndex(
	subjects map[string]githubWorkItemDerivationSubject,
	dependencies []githubWorkItemDependencyRow,
) map[string][2]string {
	donors := map[string][2]string{}
	baseNative := map[string]bool{}
	keyIndex := map[string]string{}
	ambiguous := map[string]bool{}
	allowedDonorSources := map[string]bool{
		"native_team": true, "issue_project": true, "project_ownership": true,
		"repo_ownership": true, "assignee_membership": true,
	}
	for _, subject := range subjects {
		baseNative[subject.WorkItemID] = derived.nativeTeamCandidate(subject) != nil
		teamID, teamName, candidates := derived.resolveWithoutLinked(subject)
		primarySource := ""
		for _, candidate := range candidates {
			if candidate.IsPrimary == 1 {
				primarySource = candidate.Source
				break
			}
		}
		if teamID != nil && allowedDonorSources[primarySource] {
			donors[subject.WorkItemID] = [2]string{*teamID, githubWorkItemDerivationFirstNonEmpty(githubWorkItemDerivationStringValue(teamName), *teamID)}
		}
		if (subject.Provider == "linear" || subject.Provider == "jira") && strings.Contains(subject.WorkItemID, ":") {
			_, suffix, _ := strings.Cut(subject.WorkItemID, ":")
			key := strings.ToUpper(strings.TrimSpace(suffix))
			if ambiguous[key] {
				continue
			}
			if existing, exists := keyIndex[key]; exists && existing != subject.WorkItemID {
				delete(keyIndex, key)
				ambiguous[key] = true
			} else {
				keyIndex[key] = subject.WorkItemID
			}
		}
	}
	candidates := map[string][]struct {
		target string
		team   [2]string
	}{}
	for _, dependency := range latestGitHubWorkItemDerivationDependencies(dependencies) {
		if !githubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] || baseNative[dependency.SourceWorkItemID] {
			continue
		}
		target := dependency.TargetWorkItemID
		if strings.HasPrefix(target, "extkey:") {
			target = keyIndex[strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(target, "extkey:")))]
		}
		if target == "" {
			continue
		}
		if donor, exists := donors[target]; exists {
			candidates[dependency.SourceWorkItemID] = append(candidates[dependency.SourceWorkItemID], struct {
				target string
				team   [2]string
			}{target: target, team: donor})
		}
	}
	result := map[string][2]string{}
	for source, possible := range candidates {
		sort.Slice(possible, func(left, right int) bool { return possible[left].target < possible[right].target })
		result[source] = possible[0].team
	}
	return result
}

func (derived githubWorkItemDerivationContext) resolveWithoutLinked(
	subject githubWorkItemDerivationSubject,
) (*string, *string, []githubWorkItemDerivationCandidate) {
	saved := derived.linkedIssue
	derived.linkedIssue = nil
	teamID, teamName, candidates := derived.resolve(subject)
	derived.linkedIssue = saved
	return teamID, teamName, candidates
}

func (source githubWorkItemClickHouseDerivationContextSource) Load(
	ctx context.Context,
	claim Claim,
	request githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	if ctx == nil || source.Conn == nil || source.Lease == nil || claim.Validate() != nil ||
		(claim.Provider != "github" && claim.Provider != "gitlab") ||
		claim.Dataset != "work-items" || request.AsOf.IsZero() {
		return githubWorkItemDerivationFacts{}, ErrInvalidConfiguration
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	facts := githubWorkItemDerivationFacts{}
	var err error
	if facts.Teams, err = source.loadTeams(ctx, claim.OrgID); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Projects, err = source.loadProjects(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Repos, err = source.loadRepos(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Members, err = source.loadMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.ManualFallbacks, err = source.loadManualFallbacks(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.DonorItems, err = source.loadDonors(ctx, claim.OrgID, request); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if err := source.Lease.Assert(ctx); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	return facts, nil
}

func (source githubWorkItemClickHouseDerivationContextSource) loadTeams(
	ctx context.Context, orgID string,
) ([]githubWorkItemDerivationTeamFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT provider, id,
       argMax(name, (updated_at, last_synced, name)),
       argMax(project_keys, (updated_at, last_synced, toJSONString(project_keys))),
       max(updated_at)
FROM teams
WHERE org_id = ?
GROUP BY provider, id, org_id
ORDER BY provider, id
LIMIT ?`, orgID, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationTeamFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationTeamFact
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.ProjectKeys, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadProjects(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationProjectFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT g.provider, g.team_id, ifNull(nullIf(t.name, ''), g.team_id),
       g.project_id, g.project_key, g.is_primary, g.specificity, g.priority, g.updated_at
FROM (
  SELECT o.org_id, o.provider, o.project_id, o.team_id,
         argMax(o.project_key, (o.updated_at, o.valid_from)) AS project_key,
         argMax(o.is_primary, (o.updated_at, o.valid_from)) AS is_primary,
         argMax(o.specificity, (o.updated_at, o.valid_from)) AS specificity,
         argMax(o.priority, (o.updated_at, o.valid_from)) AS priority,
         max(o.updated_at) AS updated_at
  FROM team_project_ownership AS o
  WHERE o.org_id = ? AND o.valid_from <= ? AND (o.valid_to IS NULL OR o.valid_to > ?)
  GROUP BY o.org_id, o.provider, o.project_id, o.team_id
) AS g
LEFT JOIN (
  SELECT org_id, id, argMax(name, (updated_at, last_synced, name)) AS name
  FROM teams
  GROUP BY org_id, id
) AS t ON t.org_id = g.org_id AND t.id = g.team_id
ORDER BY g.provider, g.project_id, g.team_id
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationProjectFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationProjectFact
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.ProjectID, &fact.ProjectKey, &fact.IsPrimary, &fact.Specificity, &fact.Priority, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadRepos(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationRepoFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT g.provider, g.team_id, ifNull(nullIf(t.name, ''), g.team_id),
       toString(g.repo_id), g.repo_full_name, g.is_primary, g.specificity, g.priority, g.updated_at
FROM (
  SELECT o.org_id, o.provider, o.repo_full_name, o.team_id,
         argMax(o.repo_id, (o.updated_at, o.valid_from)) AS repo_id,
         argMax(o.is_primary, (o.updated_at, o.valid_from)) AS is_primary,
         argMax(o.specificity, (o.updated_at, o.valid_from)) AS specificity,
         argMax(o.priority, (o.updated_at, o.valid_from)) AS priority,
         max(o.updated_at) AS updated_at
  FROM team_repo_ownership AS o
  WHERE o.org_id = ? AND o.valid_from <= ? AND (o.valid_to IS NULL OR o.valid_to > ?)
  GROUP BY o.org_id, o.provider, o.repo_full_name, o.team_id
) AS g
LEFT JOIN (
  SELECT org_id, id, argMax(name, (updated_at, last_synced, name)) AS name
  FROM teams
  GROUP BY org_id, id
) AS t ON t.org_id = g.org_id AND t.id = g.team_id
ORDER BY g.provider, g.repo_full_name, g.team_id
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationRepoFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationRepoFact
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.RepoID, &fact.RepoFullName, &fact.IsPrimary, &fact.Specificity, &fact.Priority, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadMembers(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationMemberFact, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT g.provider, g.team_id, ifNull(nullIf(t.name, ''), g.team_id),
       g.member_id, g.raw_provider_user_id, g.raw_email, g.identity_facets,
       g.is_primary, g.specificity, g.priority, g.updated_at
FROM (
  SELECT o.org_id, o.provider, o.team_id, o.member_id,
         argMax(o.raw_provider_user_id, (o.updated_at, o.valid_from)) AS raw_provider_user_id,
         argMax(o.raw_email, (o.updated_at, o.valid_from)) AS raw_email,
         argMax(o.identity_facets, (o.updated_at, o.valid_from)) AS identity_facets,
         argMax(o.is_primary, (o.updated_at, o.valid_from)) AS is_primary,
         argMax(o.specificity, (o.updated_at, o.valid_from)) AS specificity,
         argMax(o.priority, (o.updated_at, o.valid_from)) AS priority,
         max(o.updated_at) AS updated_at
  FROM team_memberships AS o
  WHERE o.org_id = ? AND o.valid_from <= ? AND (o.valid_to IS NULL OR o.valid_to > ?)
  GROUP BY o.org_id, o.provider, o.team_id, o.member_id
) AS g
LEFT JOIN (
  SELECT org_id, id, argMax(name, (updated_at, last_synced, name)) AS name
  FROM teams
  GROUP BY org_id, id
) AS t ON t.org_id = g.org_id AND t.id = g.team_id
ORDER BY g.provider, g.member_id, g.team_id
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationMemberFact{}
	for rows.Next() {
		var fact githubWorkItemDerivationMemberFact
		if err := rows.Scan(&fact.Provider, &fact.TeamID, &fact.TeamName, &fact.MemberID, &fact.RawProviderUserID, &fact.RawEmail, &fact.IdentityFacets, &fact.IsPrimary, &fact.Specificity, &fact.Priority, &fact.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadManualFallbacks(
	ctx context.Context, orgID string, asOf time.Time,
) ([]githubWorkItemDerivationManualFallback, error) {
	rows, err := source.Conn.Query(ctx, `
SELECT provider, scope_type, scope_id, team_id, ifNull(nullIf(team_name, ''), team_id), reason, priority
FROM manual_attribution_fallbacks FINAL
WHERE org_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
ORDER BY provider, scope_type, scope_id, priority, team_id, team_name, reason
LIMIT ?`, orgID, asOf, asOf, githubWorkItemDerivationContextLimit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationManualFallback{}
	for rows.Next() {
		var fact githubWorkItemDerivationManualFallback
		if err := rows.Scan(&fact.Provider, &fact.ScopeType, &fact.ScopeID, &fact.TeamID, &fact.TeamName, &fact.Reason, &fact.Priority); err != nil {
			return nil, err
		}
		result = append(result, fact)
		if len(result) > githubWorkItemDerivationContextLimit {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func (source githubWorkItemClickHouseDerivationContextSource) loadDonors(
	ctx context.Context, orgID string, request githubWorkItemDerivationLoadRequest,
) ([]githubWorkItemDerivationSubject, error) {
	if len(request.DonorWorkItemIDs) == 0 && len(request.DonorIssueKeys) == 0 {
		return []githubWorkItemDerivationSubject{}, nil
	}
	maximum := len(request.DonorWorkItemIDs) + len(request.DonorIssueKeys)*2
	rows, err := source.Conn.Query(ctx, `
SELECT work_item_id, provider, toString(repo_id), native_team_key, project_key,
       project_id, project_name, assignees, org_id
FROM work_items FINAL
WHERE org_id = ? AND (
  has(?, work_item_id)
  OR (provider IN ('linear', 'jira') AND has(?, upper(splitByChar(':', work_item_id)[-1])))
)
LIMIT ?`, orgID, request.DonorWorkItemIDs, request.DonorIssueKeys, maximum+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []githubWorkItemDerivationSubject{}
	for rows.Next() {
		var subject githubWorkItemDerivationSubject
		if err := rows.Scan(
			&subject.WorkItemID, &subject.Provider, &subject.RepoID, &subject.NativeTeamKey,
			&subject.ProjectKey, &subject.ProjectID, &subject.ProjectName,
			&subject.Assignees, &subject.OrgID,
		); err != nil {
			return nil, err
		}
		result = append(result, subject)
		if len(result) > maximum {
			return nil, ErrEffectRecoveryUnsafe
		}
	}
	return result, rows.Err()
}

func githubWorkItemDerivationStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	copy := value
	return &copy
}

func githubWorkItemDerivationStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func githubWorkItemDerivationFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ githubWorkItemDerivationContextSource = githubWorkItemClickHouseDerivationContextSource{}
