package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/gitlabcode"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/synccoverage"
	"github.com/full-chaos/dev-health-ops/internal/synclimits"
)

// answer is an HTTPException the Python route raises: the transaction
// rolls back and the api writes {"detail": detail} with status.
type answer struct {
	status int
	detail pyjson.Value
}

func (a *answer) Error() string { return fmt.Sprintf("HTTP %d", a.status) }

func refuse(status int, detail pyjson.Value) error { return &answer{status: status, detail: detail} }

// replaceRepositories is sync.py's replace_sync_config_repositories: the
// body (owner, min length 1; repos, a list of str), the org's config, the
// canonical-incident gate on its targets, GitHub and GitLab only, then
// _replace_planner_repository_selection in one transaction.
func (h *handlers) replaceRepositories(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	owner, _ := problems.RequiredString(object, "owner", 1, 0)
	repos, _ := problems.DefaultedStringList(object, "repos")
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	org := orgID(r)
	targets, err := storedTargets(config)
	if err != nil {
		h.fail(w, r, "sync_targets", err)
		return
	}
	if err := h.requireCanonicalIncident(ctx, org, targets); err != nil {
		h.answerOrFail(w, r, "canonical_incident_feature", err)
		return
	}
	provider := pythonparity.Lower(config.Provider)
	if provider != "github" && provider != "gitlab" {
		policy.WriteDetail(w, http.StatusBadRequest, "Repository selection is only supported for GitHub and GitLab configs", nil)
		return
	}
	var out *pyjson.Object
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var err error
		out, err = h.replacePlannerSelection(ctx, tx, org, config, owner, repos)
		return err
	})
	if err != nil {
		h.answerOrFail(w, r, "replace_repository_selection", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// answerOrFail writes a raised HTTPException, or the api's bare 500.
func (h *handlers) answerOrFail(w http.ResponseWriter, r *http.Request, step string, err error) {
	var raised *answer
	if errors.As(err, &raised) {
		policy.WriteDetail(w, raised.status, raised.detail, nil)
		return
	}
	h.fail(w, r, step, err)
}

// storedTargets is list(config.sync_targets or []): the stored JSON as
// list() reads it (a falsy value is [], a string its characters, a dict
// its keys); any other value raises TypeError.
func storedTargets(config *syncConfig) ([]pyjson.Value, error) {
	value, err := decodeStored(config.SyncTargets)
	if err != nil {
		return nil, err
	}
	if !pyjson.Truthy(value) {
		return []pyjson.Value{}, nil
	}
	switch typed := value.(type) {
	case []pyjson.Value:
		return typed, nil
	case string:
		out := []pyjson.Value{}
		for _, r := range pyjson.Runes(typed) {
			out = append(out, pyjson.FromRunes([]rune{r}))
		}
		return out, nil
	case *pyjson.Object:
		out := []pyjson.Value{}
		for _, key := range typed.Keys() {
			out = append(out, key)
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: list() of %T", errUnrenderable, value)
}

// requireCanonicalIncident is _require_canonical_incident_sync_access:
// when any target lower-cased is a gated one (a non-str target raises
// AttributeError on .lower() when reached), the org's
// canonical_incident_ingestion decision must allow it, else 403 with
// CanonicalIncidentFeatureDisabledError's text.
func (h *handlers) requireCanonicalIncident(ctx context.Context, org string, targets []pyjson.Value) error {
	gated := false
	for _, target := range targets {
		text, ok := target.(string)
		if !ok {
			return fmt.Errorf("%w: sync target %T has no lower()", errUnrenderable, target)
		}
		if gatedSyncTargets[pythonparity.Lower(text)] {
			gated = true
			break
		}
	}
	if !gated {
		return nil
	}
	reason := licensing.ReasonInvalidFeatureState
	if parsed, err := pythonparity.ParseUUID(org); err == nil {
		decision, err := h.features.Decide(ctx, parsed.String(), canonicalIncidentFeatureKey)
		if err != nil {
			return err
		}
		if decision.Allowed {
			return nil
		}
		reason = decision.Reason
	}
	return refuse(http.StatusForbidden, "feature_disabled: canonical incident ingestion is disabled ("+reason+")")
}

// sourceRow is one integration_sources row _planner_sources_for_config
// reads, as the ORM holds it: its stored values and the ones assigned
// since (nil: unchanged).
type sourceRow struct {
	ID         uuid.UUID
	ExternalID string
	SourceType string
	Name       string
	FullName   string
	Metadata   pyjson.Value
	IsEnabled  bool
	// The assignments made by the route.
	newSourceType, newName, newFullName *string
	newMetadata                         pyjson.Value
	metadataSet                         bool
	newEnabled                          *bool
	newLastSeen                         *time.Time
}

// desiredRow is one _planner_source_rows IntegrationSource.
type desiredRow struct {
	Provider, SourceType, ExternalID, Name, FullName string
	Metadata                                         *pyjson.Object
}

// replacePlannerSelection is _replace_planner_repository_selection.
func (h *handlers) replacePlannerSelection(ctx context.Context, tx pgx.Tx, org string, config *syncConfig, owner string, repos []string) (*pyjson.Object, error) {
	if config.IntegrationID == nil {
		return nil, refuse(http.StatusConflict, "Repository selection edits require a planner-managed sync config")
	}
	integrationID := *config.IntegrationID
	existing, err := plannerSourcesTx(ctx, tx, org, integrationID, config)
	if err != nil {
		return nil, err
	}
	enabledExisting := 0
	for _, source := range existing {
		if source.IsEnabled {
			enabledExisting++
		}
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, synclimits.AdvisoryLockKey(org)); err != nil {
		return nil, fmt.Errorf("repo limit lock: %w", err)
	}
	current, err := synclimits.ActiveRepoUsageCount(ctx, tx, org)
	if err != nil {
		return nil, err
	}
	requested := int64(current - enabledExisting + len(repos))
	orgUUID, err := pythonparity.ParseUUID(org)
	if err != nil {
		return nil, fmt.Errorf("org id is not a UUID: %w", err)
	}
	allowed, reason, err := licensing.CheckLimit(ctx, tx, orgUUID, "max_repos", requested)
	if err != nil {
		return nil, err
	}
	if !allowed {
		if reason == "" {
			reason = fmt.Sprintf("Repo limit exceeded (selecting %d repos)", len(repos))
		}
		return nil, refuse(http.StatusForbidden, reason)
	}

	storedOptions, err := decodeStored(config.SyncOptions)
	if err != nil {
		return nil, err
	}
	options, err := convertDict(storedOptions, true)
	if err != nil {
		return nil, err
	}
	provider := pythonparity.Lower(config.Provider)
	if provider == "gitlab" {
		options.Set("group", owner)
	} else {
		options.Set("owner", owner)
	}
	options = without(options, "all_repos")
	// The next session.execute autoflushes the assignment: an UPDATE only
	// when the new dict differs (Python ==) from the loaded value.
	if !pyjson.Equal(options, storedOptions) {
		text, err := pyjson.Dumps(options)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE sync_configurations SET sync_options = $1::json, updated_at = $2 WHERE id = $3`,
			text, h.now().UTC(), config.ID); err != nil {
			return nil, fmt.Errorf("update sync_options: %w", err)
		}
	}
	credentialID, err := integrationCredentialTx(ctx, tx, org, integrationID)
	if err != nil {
		return nil, err
	}
	// SyncConfigBatchCreate(...): name and provider need at least one
	// character, sync_targets must be list[str].
	if config.Name == "" || config.Provider == "" {
		return nil, fmt.Errorf("%w: SyncConfigBatchCreate refuses an empty name or provider", errUnrenderable)
	}
	targetsValue, err := decodeStored(config.SyncTargets)
	if err != nil {
		return nil, err
	}
	if _, err := pyStringList(targetsValue); err != nil {
		return nil, err
	}

	gitlabProjects := map[string]gitlabProject{}
	if provider == "gitlab" && len(repos) > 0 {
		var credential *string
		if credentialID != nil {
			text := credentialID.String()
			credential = &text
		}
		gitlabProjects, _, err = h.resolveGitLabProjects(ctx, tx, org, credential, options, repos)
		if err != nil {
			return nil, err
		}
		// The effective gitlab_url is set on the same dict object the
		// session already flushed, so the ORM sees no change and never
		// writes it: nothing to persist here.
	}
	desired, err := plannerSourceRows(config, options, repos, gitlabProjects, owner)
	if err != nil {
		return nil, err
	}
	byExternalID := map[string]*sourceRow{}
	for index := range existing {
		byExternalID[existing[index].ExternalID] = &existing[index]
	}
	desiredIDs := map[string]bool{}
	for _, row := range desired {
		desiredIDs[row.ExternalID] = true
	}
	for index := range existing {
		if !desiredIDs[existing[index].ExternalID] {
			disabled := false
			existing[index].newEnabled = &disabled
		}
	}
	now := h.now().UTC()
	var pending []desiredRow
	for _, row := range desired {
		source, found := byExternalID[row.ExternalID]
		if !found {
			pending = append(pending, row)
			continue
		}
		sourceType, name, fullName := row.SourceType, row.Name, row.FullName
		source.newSourceType, source.newName, source.newFullName = &sourceType, &name, &fullName
		source.newMetadata, source.metadataSet = row.Metadata, true
		enabled := true
		source.newEnabled = &enabled
		seen := now
		source.newLastSeen = &seen
	}
	if err := flushSources(ctx, tx, org, integrationID, existing, pending, now); err != nil {
		return nil, err
	}
	if err := synccoverage.InvalidateForIntegration(ctx, tx, org, integrationID.String()); err != nil {
		return nil, err
	}
	refreshed, err := plannerSourcesTx(ctx, tx, org, integrationID, config)
	if err != nil {
		return nil, err
	}
	var enabled []pyjson.Value
	for _, source := range refreshed {
		if source.IsEnabled {
			enabled = append(enabled, source.FullName)
		}
	}
	if enabled == nil {
		enabled = []pyjson.Value{}
	}
	return selection(selectionOwner(options), enabled, pyjson.Truthy(get(options, "all_repos"))), nil
}

// without is dict.pop(key, None) on a copy.
func without(object *pyjson.Object, key string) *pyjson.Object {
	out := pyjson.NewObject()
	for _, name := range object.Keys() {
		if name == key {
			continue
		}
		value, _ := object.Get(name)
		out.Set(name, value)
	}
	return out
}

// plannerSourcesTx is _planner_sources_for_config: the integration's
// sources of the config's provider in the org, kept when their metadata
// (dict() of the stored value) names this config.
func plannerSourcesTx(ctx context.Context, tx pgx.Tx, org string, integrationID uuid.UUID, config *syncConfig) ([]sourceRow, error) {
	rows, err := tx.Query(ctx, `SELECT id, external_id, source_type, name, full_name, metadata::text, is_enabled
FROM integration_sources WHERE org_id = $1 AND integration_id = $2 AND provider = $3`, org, integrationID, config.Provider)
	if err != nil {
		return nil, fmt.Errorf("read planner sources: %w", err)
	}
	defer rows.Close()
	want := config.ID.String()
	var out []sourceRow
	for rows.Next() {
		var row sourceRow
		var metadataText *string
		if err := rows.Scan(&row.ID, &row.ExternalID, &row.SourceType, &row.Name, &row.FullName, &metadataText, &row.IsEnabled); err != nil {
			return nil, fmt.Errorf("scan planner source: %w", err)
		}
		row.Metadata, err = decodeStored(metadataText)
		if err != nil {
			return nil, err
		}
		metadata, err := plainDict(row.Metadata)
		if err != nil {
			return nil, err
		}
		if value, _ := get(metadata, "planner_managed_sync_config_id").(string); value != want {
			continue
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// integrationCredentialTx is _integration_credential_id_for_config: the
// config's integration's credential_id, in the org.
func integrationCredentialTx(ctx context.Context, tx pgx.Tx, org string, integrationID uuid.UUID) (*uuid.UUID, error) {
	var credential *uuid.UUID
	err := tx.QueryRow(ctx, `SELECT credential_id FROM integrations WHERE id = $1 AND org_id = $2`, integrationID, org).Scan(&credential)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return credential, err
}

// flushSources is the unit of work's flush of the source changes: an
// UPDATE per persistent row whose assigned values differ from its loaded
// ones (only those columns), in primary-key order, then an INSERT per new
// row in the order added, with the model's defaults (uuid4 id,
// discovered_at and last_seen_at now).
func flushSources(ctx context.Context, tx pgx.Tx, org string, integrationID uuid.UUID, existing []sourceRow, pending []desiredRow, now time.Time) error {
	ordered := append([]sourceRow(nil), existing...)
	sort.Slice(ordered, func(i, j int) bool {
		return strings.Compare(string(ordered[i].ID[:]), string(ordered[j].ID[:])) < 0
	})
	for _, source := range ordered {
		var sets []string
		var args []any
		add := func(column string, value any) {
			args = append(args, value)
			sets = append(sets, fmt.Sprintf("%s = $%d", column, len(args)))
		}
		if source.newSourceType != nil && *source.newSourceType != source.SourceType {
			add("source_type", *source.newSourceType)
		}
		if source.newName != nil && *source.newName != source.Name {
			add("name", *source.newName)
		}
		if source.newFullName != nil && *source.newFullName != source.FullName {
			add("full_name", *source.newFullName)
		}
		if source.metadataSet && !pyjson.Equal(source.newMetadata, source.Metadata) {
			text, err := pyjson.Dumps(source.newMetadata)
			if err != nil {
				return err
			}
			args = append(args, text)
			sets = append(sets, fmt.Sprintf("metadata = $%d::json", len(args)))
		}
		if source.newEnabled != nil && *source.newEnabled != source.IsEnabled {
			add("is_enabled", *source.newEnabled)
		}
		if source.newLastSeen != nil {
			add("last_seen_at", *source.newLastSeen)
		}
		if len(sets) == 0 {
			continue
		}
		args = append(args, source.ID)
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE integration_sources SET %s WHERE id = $%d`, strings.Join(sets, ", "), len(args)), args...); err != nil {
			return fmt.Errorf("update planner source: %w", err)
		}
	}
	for _, row := range pending {
		metadata, err := pyjson.Dumps(row.Metadata)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id,
name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::json, true, $10, $10)`,
			uuid.New(), org, integrationID, row.Provider, row.SourceType, row.ExternalID, row.Name, row.FullName, metadata, now); err != nil {
			return fmt.Errorf("insert planner source: %w", err)
		}
	}
	return nil
}

// plannerSourceRows is _planner_source_rows for the repository selection's
// SyncConfigBatchCreate: one row per repos entry, duplicates included.
func plannerSourceRows(config *syncConfig, options *pyjson.Object, repos []string, gitlab map[string]gitlabProject, _ string) ([]desiredRow, error) {
	provider := pythonparity.Lower(config.Provider)
	ownerValue := firstTruthy(get(options, "owner"), get(options, "group"))
	owner := config.Name
	if pyjson.Truthy(ownerValue) {
		owner = pyjson.Str(ownerValue)
	}
	configID := config.ID.String()
	var out []desiredRow
	for _, repo := range repos {
		row := desiredRow{Provider: config.Provider, Metadata: pyjson.NewObject()}
		if provider == "gitlab" {
			project := gitlab[repo]
			row.SourceType = "project"
			row.ExternalID = project.ID.String()
			row.Name = project.ID.String()
			if project.FullName != "" {
				parts := strings.Split(project.FullName, "/")
				row.Name = parts[len(parts)-1]
			}
			row.FullName = project.FullName
			row.Metadata.Set("path_with_namespace", project.FullName)
			row.Metadata.Set("planner_managed_sync_config_id", configID)
		} else {
			row.SourceType = "source"
			if provider == "github" {
				row.SourceType = "repository"
			}
			sourceOwner := owner
			if provider == "github" && strings.Contains(repo, "/") {
				parts := strings.SplitN(repo, "/", 2)
				sourceOwner, row.Name, row.FullName = parts[0], parts[1], repo
			} else {
				row.Name = repo
				row.FullName = repo
				if provider == "github" {
					row.FullName = owner + "/" + repo
				}
			}
			row.ExternalID = row.FullName
			row.Metadata.Set("planner_managed_sync_config_id", configID)
			if provider == "github" {
				row.Metadata.Set("owner", sourceOwner)
			}
		}
		out = append(out, row)
	}
	return out, nil
}

// gitlabProject is one resolved batch entry: (project_id, child_name).
type gitlabProject struct {
	ID       *big.Int
	FullName string
}

const defaultGitLabURL = "https://gitlab.com"

// resolveGitLabProjects is _resolve_gitlab_batch_projects for the
// repository selection's batch payload: repos entries to (project_id,
// child_name), listing the group's projects through the stored credential
// when a name needs it (or numeric entries could shadow names), plus the
// effective gitlab_url.
func (h *handlers) resolveGitLabProjects(ctx context.Context, tx pgx.Tx, org string, credentialID *string, options *pyjson.Object, repos []string) (map[string]gitlabProject, string, error) {
	var entries []string
	seen := map[string]bool{}
	for _, repo := range repos {
		if !seen[repo] {
			seen[repo] = true
			entries = append(entries, repo)
		}
	}
	var named, numeric []string
	for _, entry := range entries {
		if isDigitString(pythonparity.Strip(entry)) {
			numeric = append(numeric, entry)
		} else {
			named = append(named, entry)
		}
	}
	group := gitlabGroup(options)
	if len(named) > 0 {
		if credentialID == nil || *credentialID == "" {
			return nil, "", refuse(http.StatusBadRequest, "GitLab batch create requires credential_id to resolve project names to project ids")
		}
		if group == "" {
			return nil, "", refuse(http.StatusBadRequest, "GitLab batch create requires a group (sync_options.group or sync_options.owner) to resolve project names")
		}
	}
	optionURL := ""
	if value := get(options, "gitlab_url"); pyjson.Truthy(value) {
		optionURL = pythonparity.Strip(pyjson.Str(value))
	}
	gitlabURL := optionURL
	if gitlabURL == "" {
		gitlabURL = defaultGitLabURL
	}
	var token pyjson.Value
	if credentialID != nil && *credentialID != "" {
		decrypted, credentialConfig, found, err := h.decryptedCredential(ctx, tx, org, *credentialID)
		if err != nil {
			return nil, "", err
		}
		if !found {
			return nil, "", refuse(http.StatusBadRequest, "Credential not found")
		}
		decryptedObject, ok := decrypted.(*pyjson.Object)
		if !ok {
			return nil, "", fmt.Errorf("%w: decrypted credentials %T has no get()", errUnrenderable, decrypted)
		}
		token = get(decryptedObject, "token")
		configValue := credentialConfig
		if !pyjson.Truthy(configValue) {
			configValue = pyjson.NewObject()
		}
		configObject, ok := configValue.(*pyjson.Object)
		if !ok {
			return nil, "", fmt.Errorf("%w: credential config %T has no get()", errUnrenderable, configValue)
		}
		switch {
		case optionURL != "":
			gitlabURL = optionURL
		case pyjson.Truthy(get(decryptedObject, "url")):
			gitlabURL = pyjson.Str(get(decryptedObject, "url"))
		case pyjson.Truthy(get(configObject, "url")):
			gitlabURL = pyjson.Str(get(configObject, "url"))
		default:
			gitlabURL = defaultGitLabURL
		}
	}
	if len(named) > 0 && !pyjson.Truthy(token) {
		return nil, "", refuse(http.StatusBadRequest, "GitLab credential missing token")
	}
	resolved := map[string]gitlabProject{}
	byID := map[string]string{}
	shouldList := len(named) > 0 || (len(numeric) > 0 && pyjson.Truthy(token) && group != "")
	if shouldList {
		client := gitlabcode.Client{BaseURL: gitlabURL, Token: pyjson.Str(token), HTTP: h.gitlabHTTP}
		projects, err := client.ListGroupProjects(ctx, group)
		if err != nil {
			var clientErr *gitlabcode.Error
			if len(named) > 0 && errors.As(err, &clientErr) {
				return nil, "", refuse(http.StatusBadRequest, fmt.Sprintf("Failed to list GitLab projects for group '%s': %s", group, clientErr.Message))
			}
			if len(named) > 0 {
				return nil, "", err
			}
			h.logger.WarnContext(ctx, "GitLab batch create: failed to list projects for group to cross-check numeric entries; treating them as project ids",
				slog.String("group", group), slog.String("error", err.Error()))
			projects = nil
		}
		byKey := map[string][]gitlabProject{}
		for _, project := range projects {
			childName := project.FullName
			if childName == "" {
				childName = project.Name
			}
			entry := gitlabProject{ID: project.ID, FullName: childName}
			byID[project.ID.String()] = childName
			keys := []string{project.Name}
			if project.FullName != project.Name {
				keys = append(keys, project.FullName)
			}
			for _, key := range keys {
				if key != "" {
					byKey[key] = append(byKey[key], entry)
				}
			}
		}
		namedLike := append([]string(nil), named...)
		for _, entry := range numeric {
			if _, ok := byKey[entry]; ok {
				namedLike = append(namedLike, entry)
			}
		}
		var missing, ambiguous []string
		for _, entry := range named {
			if _, ok := byKey[entry]; !ok {
				missing = append(missing, entry)
			}
		}
		for _, entry := range namedLike {
			if len(byKey[entry]) > 1 {
				ambiguous = append(ambiguous, entry)
			}
		}
		if len(missing) > 0 || len(ambiguous) > 0 {
			var parts []string
			if len(missing) > 0 {
				parts = append(parts, fmt.Sprintf("not found in group '%s': %s", group, strings.Join(missing, ", ")))
			}
			if len(ambiguous) > 0 {
				parts = append(parts, "ambiguous (multiple projects share this name, use the full path): "+strings.Join(ambiguous, ", "))
			}
			return nil, "", refuse(http.StatusBadRequest, "Could not resolve GitLab projects — "+strings.Join(parts, "; "))
		}
		for _, entry := range namedLike {
			resolved[entry] = byKey[entry][0]
		}
	}
	for _, entry := range entries {
		if _, ok := resolved[entry]; ok {
			continue
		}
		projectID, err := pythonparity.ParseInt(pythonparity.Strip(entry))
		if err != nil {
			return nil, "", fmt.Errorf("%w: int(%q): %v", errUnrenderable, entry, err)
		}
		childName, ok := byID[projectID.String()]
		if !ok {
			childName = projectID.String()
			if group != "" {
				childName = group + "/" + projectID.String()
			}
		}
		resolved[entry] = gitlabProject{ID: projectID, FullName: childName}
	}
	return resolved, gitlabURL, nil
}

// isDigitString is str.isdigit(): non-empty, every character a Unicode
// digit.
func isDigitString(text string) bool {
	if text == "" {
		return false
	}
	for _, r := range text {
		if !pythonparity.IsDigit(r) {
			return false
		}
	}
	return true
}

// gitlabGroup is _gitlab_group_from_options: str(group or owner or ""),
// CR and LF removed, stripped.
func gitlabGroup(options *pyjson.Object) string {
	value := firstTruthy(get(options, "group"), get(options, "owner"))
	text := ""
	if pyjson.Truthy(value) {
		text = pyjson.Str(value)
	}
	text = strings.NewReplacer("\r", "", "\n", "").Replace(text)
	return pythonparity.Strip(text)
}

// decryptedCredential is IntegrationCredentialsService(session,
// org).get_decrypted_credentials_by_id: the org's credential by id (an id
// uuid.UUID() refuses is none); found is false for none, an empty
// payload, or a payload that does not decrypt or parse as JSON.
func (h *handlers) decryptedCredential(ctx context.Context, tx pgx.Tx, org, credentialID string) (decrypted, config pyjson.Value, found bool, err error) {
	id, parseErr := pythonparity.ParseUUID(credentialID)
	if parseErr != nil {
		return nil, nil, false, nil
	}
	var encrypted, configText *string
	err = tx.QueryRow(ctx, `SELECT credentials_encrypted, config::text FROM integration_credentials WHERE org_id = $1 AND id = $2`, org, id).
		Scan(&encrypted, &configText)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("read credential: %w", err)
	}
	config, err = decodeStored(configText)
	if err != nil {
		return nil, nil, false, err
	}
	if encrypted == nil || *encrypted == "" || h.decryptor == nil {
		return nil, config, false, nil
	}
	plain, err := h.decryptor.Decrypt(secrets.NewValue(*encrypted))
	if err != nil || !utf8.Valid(plain) {
		return nil, config, false, nil
	}
	decrypted, err = pyjson.DecodeString(string(plain))
	if err != nil {
		return nil, config, false, nil
	}
	return decrypted, config, true, nil
}
