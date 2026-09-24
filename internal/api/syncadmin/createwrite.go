package syncadmin

import (
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The planner-managed sync config create path's pure pieces
// (_create_planner_managed_config and its helpers), shared by create and
// batch create.

// errGitHubWorkItemOption is canonical_github_work_item_runtime_options'
// ValueError: an explicit option of the wrong type. The create route does
// not catch it (a bare 500), as Python does not.
type errGitHubWorkItemOption struct{ message string }

func (e errGitHubWorkItemOption) Error() string { return e.message }

// canonicalGitHubWorkItemRuntimeOptions is
// providers/github/work_item_options.canonical_github_work_item_runtime_options:
// the defaults (fetch_comments True, fetch_milestones True, comments_limit
// 500) overridden by every explicit, non-None value; a non-bool fetch flag,
// or a comments_limit that is a bool, not an int, or negative, raises.
func canonicalGitHubWorkItemRuntimeOptions(options *pyjson.Object) (*pyjson.Object, error) {
	result := pyjson.NewObject()
	result.Set("fetch_comments", true)
	result.Set("fetch_milestones", true)
	result.Set("comments_limit", pyjson.IntOf(500))
	if options == nil {
		return result, nil
	}
	for _, name := range []string{"fetch_comments", "fetch_milestones"} {
		value, _ := options.Get(name)
		if value == nil {
			continue
		}
		flag, ok := value.(bool)
		if !ok {
			return nil, errGitHubWorkItemOption{fmt.Sprintf("GitHub work-items %s must be a boolean", name)}
		}
		result.Set(name, flag)
	}
	if limit, _ := options.Get("comments_limit"); limit != nil {
		count, ok := limit.(pyjson.Int)
		if !ok || count.Int == nil || count.Sign() < 0 {
			return nil, errGitHubWorkItemOption{"GitHub work-items comments_limit must be non-negative"}
		}
		result.Set("comments_limit", count)
	}
	return result, nil
}

// snapshotGitHubWorkItemRuntimeOptions is
// snapshot_github_work_item_runtime_options: a missing or None option is
// read from the legacy environment (GITHUB_FETCH_COMMENTS,
// GITHUB_FETCH_MILESTONES as env_flag, GITHUB_COMMENTS_LIMIT as env_int),
// then canonical_github_work_item_runtime_options holds the result.
func snapshotGitHubWorkItemRuntimeOptions(options *pyjson.Object, lookupEnv func(string) (string, bool)) (*pyjson.Object, error) {
	source := pyjson.NewObject()
	if options != nil {
		for _, key := range options.Keys() {
			value, _ := options.Get(key)
			source.Set(key, value)
		}
	}
	if value, _ := source.Get("fetch_comments"); value == nil {
		source.Set("fetch_comments", envFlag(lookupEnv, "GITHUB_FETCH_COMMENTS", true))
	}
	if value, _ := source.Get("fetch_milestones"); value == nil {
		source.Set("fetch_milestones", envFlag(lookupEnv, "GITHUB_FETCH_MILESTONES", true))
	}
	if value, _ := source.Get("comments_limit"); value == nil {
		source.Set("comments_limit", envInt(lookupEnv, "GITHUB_COMMENTS_LIMIT", 500))
	}
	return canonicalGitHubWorkItemRuntimeOptions(source)
}

// envFlag is providers/utils.env_flag: the stripped, lower-cased value in
// {1, true, yes, on} is True, in {0, false, no, off} is False, anything
// else (or unset) the default.
func envFlag(lookupEnv func(string) (string, bool), name string, fallback bool) bool {
	raw, ok := lookupEnv(name)
	if !ok {
		return fallback
	}
	switch pythonparity.Lower(pythonparity.Strip(raw)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return fallback
}

// envInt is providers/utils.env_int: unset or empty is the default; else
// int(raw), falling back to the default when int() refuses it.
func envInt(lookupEnv func(string) (string, bool), name string, fallback int64) pyjson.Int {
	raw, ok := lookupEnv(name)
	if !ok || raw == "" {
		return pyjson.IntOf(fallback)
	}
	value, err := pythonparity.ParseInt(raw)
	if err != nil {
		return pyjson.IntOf(fallback)
	}
	return pyjson.Int{Int: value}
}

// plannerParentOptions is _create_planner_managed_config's parent options:
// for a GitHub config (provider.lower()), the options with the snapshot's
// three keys merged in ({**parent_options, **snapshot}: an existing key
// keeps its place, a new one is appended).
func plannerParentOptions(provider string, options *pyjson.Object, lookupEnv func(string) (string, bool)) (*pyjson.Object, error) {
	if pythonparity.Lower(provider) != "github" {
		return options, nil
	}
	snapshot, err := snapshotGitHubWorkItemRuntimeOptions(options, lookupEnv)
	if err != nil {
		return nil, err
	}
	merged := pyjson.NewObject()
	for _, key := range options.Keys() {
		value, _ := options.Get(key)
		merged.Set(key, value)
	}
	for _, key := range snapshot.Keys() {
		value, _ := snapshot.Get(key)
		merged.Set(key, value)
	}
	return merged, nil
}

// plannerDatasetOptions is _planner_dataset_options: {"legacy_targets":
// the selection}, plus a dict service_repository_mappings on PagerDuty's
// services dataset, plus the canonical work-item runtime options on
// GitHub's work-items dataset. The provider is compared as given (not
// lower-cased), as Python does.
func plannerDatasetOptions(provider, datasetKey string, syncTargets []string, parentOptions *pyjson.Object) (*pyjson.Object, error) {
	options := pyjson.NewObject()
	targets := make([]pyjson.Value, len(syncTargets))
	for index, target := range syncTargets {
		targets[index] = target
	}
	options.Set("legacy_targets", targets)
	if mappings, _ := parentOptions.Get("service_repository_mappings"); provider == "pagerduty" && datasetKey == "services" {
		if object, ok := mappings.(*pyjson.Object); ok {
			options.Set("service_repository_mappings", object)
		}
	}
	if provider == "github" && datasetKey == "work-items" {
		canonical, err := canonicalGitHubWorkItemRuntimeOptions(parentOptions)
		if err != nil {
			return nil, err
		}
		for _, key := range canonical.Keys() {
			value, _ := canonical.Get(key)
			options.Set(key, value)
		}
	}
	return options, nil
}

// plannerDatasetKeys is _planner_dataset_keys (sync/datasets.py's
// planner_dataset_keys); a PagerDuty selection other than {"operational"}
// is its ValueError, a bare 500 in the create route.
func plannerDatasetKeys(provider string, syncTargets []string) ([]string, error) {
	return providersync.PlannerDatasetKeys(provider, syncTargets)
}

// nonGitExplicitSourceID is _non_git_explicit_source_id: the first of
// project_id, project_key, team_id and repo that is neither None nor a
// whitespace-only string (any JSON value), else nil.
func nonGitExplicitSourceID(options *pyjson.Object) pyjson.Value {
	for _, key := range []string{"project_id", "project_key", "team_id", "repo"} {
		value, _ := options.Get(key)
		if value == nil {
			continue
		}
		if text, ok := value.(string); ok && pythonparity.Strip(text) == "" {
			continue
		}
		return value
	}
	return nil
}

// jiraConfigMaterializesZeroSources is
// _jira_config_materializes_zero_sources: a Jira config (provider.lower())
// without a truthy explicit scope.
func jiraConfigMaterializesZeroSources(provider string, options *pyjson.Object) bool {
	return pythonparity.Lower(provider) == "jira" && !pyjson.Truthy(nonGitExplicitSourceID(options))
}

// newSourceRow is one IntegrationSource the create path adds.
type newSourceRow struct {
	provider, sourceType, externalID, name, fullName string
	metadata                                         *pyjson.Object
}

// nonGitSourceRows is _non_git_source_rows: none for a Jira config without
// explicit scope; else one enabled source keyed on the explicit id (str()
// of it), or for Linear without one the provider name as an org-wide
// placeholder, or else the config name. Jira and Linear sources are
// projects; others are sources. The metadata names the config, and marks
// the Linear placeholder and an explicitly scoped Jira project.
func nonGitSourceRows(provider string, options *pyjson.Object, name, configID string) []newSourceRow {
	explicit := nonGitExplicitSourceID(options)
	lower := pythonparity.Lower(provider)
	linearOrgWide := lower == "linear" && !pyjson.Truthy(explicit)
	if jiraConfigMaterializesZeroSources(provider, options) {
		return nil
	}
	var externalID string
	switch {
	case pyjson.Truthy(explicit):
		externalID = pyjson.Str(explicit)
	case linearOrgWide:
		externalID = lower
	default:
		externalID = name
	}
	sourceType := "source"
	if lower == "jira" || lower == "linear" {
		sourceType = "project"
	}
	fullName := externalID
	if value, _ := options.Get("full_name"); pyjson.Truthy(value) {
		fullName = pyjson.Str(value)
	}
	metadata := pyjson.NewObject()
	metadata.Set("planner_managed_sync_config_id", configID)
	if linearOrgWide {
		metadata.Set("org_wide_placeholder", true)
	}
	if lower == "jira" && pyjson.Truthy(explicit) {
		metadata.Set("explicit_project_scope", true)
	}
	return []newSourceRow{{provider: provider, sourceType: sourceType, externalID: externalID, name: name, fullName: fullName, metadata: metadata}}
}

// errPagerDutyIdentity is PagerDutyOperationalTargetError from
// pagerduty_provider_instance_id.
var errPagerDutyIdentity = errors.New("PagerDuty credential account identity is invalid")

// errPagerDutyConfigNotMapping is the AttributeError of `config.get` on a
// truthy credential config that is not a dict: the repair does not catch
// it, so the create route answers a bare 500.
var errPagerDutyConfigNotMapping = errors.New("pagerduty_provider_instance_id: credential config is not a dict")

// pagerDutyProviderInstanceID is sync/pagerduty_repair's
// pagerduty_provider_instance_id: the credential config's stripped
// account_id, when both account_id and subdomain are non-empty strings
// (`config or {}`: a falsy config has neither).
func pagerDutyProviderInstanceID(config pyjson.Value) (string, error) {
	object, isObject := config.(*pyjson.Object)
	if !isObject && pyjson.Truthy(config) {
		return "", errPagerDutyConfigNotMapping
	}
	component := func(key string) (string, bool) {
		if object == nil {
			return "", false
		}
		value, _ := object.Get(key)
		text, ok := value.(string)
		if !ok {
			return "", false
		}
		text = pythonparity.Strip(text)
		return text, text != ""
	}
	accountID, hasAccount := component("account_id")
	_, hasSubdomain := component("subdomain")
	if !hasAccount || !hasSubdomain {
		return "", errPagerDutyIdentity
	}
	return accountID, nil
}
