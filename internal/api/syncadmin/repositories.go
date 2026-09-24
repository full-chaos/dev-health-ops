package syncadmin

import (
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// getRepositories is get_sync_config_repositories:
// _repository_selection_for_config. The planner-managed sources win; else
// the legacy child configs' repos; else the config's own single repo.
func (h *handlers) getRepositories(w http.ResponseWriter, r *http.Request) {
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	org := orgID(r)
	optionsValue, err := decodeStored(config.SyncOptions)
	if err != nil {
		h.fail(w, r, "decode_options", err)
		return
	}
	// Each read of sync_options below is its own dict(...) call in Python;
	// one conversion answers all of them.
	options, err := plainDict(optionsValue)
	if err != nil {
		h.fail(w, r, "render_options", err)
		return
	}
	owner := selectionOwner(options)
	allRepos := pyjson.Truthy(get(options, "all_repos"))

	if config.IntegrationID != nil {
		sources, err := h.store.sourcesForIntegration(ctx, org, *config.IntegrationID, config.Provider)
		if err != nil {
			h.fail(w, r, "planner_sources", err)
			return
		}
		want := config.ID.String()
		var enabled []pyjson.Value
		matched := 0
		for _, source := range sources {
			metadataValue, err := decodeStored(source.Metadata)
			if err != nil {
				h.fail(w, r, "decode_source_metadata", err)
				return
			}
			metadata, err := plainDict(metadataValue)
			if err != nil {
				h.fail(w, r, "render_source_metadata", err)
				return
			}
			if value, _ := get(metadata, "planner_managed_sync_config_id").(string); value != want {
				continue
			}
			matched++
			if source.IsEnabled {
				enabled = append(enabled, source.FullName)
			}
		}
		if matched > 0 {
			policy.WriteModel(w, http.StatusOK, selection(owner, enabled, allRepos), nil)
			return
		}
	}

	childOptions, err := h.store.childOptions(ctx, org, config.ID)
	if err != nil {
		h.fail(w, r, "legacy_children", err)
		return
	}
	var repos []pyjson.Value
	for _, text := range childOptions {
		childValue, err := decodeStored(text)
		if err != nil {
			h.fail(w, r, "decode_child_options", err)
			return
		}
		child, err := plainDict(childValue)
		if err != nil {
			h.fail(w, r, "render_child_options", err)
			return
		}
		repo := firstTruthy(get(child, "repo"), get(child, "project_id"))
		if repo == nil {
			continue
		}
		childOwner := firstTruthy(get(child, "owner"), get(child, "group"))
		repoText := pyjson.Str(repo)
		if pyjson.Truthy(childOwner) && !strings.Contains(repoText, "/") {
			repos = append(repos, pyjson.Str(childOwner)+"/"+repoText)
		} else {
			repos = append(repos, repoText)
		}
	}
	if len(repos) == 0 {
		if repo := get(options, "repo"); repo != nil {
			repoText := pyjson.Str(repo)
			if owner != "" && !strings.Contains(repoText, "/") {
				repos = []pyjson.Value{owner + "/" + repoText}
			} else {
				repos = []pyjson.Value{repoText}
			}
		}
	}
	policy.WriteModel(w, http.StatusOK, selection(owner, repos, allRepos), nil)
}

// selectionOwner is _repo_selection_owner:
// str(owner or group or "").
func selectionOwner(options *pyjson.Object) string {
	value := firstTruthy(get(options, "owner"), get(options, "group"))
	if !pyjson.Truthy(value) {
		return ""
	}
	return pyjson.Str(value)
}

// selection is SyncConfigRepositorySelection.
func selection(owner string, repos []pyjson.Value, allRepos bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("owner", owner)
	out.Set("repos", repos)
	out.Set("sync_all_repos", allRepos)
	return out
}

// get is dict.get(key): None when absent.
func get(object *pyjson.Object, key string) pyjson.Value {
	value, _ := object.Get(key)
	return value
}

// firstTruthy is `a or b`: a when truthy, else b.
func firstTruthy(first, second pyjson.Value) pyjson.Value {
	if pyjson.Truthy(first) {
		return first
	}
	return second
}
