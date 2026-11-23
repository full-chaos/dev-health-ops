package syncadmin

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
)

// artifactExcludeChange validates the body's sync_options.ci_artifact_exclude_prefixes for a configuration of
// provider (artifactexclude.Parse, the validator `dho admin sync-configs artifact-excludes` uses). nil when the body
// does not carry the key.
func artifactExcludeChange(provider string, options *pyjson.Object) (*artifactexclude.Change, error) {
	value, present := options.Get(artifactexclude.Key)
	if !present {
		return nil, nil
	}
	change, err := artifactexclude.Parse(provider, value)
	if err != nil {
		return nil, artifactExcludeAnswer(err)
	}
	return &change, nil
}

// artifactExcludeAnswer is the 422 of a refused value: the message, the field and the entry index, never the value.
func artifactExcludeAnswer(err error) error {
	var refused *artifactexclude.Refusal
	if !errors.As(err, &refused) {
		return err
	}
	detail := pyjson.NewObject()
	detail.Set("message", refused.Message)
	detail.Set("field", artifactexclude.Key)
	if refused.Index >= 0 {
		detail.Set("index", pyjson.IntOf(int64(refused.Index)))
	}
	return refuse(http.StatusUnprocessableEntity, detail)
}

// createArtifactExclude validates the key of a create's options and drops it when the value clears it. The create
// writes the same options to the configuration and to its new integration (createPlannerManagedConfig), so a valid
// list lands in both as given.
func createArtifactExclude(provider string, options *pyjson.Object) error {
	change, err := artifactExcludeChange(provider, options)
	if err != nil || change == nil {
		return err
	}
	if change.Clear {
		options.Delete(artifactexclude.Key)
	}
	return nil
}
