package integrationsadmin

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
)

// keepArtifactExclude is the update's rule for the CI artifact exclude list: the integration's config is the one store
// of the list (it applies to every sync config of this integration) and the update replaces the config whole, so a new
// config without the key keeps the stored list; one that carries the key is validated (a refusal is a 422 that names
// no value) and null or [] removes it.
func keepArtifactExclude(current *integration, next *pyjson.Object) error {
	if err := artifactexclude.KeepOnReplace(current.Provider, &current.Config, next); err != nil {
		var refused *artifactexclude.Refusal
		if errors.As(err, &refused) {
			return refuse(http.StatusUnprocessableEntity, refused.Message)
		}
		return fmt.Errorf("keep artifact exclude list: %w", err)
	}
	return nil
}
