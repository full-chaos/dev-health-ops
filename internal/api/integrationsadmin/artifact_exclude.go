package integrationsadmin

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
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

// keepArtifactExcludeAndDecode is the update's config step under the lock order every writer of an integration's config
// keeps: the integration's selection lock, then the integration row (FOR NO KEY UPDATE, the lock the UPDATE takes
// anyway), read fresh under the lock, so a list another writer committed meanwhile is the one kept. The org-scoped
// existence check stays requireIntegration, before this step, unchanged. Then keepArtifactExclude, then the stored
// config decoded for the update's compare.
func keepArtifactExcludeAndDecode(ctx context.Context, tx configLockTx, orgID string, current *integration, next *pyjson.Object) (pyjson.Value, error) {
	if err := providersync.LockIntegrationSelection(ctx, tx, orgID, current.ID); err != nil {
		return nil, err
	}
	var fresh string
	err := tx.QueryRow(ctx, `SELECT config::text FROM public.integrations WHERE id=$1 AND org_id=$2 FOR NO KEY UPDATE`,
		current.ID, orgID).Scan(&fresh)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, refuse(http.StatusNotFound, "Integration not found")
	}
	if err != nil {
		return nil, fmt.Errorf("lock integration config: %w", err)
	}
	current.Config = fresh
	if err := keepArtifactExclude(current, next); err != nil {
		return nil, err
	}
	return pyjson.DecodeString(current.Config)
}

// configLockTx is what the config step needs of the update's transaction.
type configLockTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// createConfigText is the create's config: the CI artifact exclude list validated and cleared by the same rule as the
// update (keepArtifactExclude, artifactexclude.Parse for the provider; nothing is stored yet), then dumped.
func createConfigText(provider string, config *pyjson.Object) (string, error) {
	if err := keepArtifactExclude(&integration{Provider: provider, Config: "{}"}, config); err != nil {
		return "", err
	}
	return dumps(config)
}
