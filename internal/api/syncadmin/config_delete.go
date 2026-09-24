package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// writer is the sync admin routes' writes over the api role's pool.
type writer interface {
	deleteConfig(ctx context.Context, orgID string, config *syncConfig) error
}

var _ writer = store{}

// errDeleteWithChildren is the Python delete's refusal of a config that has
// child configs. SyncConfigurationService.delete runs session.delete, whose
// children cascade (cascade="all, delete-orphan") loads the children and
// reaches each child's parent relationship, declared lazy="raise": SQLAlchemy
// raises InvalidRequestError ("'SyncConfiguration.parent' is not available
// due to lazy='raise'"), the api answers its unhandled 500, and the
// transaction rolls back with nothing deleted. The children relationship
// joins on parent_id alone, so a child in another org counts too.
var errDeleteWithChildren = errors.New("sync config has child configs: the Python ORM delete raises on the lazy='raise' parent relationship")

// deleteSyncConfig is sync.py's delete_sync_config: the org's config by id
// (SyncConfigurationService.get_by_id: an id uuid.UUID() refuses, or none
// in the org, is 404), then SyncConfigurationService.delete by its name
// and provider, and FastAPI's empty 204 (the default JSONResponse class,
// so it carries Content-Type: application/json and no body).
func (h *handlers) deleteSyncConfig(w http.ResponseWriter, r *http.Request) {
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	if err := h.writes.deleteConfig(r.Context(), orgID(r), config); err != nil {
		h.fail(w, r, "delete_config", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// deleteConfig is SyncConfigurationService.delete for config, in one
// transaction, as Python runs it: the org's config named (name, provider)
// is looked up again (uq_sync_config_org_provider_name makes it one row,
// which is config unless a concurrent request replaced it); none found is
// not an error (the Python delete returns False and the route still
// answers 204); errDeleteWithChildren when any config names that row as
// parent; else that row is deleted. Every foreign key to
// sync_configurations is ON DELETE CASCADE or SET NULL, so one DELETE
// leaves the rows the ORM delete leaves.
func (s store) deleteConfig(ctx context.Context, orgID string, config *syncConfig) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return deleteConfigTx(ctx, tx, orgID, config.Name, config.Provider)
	})
}

func deleteConfigTx(ctx context.Context, tx pgx.Tx, orgID, name, provider string) error {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM sync_configurations WHERE org_id = $1 AND name = $2 AND provider = $3`, orgID, name, provider).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read config by name: %w", err)
	}
	var hasChildren bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sync_configurations WHERE parent_id = $1)`, id).Scan(&hasChildren); err != nil {
		return fmt.Errorf("read child configs: %w", err)
	}
	if hasChildren {
		return errDeleteWithChildren
	}
	_, err = tx.Exec(ctx, `DELETE FROM sync_configurations WHERE id = $1`, id)
	return err
}
