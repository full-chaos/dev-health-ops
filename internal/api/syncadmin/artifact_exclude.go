package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
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

// takeArtifactExclude validates the key of a create's or a PATCH's options and takes it out of them: the list is the
// integration's (it applies to every sync config of this integration), written by writeArtifactExclude once the
// configuration and its integration exist, never into sync_options. nil when the options do not carry the key.
func takeArtifactExclude(provider string, options *pyjson.Object) (*artifactexclude.Change, error) {
	change, err := artifactExcludeChange(provider, options)
	if err != nil {
		return nil, err
	}
	options.Delete(artifactexclude.Key)
	return change, nil
}

// listOnlyPatch: the PATCH changes ci_artifact_exclude_prefixes and nothing else (its body is only sync_options holding
// only that key). It is a list edit: it skips the GitHub work-item runtime refresh, so it writes what the dho verb
// writes. Any other PATCH keeps the refresh.
func listOnlyPatch(in syncConfigUpdate) bool {
	if in.syncOptions == nil || len(in.provided) != 1 || !in.provided["sync_options"] || in.syncOptions.Len() != 1 {
		return false
	}
	_, present := in.syncOptions.Get(artifactexclude.Key)
	return present
}

// writeArtifactExclude writes a create's or a PATCH's list into the configuration's integration (artifactexclude.WriteTx,
// the writer of the dho verb too); config takes the stored sync_options when the write removed an old copy from them.
func writeArtifactExclude(ctx context.Context, tx pgx.Tx, org string, config *syncConfig, change *artifactexclude.Change, now time.Time) error {
	if change == nil {
		return nil
	}
	written, err := artifactexclude.WriteTx(ctx, tx, selectionLock, org, config.ID, *change, now)
	if err != nil {
		return artifactExcludeAnswer(err)
	}
	if written.SyncOptionsChanged {
		config.SyncOptions = &written.SyncOptions
		config.UpdatedAt = now
	}
	return nil
}

// selectionLock is providersync.LockIntegrationSelection as artifactexclude.WriteTx takes it: the first lock of every
// writer of an integration's config (the PATCH already holds it; it is re-entrant in one transaction).
func selectionLock(ctx context.Context, tx pgx.Tx, org string, integrationID uuid.UUID) error {
	return providersync.LockIntegrationSelection(ctx, tx, org, integrationID)
}

// excludeListReader is the read deriveShown makes for the list; store has it (below), and a test reader may.
type excludeListReader interface {
	integrationExcludeLists(ctx context.Context, orgID string, integrationIDs []uuid.UUID) (map[uuid.UUID]*string, error)
}

var _ excludeListReader = store{}

// deriveShown derives what a rendered configuration shows from its integration: the targets (deriveShownTargets) and
// the CI artifact exclude list (deriveArtifactExclude). step names the read that failed.
func (h *handlers) deriveShown(ctx context.Context, org string, configs ...*syncConfig) (string, error) {
	if err := deriveShownTargets(ctx, h.store.enabledDatasetKeys, org, configs...); err != nil {
		return "enabled_datasets", err
	}
	if lists, ok := h.store.(excludeListReader); ok {
		if err := deriveArtifactExclude(ctx, lists.integrationExcludeLists, org, configs...); err != nil {
			return "artifact_exclude", err
		}
	}
	return "", nil
}

// integrationExcludeLists reads the config text of each integration (one read for all), for deriveArtifactExclude.
func integrationExcludeLists(ctx context.Context, db rowQuerier, orgID string, integrationIDs []uuid.UUID) (map[uuid.UUID]*string, error) {
	found := map[uuid.UUID]*string{}
	if len(integrationIDs) == 0 {
		return found, nil
	}
	rows, err := db.Query(ctx, `SELECT id, config::text FROM integrations WHERE org_id = $1 AND id = ANY($2)`, orgID, integrationIDs)
	if err != nil {
		return nil, fmt.Errorf("read integration configs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var text *string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, fmt.Errorf("read integration config: %w", err)
		}
		found[id] = text
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read integration configs: %w", err)
	}
	return found, nil
}

func (s store) integrationExcludeLists(ctx context.Context, orgID string, integrationIDs []uuid.UUID) (map[uuid.UUID]*string, error) {
	return integrationExcludeLists(ctx, s.pool, orgID, integrationIDs)
}

// deriveArtifactExclude puts, into the sync_options every config with an integration shows, its integration's exclude
// list (artifactexclude.OverlayText): one read serves all the configs. Options that hold no list and whose integration
// holds none stay the stored text, byte for byte; a config without an integration shows its stored options as they are.
func deriveArtifactExclude(ctx context.Context, read func(context.Context, string, []uuid.UUID) (map[uuid.UUID]*string, error),
	orgID string, configs ...*syncConfig) error {
	var integrationIDs []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for _, config := range configs {
		if config.IntegrationID != nil && !seen[*config.IntegrationID] {
			seen[*config.IntegrationID] = true
			integrationIDs = append(integrationIDs, *config.IntegrationID)
		}
	}
	if len(integrationIDs) == 0 {
		return nil
	}
	texts, err := read(ctx, orgID, integrationIDs)
	if err != nil {
		return err
	}
	for _, config := range configs {
		if config.IntegrationID == nil {
			continue
		}
		value, present, err := artifactexclude.Stored(texts[*config.IntegrationID])
		if err != nil {
			return fmt.Errorf("decode integration config: %w", err)
		}
		shown, changed, err := artifactexclude.OverlayText(config.SyncOptions, value, present)
		if err != nil {
			return fmt.Errorf("show the integration's list: %w", err)
		}
		if changed {
			config.SyncOptions = &shown
		}
	}
	return nil
}
