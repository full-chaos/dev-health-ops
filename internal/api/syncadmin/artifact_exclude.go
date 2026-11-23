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

// createArtifactExclude validates the key of a create's options and takes it out of them: the list is the
// integration's (it applies to every sync config of this integration), written by createArtifactExcludeWrite once the
// configuration and its integration exist, never into sync_options.
func createArtifactExclude(provider string, options *pyjson.Object) (*artifactexclude.Change, error) {
	change, err := artifactExcludeChange(provider, options)
	if err != nil {
		return nil, err
	}
	options.Delete(artifactexclude.Key)
	return change, nil
}

// createArtifactExcludeWrite writes a create's list into the new configuration's integration (artifactexclude.WriteTx).
func createArtifactExcludeWrite(ctx context.Context, tx pgx.Tx, org string, configID uuid.UUID, change *artifactexclude.Change, now time.Time) error {
	if change == nil || change.Clear {
		return nil
	}
	if _, err := artifactexclude.WriteTx(ctx, tx, org, configID, *change, now); err != nil {
		return artifactExcludeAnswer(err)
	}
	return nil
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

// deriveArtifactExclude sets, on every config with an integration, the integration's exclude list its response shows
// in sync_options (artifactexclude.Overlay). One read serves all the configs; a config without an integration shows
// its stored options as they are.
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
		config.artifactExclude, config.artifactExcludePresent, config.artifactExcludeDerived = value, present, true
	}
	return nil
}
