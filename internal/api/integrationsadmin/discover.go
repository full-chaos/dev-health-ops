package integrationsadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pydict"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

const (
	discoveryErrorCode    = "integration_discovery_failed"
	discoveryErrorMessage = "Integration discovery failed"
)

// discover is POST /integrations/{integration_id}/discover
// (discover_integration_sources): run source discovery for the integration
// and answer 202 with every source the run upserted, new and updated, in
// discovery order. Any failure inside the discovery answers 503 with the
// fixed error code, and the cause only in the log.
func (h handlers) discover(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID := orgOf(r)
	raw := r.PathValue("integration_id")
	integration, id, err := requireIntegration(ctx, h.Pool, orgID, raw)
	if err != nil {
		h.fail(w, r, "get integration", err)
		return
	}
	rows, err := h.runDiscovery(ctx, orgID, *integration, id)
	if err != nil {
		h.Logger.ErrorContext(ctx, "integration_discovery.failed", slog.String("error_code", discoveryErrorCode),
			slog.String("org_id", orgID), slog.String("integration_id", id.String()), slog.String("error", err.Error()))
		detail := pyjson.NewObject()
		detail.Set("code", discoveryErrorCode)
		detail.Set("message", discoveryErrorMessage)
		policy.WriteDetail(w, http.StatusServiceUnavailable, detail, nil)
		return
	}
	sources := make([]pyjson.Value, 0, len(rows))
	for _, row := range rows {
		object, err := sourceObject(row)
		if err != nil {
			h.fail(w, r, "render source", err)
			return
		}
		sources = append(sources, object)
	}
	out := pyjson.NewObject()
	// The response echoes the path value as the caller spelled it.
	out.Set("integration_id", raw)
	out.Set("discovered", pyjson.IntOf(int64(len(sources))))
	out.Set("sources", sources)
	policy.WriteModel(w, http.StatusAccepted, out, nil)
}

// runDiscovery is discover_sources_for_integration: the provider and options
// come from the integration (a Jira integration reads its planner-managed
// sync config's current options and tags the sources it upserts with that
// config), the stored credential is the integration's, and the rows are the
// ones the run upserted.
func (h handlers) runDiscovery(ctx context.Context, orgID string, integration integration, id uuid.UUID) ([]source, error) {
	if h.Discovery == nil {
		return nil, errors.New("source discovery is unavailable in this process")
	}
	options, err := storedDict(integration.Config)
	if err != nil {
		return nil, fmt.Errorf("integration config: %w", err)
	}
	args := schedsync.SourceDiscoveryArgs{OrgID: orgID, IntegrationID: id.String(), Provider: integration.Provider}
	if integration.CredentialID != nil {
		text := integration.CredentialID.String()
		args.CredentialID = &text
	}
	if jiraKey(integration.Provider) == "jira" {
		planner, err := plannerConfig(ctx, h.Pool, id)
		if err != nil {
			return nil, err
		}
		if planner != nil {
			args.ConfigID, args.PlannerManaged = planner.id.String(), true
			options = planner.options
		}
	}
	encoded, err := pyjson.Dumps(options)
	if err != nil {
		return nil, fmt.Errorf("encode sync options: %w", err)
	}
	if err := json.Unmarshal([]byte(encoded), &args.SyncOptions); err != nil {
		return nil, fmt.Errorf("decode sync options: %w", err)
	}
	report, err := h.Discovery.Discover(ctx, args)
	if err != nil {
		return nil, err
	}
	return sourcesByID(ctx, h.Pool, orgID, report.SourceIDs)
}

// plannerConfigRow is the owning planner-managed parent sync configuration.
type plannerConfigRow struct {
	id      uuid.UUID
	options *pyjson.Object
}

// plannerConfig is _planner_managed_config_for_integration: the one parent
// planner-managed sync configuration of the integration, or nil. More than
// one is Python's MultipleResultsFound.
func plannerConfig(ctx context.Context, q querier, integrationID uuid.UUID) (*plannerConfigRow, error) {
	rows, err := q.Query(ctx, `SELECT id, sync_options::text FROM public.sync_configurations
WHERE integration_id=$1 AND planner_managed IS TRUE AND parent_id IS NULL`, integrationID)
	if err != nil {
		return nil, fmt.Errorf("read the planner-managed config: %w", err)
	}
	defer rows.Close()
	var found []plannerConfigRow
	for rows.Next() {
		var id uuid.UUID
		var text *string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, fmt.Errorf("scan the planner-managed config: %w", err)
		}
		options := pyjson.NewObject()
		if text != nil {
			value, err := pyjson.DecodeString(*text)
			if err != nil {
				return nil, fmt.Errorf("decode the planner-managed config options: %w", err)
			}
			// dict(sync_options or {}).
			if options, err = pydict.Plain(value); err != nil {
				return nil, err
			}
		}
		found = append(found, plannerConfigRow{id: id, options: options})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	switch len(found) {
	case 0:
		return nil, nil
	case 1:
		return &found[0], nil
	}
	return nil, fmt.Errorf("%d planner-managed parent configs for one integration", len(found))
}

// sourcesByID reads the rows a discovery run upserted, in the run's order (a
// row the run matched twice appears twice), as they stand after it.
func sourcesByID(ctx context.Context, q querier, orgID string, ids []string) ([]source, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := q.Query(ctx, `SELECT `+sourceColumns+` FROM public.integration_sources WHERE org_id=$1 AND id = ANY($2::uuid[])`, orgID, ids)
	if err != nil {
		return nil, fmt.Errorf("read the discovered sources: %w", err)
	}
	defer rows.Close()
	byID := map[string]source{}
	for rows.Next() {
		row, err := scanSource(rows)
		if err != nil {
			return nil, fmt.Errorf("scan a discovered source: %w", err)
		}
		byID[row.ID.String()] = row
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]source, 0, len(ids))
	for _, id := range ids {
		row, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("discovered source %s is not readable", id)
		}
		out = append(out, row)
	}
	return out, nil
}
