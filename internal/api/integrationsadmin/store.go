package integrationsadmin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// querier is what the reads need: a pool or a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// integration is an integrations row; config is the stored JSON text.
type integration struct {
	ID           uuid.UUID
	OrgID        string
	Provider     string
	CredentialID *uuid.UUID
	Name         string
	Config       string
	IsActive     bool
	ScheduleCron *string
	Timezone     *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

const integrationColumns = `id, org_id, provider, credential_id, name, config::text, is_active, schedule_cron, timezone, created_at, updated_at`

func scanIntegration(row pgx.Row) (integration, error) {
	var out integration
	err := row.Scan(&out.ID, &out.OrgID, &out.Provider, &out.CredentialID, &out.Name, &out.Config, &out.IsActive,
		&out.ScheduleCron, &out.Timezone, &out.CreatedAt, &out.UpdatedAt)
	return out, err
}

// listIntegrations is IntegrationService.list_all.
func listIntegrations(ctx context.Context, q querier, orgID string) ([]integration, error) {
	rows, err := q.Query(ctx, `SELECT `+integrationColumns+` FROM public.integrations WHERE org_id=$1 ORDER BY created_at DESC`, orgID)
	if err != nil {
		return nil, fmt.Errorf("list integrations: %w", err)
	}
	defer rows.Close()
	var out []integration
	for rows.Next() {
		item, err := scanIntegration(rows)
		if err != nil {
			return nil, fmt.Errorf("scan integration: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// getIntegration is IntegrationService.get_by_id for a parsed id (nil when
// the row is absent or belongs to another org).
func getIntegration(ctx context.Context, q querier, orgID string, id uuid.UUID, forUpdate bool) (*integration, error) {
	query := `SELECT ` + integrationColumns + ` FROM public.integrations WHERE id=$1 AND org_id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	item, err := scanIntegration(q.QueryRow(ctx, query, id, orgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get integration: %w", err)
	}
	return &item, nil
}

// credentialOwned reports whether credential id belongs to orgID and provider
// (_resolve_credential_id's query).
func credentialOwned(ctx context.Context, q querier, orgID, provider string, id uuid.UUID) (bool, error) {
	var found uuid.UUID
	err := q.QueryRow(ctx, `SELECT id FROM public.integration_credentials WHERE id=$1 AND org_id=$2 AND provider=$3`, id, orgID, provider).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check credential: %w", err)
	}
	return true, nil
}

// source is an integration_sources row; metadata is the stored JSON text.
type source struct {
	ID              uuid.UUID
	OrgID           string
	IntegrationID   uuid.UUID
	Provider        string
	SourceType      string
	ExternalID      string
	Name            string
	FullName        string
	Metadata        string
	IsEnabled       bool
	DiscoveredAt    time.Time
	LastSeenAt      time.Time
	LastSyncAt      *time.Time
	LastSyncSuccess *bool
	LastSyncError   *string
}

const sourceColumns = `id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata::text, is_enabled,
discovered_at, last_seen_at, last_sync_at, last_sync_success, last_sync_error`

func scanSource(row pgx.Row) (source, error) {
	var out source
	err := row.Scan(&out.ID, &out.OrgID, &out.IntegrationID, &out.Provider, &out.SourceType, &out.ExternalID, &out.Name, &out.FullName,
		&out.Metadata, &out.IsEnabled, &out.DiscoveredAt, &out.LastSeenAt, &out.LastSyncAt, &out.LastSyncSuccess, &out.LastSyncError)
	return out, err
}

// listSources is IntegrationSourceService.list_for_integration.
func listSources(ctx context.Context, q querier, orgID string, integrationID uuid.UUID) ([]source, error) {
	rows, err := q.Query(ctx, `SELECT `+sourceColumns+` FROM public.integration_sources
WHERE org_id=$1 AND integration_id=$2 ORDER BY full_name, id`, orgID, integrationID)
	if err != nil {
		return nil, fmt.Errorf("list sources: %w", err)
	}
	defer rows.Close()
	var out []source
	for rows.Next() {
		item, err := scanSource(rows)
		if err != nil {
			return nil, fmt.Errorf("scan source: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// getSource is IntegrationSourceService.get_by_id.
func getSource(ctx context.Context, q querier, orgID string, integrationID, sourceID uuid.UUID) (*source, error) {
	item, err := scanSource(q.QueryRow(ctx, `SELECT `+sourceColumns+` FROM public.integration_sources
WHERE id=$1 AND integration_id=$2 AND org_id=$3`, sourceID, integrationID, orgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	return &item, nil
}

// dataset is an integration_datasets row; options is the stored JSON text.
type dataset struct {
	ID                    uuid.UUID
	OrgID                 string
	IntegrationID         uuid.UUID
	DatasetKey            string
	IsEnabled             bool
	Options               string
	UnavailableReason     *string
	UnavailableSince      *time.Time
	UnavailableLastSeenAt *time.Time
}

const datasetColumns = `id, org_id, integration_id, dataset_key, is_enabled, options::text, unavailable_reason, unavailable_since, unavailable_last_seen_at`

func scanDataset(row pgx.Row) (dataset, error) {
	var out dataset
	err := row.Scan(&out.ID, &out.OrgID, &out.IntegrationID, &out.DatasetKey, &out.IsEnabled, &out.Options,
		&out.UnavailableReason, &out.UnavailableSince, &out.UnavailableLastSeenAt)
	return out, err
}

// listDatasets is IntegrationDatasetService.list_for_integration.
func listDatasets(ctx context.Context, q querier, orgID string, integrationID uuid.UUID) ([]dataset, error) {
	rows, err := q.Query(ctx, `SELECT `+datasetColumns+` FROM public.integration_datasets
WHERE org_id=$1 AND integration_id=$2 ORDER BY dataset_key`, orgID, integrationID)
	if err != nil {
		return nil, fmt.Errorf("list datasets: %w", err)
	}
	defer rows.Close()
	var out []dataset
	for rows.Next() {
		item, err := scanDataset(rows)
		if err != nil {
			return nil, fmt.Errorf("scan dataset: %w", err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// getDatasetByKey is IntegrationDatasetService.get_by_key.
func getDatasetByKey(ctx context.Context, q querier, orgID string, integrationID uuid.UUID, key string) (*dataset, error) {
	item, err := scanDataset(q.QueryRow(ctx, `SELECT `+datasetColumns+` FROM public.integration_datasets
WHERE org_id=$1 AND integration_id=$2 AND dataset_key=$3`, orgID, integrationID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get dataset: %w", err)
	}
	return &item, nil
}
