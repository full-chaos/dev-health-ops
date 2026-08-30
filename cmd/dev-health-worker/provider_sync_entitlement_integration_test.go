//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/providersyncschema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestProviderUnitRefusedByEntitlementTerminalizesAsFeatureDisabled is the
// reachability proof past the handler for CHAOS-4219: a provider unit
// dispatched through the PRODUCTION provider-sync handler (the same
// buildProviderSyncHandlerWithRuntimeDependencies the worker binary uses,
// with a real PostgresRepository, real credential resolution through the
// worker's Fernet cipher, and the real PostgresIncidentEntitlement) for an
// organization whose canonical-incident feature is disabled lands as a
// sync_run_units row with error_category=feature_disabled on its FIRST
// attempt, is never re-claimable, and increments the scraped refusal counter.
//
// Both gated providers run through the same table: PagerDuty is the new
// seam, Jira pins the intentional behavior change from five attempts ending
// in provider_unit_exhausted to one attempt ending in feature_disabled.
func TestProviderUnitRefusedByEntitlementTerminalizesAsFeatureDisabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := providersyncschema.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		// The shared fixture keeps integration_credentials minimal; the
		// worker's credential resolver reads these columns (alembic 0015).
		`ALTER TABLE public.integration_credentials
		   ADD COLUMN org_id text, ADD COLUMN provider text, ADD COLUMN name text,
		   ADD COLUMN is_active boolean NOT NULL DEFAULT TRUE,
		   ADD COLUMN credentials_encrypted text, ADD COLUMN config jsonb`,
		`CREATE TABLE organizations (id uuid PRIMARY KEY, tier text NOT NULL)`,
		`CREATE TABLE feature_flags (
		   id uuid PRIMARY KEY, key text UNIQUE NOT NULL, min_tier text NOT NULL,
		   is_enabled boolean NOT NULL)`,
		`CREATE TABLE org_feature_overrides (
		   org_id uuid NOT NULL, feature_id uuid NOT NULL, is_enabled boolean NOT NULL,
		   expires_at timestamptz, PRIMARY KEY (org_id, feature_id))`,
		`CREATE TABLE org_licenses (
		   org_id uuid PRIMARY KEY, tier text NOT NULL, features_override json)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	featureID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO feature_flags (id, key, min_tier, is_enabled)
VALUES ($1, 'canonical_incident_ingestion', 'community', true)`, featureID); err != nil {
		t.Fatal(err)
	}
	cipher, err := newWorkerCredentialCipher(config.Config{
		SettingsEncryptionKey: secrets.NewValue("test-master-key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := providersync.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	handler, providerMetrics := buildProviderSyncHandlerWithRuntimeDependencies(
		repository, cipher, nil, nil, nil, pool,
		providersync.PostgresIncidentEntitlement{Pool: pool},
		nil, slog.Default(), workItemsRuntimeConfig{},
	)

	for _, test := range []struct {
		provider, dataset, sourceExternalID string
		secret                              map[string]string
		credentialConfig                    map[string]string
	}{
		{
			provider: "pagerduty", dataset: "incidents", sourceExternalID: "acme",
			secret:           map[string]string{"api_token": "pd-token"},
			credentialConfig: map[string]string{"subdomain": "acme"},
		},
		{
			provider: "jira", dataset: "incidents", sourceExternalID: "JSM",
			secret:           map[string]string{"email": "ops@acme.example", "api_token": "jira-token"},
			credentialConfig: map[string]string{"base_url": "https://acme.atlassian.net"},
		},
	} {
		t.Run(test.provider, func(t *testing.T) {
			orgID := uuid.NewString()
			credentialID, integrationID, sourceID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			runID, unitID := uuid.NewString(), uuid.NewString()
			capability, ok := providersync.Capability(test.provider, test.dataset)
			if !ok {
				t.Fatalf("%s/%s has no capability", test.provider, test.dataset)
			}
			plaintext, err := json.Marshal(test.secret)
			if err != nil {
				t.Fatal(err)
			}
			ciphertext, err := cipher.Encrypt(plaintext)
			if err != nil {
				t.Fatal(err)
			}
			credentialConfig, err := json.Marshal(test.credentialConfig)
			if err != nil {
				t.Fatal(err)
			}
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, tier) VALUES ($1, 'community')`, []any{orgID}},
				// Enabled by tier, then disabled by an org override: the
				// exact "disable committed after the dispatch gate's read"
				// shape the execution-time re-check exists for.
				{`INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled) VALUES ($1, $2, false)`,
					[]any{orgID, featureID}},
				{`INSERT INTO public.integration_credentials
				   (id, org_id, provider, name, is_active, credentials_encrypted, config)
				   VALUES ($1, $2, $3, 'default', TRUE, $4, $5::jsonb)`,
					[]any{credentialID, orgID, test.provider, ciphertext.Reveal(), string(credentialConfig)}},
				{`INSERT INTO public.integrations (id, org_id, credential_id, config)
				   VALUES ($1, $2, $3, '{}'::jsonb)`, []any{integrationID, orgID, credentialID}},
				{`INSERT INTO public.integration_sources
				   (id, org_id, integration_id, external_id, full_name, metadata)
				   VALUES ($1, $2, $3, $4, $4, '{}'::jsonb)`,
					[]any{sourceID, orgID, integrationID, test.sourceExternalID}},
				{`INSERT INTO public.integration_datasets
				   (id, org_id, integration_id, dataset_key, options)
				   VALUES ($1, $2, $3, $4, '{}'::jsonb)`,
					[]any{uuid.NewString(), orgID, integrationID, test.dataset}},
				{`INSERT INTO public.sync_runs
				   (id, org_id, integration_id, status, credential_id, credential_fingerprint, auth_source,
				    total_units, completed_units, failed_units)
				   VALUES ($1, $2, $3, 'running', $4, 'fingerprint', 'integration_credential', 0, 0, 0)`,
					[]any{runID, orgID, integrationID, credentialID}},
				{`INSERT INTO public.sync_run_units (
				     id, org_id, sync_run_id, integration_id, source_id, provider,
				     dataset_key, cost_class, mode, since_at, before_at, status,
				     processor_flags, updated_at
				   ) VALUES (
				     $1, $2, $3, $4, $5, $6, $7, $8, 'incremental',
				     '2026-08-01T00:00:00Z', '2026-08-02T00:00:00Z', 'dispatching',
				     '{}'::jsonb, NOW()
				   )`,
					[]any{unitID, orgID, runID, integrationID, sourceID, test.provider, test.dataset, string(capability.CostClass)}},
			} {
				if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatalf("%s: %v", statement.sql, err)
				}
			}

			execution := providerUnitExecution(orgID, runID, unitID, 1)
			err = handler.Work(ctx, execution)
			if !errors.Is(err, providersync.ErrIncidentEntitlementDisabled) {
				t.Fatalf("first attempt err=%v want ErrIncidentEntitlementDisabled", err)
			}
			if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
				t.Fatalf("a policy refusal must not snooze: %v", err)
			}
			status, attempts, category, errorText, leaseOwner := readUnitTerminalRow(t, ctx, pool, unitID)
			if status != "failed" || attempts != 1 || category != "feature_disabled" ||
				errorText != "feature_disabled" || leaseOwner != nil {
				t.Fatalf("row status=%s attempts=%d category=%s error=%s lease=%v",
					status, attempts, category, errorText, leaseOwner)
			}

			// A second dispatch of the same unit finds nothing to claim: the
			// refusal is durable on the first attempt, not a retry that
			// happens to land on the same answer.
			err = handler.Work(ctx, providerUnitExecution(orgID, runID, unitID, 2))
			if !errors.Is(err, providersync.ErrUnitNotClaimable) {
				t.Fatalf("second attempt err=%v want ErrUnitNotClaimable", err)
			}
			status, attempts, category, _, _ = readUnitTerminalRow(t, ctx, pool, unitID)
			if status != "failed" || attempts != 1 || category != "feature_disabled" {
				t.Fatalf("after redispatch status=%s attempts=%d category=%s", status, attempts, category)
			}

			var rendered bytes.Buffer
			if err := providerMetrics.WritePrometheus(&rendered); err != nil {
				t.Fatal(err)
			}
			want := `dev_health_provider_incident_entitlement_refused_total{provider="` +
				test.provider + `",dataset="incidents",seam="collect"} 1`
			if !strings.Contains(rendered.String(), want) {
				t.Fatalf("missing %s in:\n%s", want, rendered.String())
			}
		})
	}
}

func providerUnitExecution(orgID, runID, unitID string, attempt int) *jobruntime.Execution[jobruntime.ProviderUnitArgs] {
	args := jobruntime.ProviderUnitArgs{
		EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ProviderUnitPayload]{
			ContractVersion: 1,
			OrganizationID:  &orgID,
			CorrelationID:   "sync-run:" + runID,
			IdempotencyKey:  "sync.provider_unit:" + unitID,
			Domain:          jobcontract.DomainLink{Type: "sync_run_unit", ID: unitID},
			Payload:         jobcontract.ProviderUnitPayload{UnitID: unitID},
		},
	}
	return &jobruntime.Execution[jobruntime.ProviderUnitArgs]{
		Attempt: attempt, Args: args, Envelope: args.ContractEnvelope(),
		OrganizationID: &orgID, Deadline: time.Now().Add(10 * time.Minute),
		Definition: jobruntime.Descriptor{MaxAttempts: 5},
	}
}

func readUnitTerminalRow(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string,
) (status string, attempts int, category string, errorText string, leaseOwner *string) {
	t.Helper()
	var storedCategory, storedError *string
	if err := pool.QueryRow(ctx, `
SELECT status, attempts, result::jsonb->>'error_category', error, lease_owner
FROM public.sync_run_units WHERE id = $1::uuid`, unitID).Scan(
		&status, &attempts, &storedCategory, &storedError, &leaseOwner,
	); err != nil {
		t.Fatal(err)
	}
	if storedCategory != nil {
		category = *storedCategory
	}
	if storedError != nil {
		errorText = *storedError
	}
	return status, attempts, category, errorText, leaseOwner
}
