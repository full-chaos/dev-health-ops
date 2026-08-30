//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/providersyncschema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// githubAllArtifactsUnreadableFixtureDoer is an httptest handler for a
// GitHub repository whose every cicd artifact answers unreadable. It
// distinguishes the runs-phase listing (no `branch` query) from the
// artifacts-phase listing (has `branch`, since the fixture repo declares a
// default branch) exactly as production traffic does: the runs phase is
// left empty so this test needs no ci_pipeline_runs/ci_job_runs ClickHouse
// writes at all, isolating the assertion to the totality gate itself.
func githubAllArtifactsUnreadableFixtureDoer(t *testing.T, runs int) http.HandlerFunc {
	t.Helper()
	return func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/repos/acme/totality-repo":
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(`{"id":1,"name":"totality-repo","full_name":"acme/totality-repo",` +
				`"default_branch":"main","archived":false}`))
		case request.URL.Path == "/repos/acme/totality-repo/actions/runs":
			if request.URL.Query().Get("branch") == "" {
				writer.WriteHeader(http.StatusOK)
				writer.Write([]byte(`{"workflow_runs":[]}`))
				return
			}
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(githubAllArtifactsUnreadableRunsFixture(runs)))
		case strings.HasSuffix(request.URL.Path, "/jobs"):
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(`{"jobs":[]}`))
		case strings.HasSuffix(request.URL.Path, "/artifacts"):
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(`{"artifacts":[{"id":1,"name":"test-results-1","expired":false}]}`))
		case strings.Contains(request.URL.Path, "/actions/artifacts/") && strings.HasSuffix(request.URL.Path, "/zip"):
			// A 200 whose body is not a zip -- the shape a proxy or auth edge
			// produces when it intercepts every artifact request with an
			// error document instead of the archive (CHAOS-4185).
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte(`{"message":"Not Found","documentation_url":"https://docs.github.com/rest"}`))
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
		}
	}
}

func githubAllArtifactsUnreadableRunsFixture(runs int) string {
	var body strings.Builder
	body.WriteString(`{"workflow_runs":[`)
	for id := 1; id <= runs; id++ {
		if id > 1 {
			body.WriteByte(',')
		}
		body.WriteString(`{"id":` + strconv.Itoa(id) + `,"name":"ci","status":"completed",` +
			`"conclusion":"success","run_attempt":1,"created_at":"2026-08-01T10:00:00Z",` +
			`"updated_at":"2026-08-01T10:05:00Z","run_started_at":"2026-08-01T10:00:00Z",` +
			`"html_url":"https://github.com/acme/totality-repo/actions/runs/` + strconv.Itoa(id) + `"}`)
	}
	body.WriteString(`]}`)
	return body.String()
}

// TestProviderUnitAllArtifactsUnreadableTerminalizesOnFirstAttempt is the
// CHAOS-4185 reachability proof past the handler: a github/cicd provider
// unit dispatched through the PRODUCTION provider-sync handler (the same
// buildProviderSyncHandlerWithRuntimeDependencies the worker binary uses,
// with a real PostgresRepository, real credential resolution through the
// worker's Fernet cipher, a real ClickHouse effect sink, and a real Valkey
// budget store) against a source whose every observed cicd artifact is
// unreadable lands as a sync_run_units row with
// error_category=all_artifacts_unreadable on its FIRST attempt, is never
// re-claimable, and increments the scraped totality counter -- rather than
// completing a unit that ingested nothing, or burning all five attempts
// before recording the generic provider_unit_exhausted category (pattern:
// provider_sync_entitlement_integration_test.go, PR #1899).
func TestProviderUnitAllArtifactsUnreadableTerminalizesOnFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := postgres.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := providersyncschema.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE public.integration_credentials
		   ADD COLUMN org_id text, ADD COLUMN provider text, ADD COLUMN name text,
		   ADD COLUMN is_active boolean NOT NULL DEFAULT TRUE,
		   ADD COLUMN credentials_encrypted text, ADD COLUMN config jsonb`); err != nil {
		t.Fatal(err)
	}

	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })
	clickhouseConn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouse.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouseConn.Close() })

	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })
	valkeyClient, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(valkey.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(valkeyClient.Close)

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
		repository, cipher, nil, clickhouseConn, valkeyClient, pool,
		nil, nil, slog.Default(), workItemsRuntimeConfig{},
	)

	server := httptest.NewServer(githubAllArtifactsUnreadableFixtureDoer(t, 2))
	t.Cleanup(server.Close)

	capability, ok := providersync.Capability("github", "cicd")
	if !ok {
		t.Fatal("github/cicd has no capability")
	}

	orgID := uuid.NewString()
	credentialID, integrationID, sourceID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	runID, unitID := uuid.NewString(), uuid.NewString()
	secretPayload, err := json.Marshal(map[string]string{"token": "gh-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := cipher.Encrypt(secretPayload)
	if err != nil {
		t.Fatal(err)
	}
	credentialConfig, err := json.Marshal(map[string]string{"base_url": server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.integration_credentials
		   (id, org_id, provider, name, is_active, credentials_encrypted, config)
		   VALUES ($1, $2, 'github', 'default', TRUE, $3, $4::jsonb)`,
			[]any{credentialID, orgID, ciphertext.Reveal(), string(credentialConfig)}},
		{`INSERT INTO public.integrations (id, org_id, credential_id, config)
		   VALUES ($1, $2, $3, '{}'::jsonb)`, []any{integrationID, orgID, credentialID}},
		{`INSERT INTO public.integration_sources
		   (id, org_id, integration_id, external_id, full_name, metadata)
		   VALUES ($1, $2, $3, $4, $4, '{}'::jsonb)`,
			[]any{sourceID, orgID, integrationID, "acme/totality-repo"}},
		{`INSERT INTO public.integration_datasets
		   (id, org_id, integration_id, dataset_key, options)
		   VALUES ($1, $2, $3, 'cicd', '{}'::jsonb)`,
			[]any{uuid.NewString(), orgID, integrationID}},
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
		     $1, $2, $3, $4, $5, 'github', 'cicd', $6, 'incremental',
		     '2026-08-01T00:00:00Z', '2026-08-02T00:00:00Z', 'dispatching',
		     '{}'::jsonb, NOW()
		   )`,
			[]any{unitID, orgID, runID, integrationID, sourceID, string(capability.CostClass)}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("%s: %v", statement.sql, err)
		}
	}

	execution := providerUnitExecution(orgID, runID, unitID, 1)
	err = handler.Work(ctx, execution)
	if !errors.Is(err, providersync.ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("first attempt err=%v want ErrGitHubTestsAllArtifactsUnreadable", err)
	}
	if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
		t.Fatalf("a deterministic totality failure must not snooze: %v", err)
	}
	status, attempts, category, errorText, leaseOwner := readUnitTerminalRow(t, ctx, pool, unitID)
	if status != "failed" || attempts != 1 || category != "all_artifacts_unreadable" ||
		errorText != "all_artifacts_unreadable" || leaseOwner != nil {
		t.Fatalf("row status=%s attempts=%d category=%s error=%s lease=%v",
			status, attempts, category, errorText, leaseOwner)
	}

	// A second dispatch of the same unit finds nothing to claim: the
	// failure is durable on the first attempt, not a retry that happens to
	// land on the same outcome.
	err = handler.Work(ctx, providerUnitExecution(orgID, runID, unitID, 2))
	if !errors.Is(err, providersync.ErrUnitNotClaimable) {
		t.Fatalf("second attempt err=%v want ErrUnitNotClaimable", err)
	}
	status, attempts, category, _, _ = readUnitTerminalRow(t, ctx, pool, unitID)
	if status != "failed" || attempts != 1 || category != "all_artifacts_unreadable" {
		t.Fatalf("after redispatch status=%s attempts=%d category=%s", status, attempts, category)
	}

	var rendered bytes.Buffer
	if err := providerMetrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	want := `dev_health_provider_all_artifacts_unreadable_total{provider="github",dataset="cicd"} 1`
	if !strings.Contains(rendered.String(), want) {
		t.Fatalf("missing %s in:\n%s", want, rendered.String())
	}
}
