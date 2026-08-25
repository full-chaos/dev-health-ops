//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testGitHubProjectsV2NullSnapshotsPreserveDurableMembershipAndWatermark is
// CHAOS-4289's LIVE-DATA-EVIDENCE proof. It starts at the production route's
// decoded GraphQL response and crosses every durable boundary: the real
// CompleteRouteExecutor, PostgresRepository effect ledger, ClickHouse effect
// sink/readback, and PostgresRepository.Complete watermark transaction.
//
// The first unit seeds a prior membership through that route. The next two
// units feed a decoded null organization and null projectV2 response. Neither
// is an authoritative empty board: both must produce zero removal rows, leave
// the prior ClickHouse transition and presence intact, and leave the five
// family watermarks at the seed value. The final unit feeds a genuinely empty,
// non-null board and must produce the removal transition and advance all five
// watermarks. The only fake is the HTTP doer, which injects provider payloads.
func testGitHubProjectsV2NullSnapshotsPreserveDurableMembershipAndWatermark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	_, conn := newWorkItemEffectsConn(t)

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
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	const durableOrgID = "6f1c2d3e-4a5b-4c6d-8e9f-0a1b2c3d4e5f"
	for _, statement := range []string{
		"UPDATE public.integrations SET org_id = $1",
		"UPDATE public.integration_sources SET org_id = $1",
		"UPDATE public.integration_datasets SET org_id = $1",
		"UPDATE public.sync_runs SET org_id = $1",
		"UPDATE public.sync_run_units SET org_id = $1",
	} {
		if _, err := pool.Exec(ctx, statement, durableOrgID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.integrations
SET config = '{"api_url":"https://api.github.com","github_projects_v2":[{"org_login":"acme","project_number":3}]}'::jsonb
WHERE id = $1`, firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options)
VALUES ($1, $3, $2, 'work-items',
        '{"include_issues":false,"include_pull_requests":false,"fetch_comments":false,"fetch_milestones":false}'::jsonb)`,
		firstCredentialID, firstIntegrationID, durableOrgID); err != nil {
		t.Fatal(err)
	}

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		unitID       string
		before       time.Time
		graphqlReply string
		wantRemoval  bool
		wantCause    string
	}{
		{
			name:         "seed prior membership",
			unitID:       "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			before:       time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC),
			graphqlReply: githubProjectsV2DurablePRResponse,
		},
		{
			name:         "null organization",
			unitID:       "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
			before:       time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC),
			graphqlReply: `{"data":{"organization":null}}`,
			wantCause:    githubProjectsV2NullOrganization,
		},
		{
			name:         "null project",
			unitID:       "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
			before:       time.Date(2026, 8, 20, 3, 0, 0, 0, time.UTC),
			graphqlReply: `{"data":{"organization":{"projectV2":null}}}`,
			wantCause:    githubProjectsV2NullProject,
		},
		{
			name:         "genuinely empty board",
			unitID:       "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
			before:       time.Date(2026, 8, 20, 4, 0, 0, 0, time.UTC),
			graphqlReply: `{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
			wantRemoval:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			insertGitHubProjectsV2DurableUnit(t, ctx, pool, test.unitID, test.before)
			claim, err := repository.Claim(ctx, ClaimRequest{
				UnitID: test.unitID, OrgID: durableOrgID, Owner: test.unitID,
				Now: test.before.Add(time.Minute), LeaseDuration: time.Minute,
				AllowExpiredRecovery: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if claim.IntegrationConfig[gitHubProjectsV2IntegrationConfigKey] == nil {
				t.Fatalf("claim lost durable Projects V2 config: %#v", claim.IntegrationConfig)
			}

			metrics := providerfoundation.NewMetrics()
			sink, err := NewGitHubWorkItemClickHouseEffects(
				conn, leaseGuardAt(repository, claim, test.before.Add(time.Minute)), metrics,
			)
			if err != nil {
				t.Fatal(err)
			}
			handler := GitHubWorkItemsRouteHandler{
				Projects: GitHubProjectV2Fetcher{},
				ProjectMembershipSnapshotDiff: GitHubProjectV2SnapshotDiffClickHouseReader{
					Conn: conn,
				},
				Deriver: &githubWorkItemsRouteDeriver{rows: projectsV2DurableEmptyDerivedRows()},
			}
			executor := CompleteRouteExecutor{
				Credentials: providerfoundation.CredentialResolver{
					Repository: projectsV2DurableCredentialRepository{},
					Decryptor:  projectsV2DurableCredentialDecryptor{},
				},
				Doer: projectsV2DurableDoer(t, test.graphqlReply),
				Retry: providerfoundation.RetryPolicy{
					MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
				},
				Budget:       executorBudgetStore{},
				BudgetLimits: map[CostClass]int{CostMedium: 1},
				BudgetTTL:    time.Minute,
				Gate: func(Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
					return executorBackoffGate{}
				},
				Metrics:    metrics,
				Handler:    handler,
				Comparator: ProductionContractComparator{},
				Committer: EffectCommitter{
					Ledger: repository, Sink: sink, Readback: sink,
					Now: func() time.Time { return test.before.Add(time.Minute) },
				},
				HeartbeatInterval: 30 * time.Second,
				Now:               func() time.Time { return test.before.Add(time.Minute) },
			}
			descriptor, ok := Descriptor("github", "work-items")
			if !ok || !descriptor.RouteReady || !descriptor.Plannable {
				t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
			}
			session := &LeaseSession{
				Repository: repository, Claim: claim, LeaseDuration: time.Minute,
				Deadline: claim.LeaseExpiresAt, Now: func() time.Time { return test.before.Add(time.Minute) },
			}
			result, err := executor.Execute(ctx, session, descriptor)
			if err != nil {
				t.Fatalf("execute: %v claim=%+v descriptor=%+v session_valid=%t", err, claim, descriptor, session.valid())
			}
			ledgerState, err := repository.LoadEffects(ctx, claim, test.before.Add(time.Minute))
			if err != nil {
				t.Fatalf("load durable effect ledger before completion: %v", err)
			}
			membershipEffectRows := -1
			for _, effect := range ledgerState.Effects {
				if effect.Destination == "project_membership_transitions" {
					membershipEffectRows = effect.RowCount
				}
			}
			t.Logf("LIVE-DATA-EVIDENCE effect-ledger case=%s schema=%s membership_transition_rows=%d effects=%+v",
				test.name, ledgerState.SchemaVersion, membershipEffectRows, ledgerState.Effects)
			if membershipEffectRows < 0 {
				t.Fatal("durable effect ledger omitted project_membership_transitions")
			}
			if test.wantCause != "" && membershipEffectRows != 0 {
				t.Fatalf("degraded response prepared %d removal effect rows", membershipEffectRows)
			}
			if test.wantRemoval && membershipEffectRows != 1 {
				t.Fatalf("empty board prepared %d removal effect rows, want 1", membershipEffectRows)
			}
			if err := repository.Complete(
				ctx, claim, result.Result, result.Watermark,
				test.before.Add(time.Minute), test.before.Add(62*time.Second),
			); err != nil {
				t.Fatal(err)
			}

			var membershipRows uint64
			var subjectKind, subjectID, toProjectID string
			if err := conn.QueryRow(ctx, `
SELECT count(), any(subject_kind), any(subject_id), any(to_project_id)
FROM project_membership_transitions FINAL
WHERE org_id = ? AND subject_id = '42'`, claim.OrgID,
			).Scan(&membershipRows, &subjectKind, &subjectID, &toProjectID); err != nil {
				t.Fatal(err)
			}
			var presenceRows uint64
			if err := conn.QueryRow(ctx, `
SELECT count() FROM project_membership_presence
WHERE org_id = ? AND subject_kind = 'pull_request' AND subject_id = '42'`, claim.OrgID,
			).Scan(&presenceRows); err != nil {
				t.Fatal(err)
			}
			var status string
			var resultJSON string
			if err := pool.QueryRow(ctx, `
SELECT status, result::text FROM public.sync_run_units WHERE id = $1`, test.unitID,
			).Scan(&status, &resultJSON); err != nil {
				t.Fatal(err)
			}
			watermarks := projectsV2DurableWatermarks(t, ctx, pool, claim.OrgID, claim.SourceExternalID)
			t.Logf("LIVE-DATA-EVIDENCE case=%s status=%s result=%s transitions=%d subject=%s/%s to=%s presence=%d watermarks=%v",
				test.name, status, resultJSON, membershipRows, subjectKind, subjectID,
				toProjectID, presenceRows, watermarks)
			if status != "success" {
				t.Fatalf("durable unit status=%q, raw result=%s", status, resultJSON)
			}
			if test.wantCause != "" {
				if result.Watermark != nil || !strings.Contains(resultJSON, test.wantCause) {
					t.Fatalf("degraded result watermark=%v result=%s want cause=%q", result.Watermark, resultJSON, test.wantCause)
				}
				if result.Effects.Written == 0 {
					t.Fatal("degraded route committed no durable effects")
				}
				if membershipRows != 1 || presenceRows != 1 || toProjectID != "ghprojv2:acme#3" {
					t.Fatalf("degraded response changed prior membership: transitions=%d presence=%d to=%q", membershipRows, presenceRows, toProjectID)
				}
				seedWatermark := time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC)
				for dataset, watermark := range watermarks {
					if !watermark.Equal(seedWatermark) {
						t.Fatalf("degraded response advanced %s watermark to %s, seed=%s", dataset, watermark, seedWatermark)
					}
				}
			}
			if test.wantRemoval {
				if result.Watermark == nil || !result.Watermark.Equal(*claim.BeforeAt) {
					t.Fatalf("empty board watermark=%v want=%v", result.Watermark, claim.BeforeAt)
				}
				if membershipRows != 2 || presenceRows != 0 || toProjectID != "" {
					t.Fatalf("empty board did not durably remove prior membership: transitions=%d presence=%d to=%q", membershipRows, presenceRows, toProjectID)
				}
				for dataset, watermark := range watermarks {
					if !watermark.Equal(*claim.BeforeAt) {
						t.Fatalf("empty board %s watermark=%s want=%s", dataset, watermark, *claim.BeforeAt)
					}
				}
			}

			var exposition bytes.Buffer
			if err := metrics.WritePrometheus(&exposition); err != nil {
				t.Fatal(err)
			}
			t.Logf("LIVE-DATA-EVIDENCE telemetry=%s", exposition.String())
			if test.wantCause != "" && !strings.Contains(exposition.String(),
				`dev_health_providersync_projects_v2_degraded_snapshots_total{reason="`+test.wantCause+`"} 1`) {
				t.Fatalf("degraded telemetry missing cause=%q: %s", test.wantCause, exposition.String())
			}
		})
	}
}

const githubProjectsV2DurablePRResponse = `{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`

func projectsV2DurableDoer(t *testing.T, graphqlReply string) *githubWorkItemsRouteDoer {
	t.Helper()
	return &githubWorkItemsRouteDoer{
		t: t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
			"/repos/acme/api": {{body: `{"id":4567,"full_name":"acme/api"}`}},
		}},
		graphqlReplies: []string{graphqlReply},
	}
}

func projectsV2DurableEmptyDerivedRows() map[string][]json.RawMessage {
	rows := make(map[string][]json.RawMessage, len(githubWorkItemDerivedDestinations))
	for _, destination := range githubWorkItemDerivedDestinations {
		rows[destination] = []json.RawMessage{}
	}
	return rows
}

func insertGitHubProjectsV2DurableUnit(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string, before time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, '6f1c2d3e-4a5b-4c6d-8e9f-0a1b2c3d4e5f', $2, $3, $4, 'github', 'work-items', 'medium',
    'incremental', $5, $6, 'dispatching',
    '{"family_dataset_work_items":true,
      "family_dataset_work_item_labels":true,
      "family_dataset_work_item_projects":true,
      "family_dataset_work_item_history":true,
      "family_dataset_work_item_comments":true}'::jsonb,
    $6
)`, unitID, firstRunID, firstIntegrationID, firstSourceID,
		before.Add(-24*time.Hour), before); err != nil {
		t.Fatal(err)
	}
}

func projectsV2DurableWatermarks(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, sourceID string,
) map[string]time.Time {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT dataset_key, last_synced_at FROM public.sync_watermarks
WHERE org_id = $1 AND source_id = $2 ORDER BY dataset_key`, orgID, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]time.Time{}
	for rows.Next() {
		var datasetKey string
		var syncedAt time.Time
		if err := rows.Scan(&datasetKey, &syncedAt); err != nil {
			t.Fatal(err)
		}
		result[datasetKey] = syncedAt.UTC()
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

type projectsV2DurableCredentialRepository struct{}

func (projectsV2DurableCredentialRepository) ResolveEncrypted(
	context.Context, providerfoundation.TenantScope,
) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{
		ID: firstCredentialID, Provider: "github", Name: "fixture", Active: true,
		Ciphertext: secrets.NewValue("opaque"),
		Config:     map[string]string{"base_url": "https://api.github.com"},
	}, nil
}

type projectsV2DurableCredentialDecryptor struct{}

func (projectsV2DurableCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"token":"fixture-token"}`), nil
}
