package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// ownershipMetricsWiringLeaseRepository is a no-op LeaseRepository: this
// test never exercises lease recovery or renewal, only that a write can
// pass the ONE Assert call the write path itself makes.
type ownershipMetricsWiringLeaseRepository struct{}

func (ownershipMetricsWiringLeaseRepository) Claim(
	context.Context, providersync.ClaimRequest,
) (providersync.Claim, error) {
	return providersync.Claim{}, nil
}

func (ownershipMetricsWiringLeaseRepository) Assert(
	context.Context, providersync.Claim, time.Time,
) error {
	return nil
}

func (ownershipMetricsWiringLeaseRepository) Renew(
	context.Context, providersync.Claim, time.Time, time.Time,
) error {
	return nil
}

// ownershipMetricsWiringConn is a non-nil driver.Conn whose PrepareBatch
// returns a no-op batch, sufficient to reach the real write boundary without
// a live ClickHouse.
type ownershipMetricsWiringConn struct{ driver.Conn }

func (*ownershipMetricsWiringConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return &ownershipMetricsWiringBatch{}, nil
}

type ownershipMetricsWiringBatch struct{ driver.Batch }

func (*ownershipMetricsWiringBatch) Append(...any) error { return nil }
func (*ownershipMetricsWiringBatch) Send() error         { return nil }
func (*ownershipMetricsWiringBatch) Abort() error        { return nil }

// ownershipMetricsWiringTeamAttributionRow and
// ownershipMetricsWiringRejectionRow mirror providersync's unexported row
// shapes field-for-field by JSON tag only -- this package cannot import the
// unexported types, and does not need to: the write boundary decodes by tag,
// not by Go type.
type ownershipMetricsWiringTeamAttributionRow struct {
	WorkItemID      string    `json:"work_item_id"`
	Provider        string    `json:"provider"`
	Source          string    `json:"source"`
	IsPrimary       int       `json:"is_primary"`
	Confidence      string    `json:"confidence"`
	Evidence        string    `json:"evidence"`
	ComputedAt      time.Time `json:"computed_at"`
	RepoID          *string   `json:"repo_id"`
	TeamID          *string   `json:"team_id"`
	TeamName        *string   `json:"team_name"`
	OrgID           string    `json:"org_id"`
	Priority        int       `json:"priority"`
	OwnershipReason string    `json:"ownership_reason"`
}

type ownershipMetricsWiringRejectionRow struct {
	WorkItemID string  `json:"work_item_id"`
	Provider   string  `json:"provider"`
	RepoID     *string `json:"repo_id"`
	Source     string  `json:"source"`
	TeamID     *string `json:"team_id"`
	TeamName   *string `json:"team_name"`
	Reason     string  `json:"reason"`
}

// providerSyncOwnershipMetricsClaim builds a Claim that passes both
// Unit.Validate and Claim.Validate for the given provider's work-items
// dataset -- the exact shape the real write boundary's identity check
// requires.
func providerSyncOwnershipMetricsClaim(t *testing.T, provider string) providersync.Claim {
	t.Helper()
	capability, ok := providersync.Capability(provider, "work-items")
	if !ok {
		t.Fatalf("no work-items capability registered for %s", provider)
	}
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	return providersync.Claim{
		Unit: providersync.Unit{
			ID:               "11111111-1111-4111-8111-111111111111",
			SyncRunID:        "22222222-2222-4222-8222-222222222222",
			OrgID:            "org-acme",
			IntegrationID:    "33333333-3333-4333-8333-333333333333",
			SourceID:         "44444444-4444-4444-8444-444444444444",
			SourceExternalID: "acme/api",
			SourceName:       "acme/api",
			Provider:         provider,
			Dataset:          "work-items",
			CostClass:        capability.CostClass,
			Mode:             "incremental",
			SinceAt:          &since,
			BeforeAt:         &before,
			CredentialID:     "55555555-5555-4555-8555-555555555555",
			AuthSource:       "integration_credential",
		},
		Owner:          "66666666-6666-4666-8666-666666666666",
		Attempt:        1,
		LeaseExpiresAt: time.Now().Add(-time.Hour),
	}
}

// TestBuildProviderSyncHandlerWiresOwnershipMetricsIntoEveryWorkItemTeamAttributionsSink
// is a mutation-resistant pin for the ownership-gate rejection
// telemetry test-strength gap: the existing construction tests
// (TestBuildProviderSyncHandlerConstructsAggregateWorkItemRoutes,
// TestBuildProviderSyncHandlerConstructsGitHubWorkItemsWithValidatedRuntimeConfig)
// only assert the constructed handler/sink TYPES, never that the shared
// providerMetrics instance this worker scrapes actually reached the
// constructed sink -- so independently passing nil instead of providerMetrics
// at any one of the four NewXWorkItemXClickHouseEffects call sites in
// buildProviderSyncHandlerWithRuntimeDependencies passed the full committed
// suite for that provider. This builds the REAL handler via the REAL
// BuildExecutor for each provider's work-items route, writes a
// work_item_team_attributions effect (one granted candidate, one gate
// rejection) through the REAL constructed sink, and asserts both ownership
// outcomes rendered on the SAME providerMetrics instance the worker
// registers for scraping.
func TestBuildProviderSyncHandlerWiresOwnershipMetricsIntoEveryWorkItemTeamAttributionsSink(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(validGitHubWorkItemsRuntimeConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			handler, providerMetrics := buildProviderSyncHandlerWithGitHubWorkItemsRuntimeConfig(
				nil, nil,
				&ownershipMetricsWiringConn{}, nil, nil, nil, nil,
				slog.Default(), runtimeConfig,
			)
			claim := providerSyncOwnershipMetricsClaim(t, provider)
			session := &providersync.LeaseSession{
				Repository:    ownershipMetricsWiringLeaseRepository{},
				Claim:         claim,
				LeaseDuration: time.Minute,
				Deadline:      time.Now().Add(time.Hour),
			}
			executor, err := handler.BuildExecutor(session)
			if err != nil {
				t.Fatalf("%s: BuildExecutor: %v", provider, err)
			}

			granted, err := json.Marshal(ownershipMetricsWiringTeamAttributionRow{
				WorkItemID: "acme/api#1", Provider: provider, Source: "assignee_membership",
				IsPrimary: 1, Confidence: "high", Evidence: "assignee=dev@example.com",
				ComputedAt: time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
				OrgID:      "org-acme", OwnershipReason: "ownership_unknown",
			})
			if err != nil {
				t.Fatal(err)
			}
			effect, err := providersync.BuildEffectBatch(
				"work_item_team_attributions", providersync.EffectReadbackRequired,
				[]json.RawMessage{granted},
			)
			if err != nil {
				t.Fatal(err)
			}
			teamID := "nonowner"
			rejection, err := json.Marshal(ownershipMetricsWiringRejectionRow{
				WorkItemID: "acme/api#2", Provider: provider, Source: "author_membership",
				TeamID: &teamID, Reason: "repo_not_owned",
			})
			if err != nil {
				t.Fatal(err)
			}
			effect.MembershipRejections = []json.RawMessage{rejection}

			if err := executor.Committer.Sink.WriteEffect(context.Background(), claim, effect); err != nil {
				t.Fatalf("%s: WriteEffect: %v", provider, err)
			}
			var rendered strings.Builder
			if err := providerMetrics.WritePrometheus(&rendered); err != nil {
				t.Fatal(err)
			}
			output := rendered.String()
			if !strings.Contains(output, `dev_health_team_attribution_ownership_checked_total{reason="ownership_unknown"} 1`) {
				t.Fatalf(
					"%s: granted candidate produced no ownership_unknown sample -- the worker's real "+
						"constructor call site is not passing the shared providerMetrics instance:\n%s",
					provider, output,
				)
			}
			if !strings.Contains(output, `dev_health_team_attribution_ownership_checked_total{reason="repo_not_owned"} 1`) {
				t.Fatalf(
					"%s: rejection produced no repo_not_owned sample -- the worker's real constructor "+
						"call site is not passing the shared providerMetrics instance:\n%s",
					provider, output,
				)
			}
		})
	}
}
