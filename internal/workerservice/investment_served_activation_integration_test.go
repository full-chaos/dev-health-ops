//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The executor that the WORKER builds (buildNativeInvestmentExecutor), with
// only the served decision switch changed between the rows of the table
// (CHAOS-8874). A cited constructor is not proof: the served row itself must
// carry the decision stamp, with only its own switch on.
func TestTheWorkersInvestmentExecutorServesFromTheDecisionBackendWithOnlyItsOwnSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	within := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -5)
	text := strings.Repeat("Rework the retry path of the importer after the outage review. ", 8)
	for _, id := range []string{"W1", "W2"} {
		if err := conn.Exec(ctx, `
			INSERT INTO work_items (
				repo_id, work_item_id, provider, title, description, type, status, status_raw,
				project_key, project_id, assignees, reporter, created_at, updated_at,
				labels, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id
			) VALUES (generateUUIDv4(), ?, 'linear', ?, ?, 'issue', 'open', 'open', '', '', [], '', ?, ?, [], '', '', '', '', '', ?, ?)`,
			id, id, text, within, within, within, shadowActivationOrg); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, `
		INSERT INTO work_graph_edges (
			edge_id, source_type, source_id, target_type, target_id, edge_type,
			repo_id, provider, provenance, confidence, evidence, discovered_at, last_synced, event_ts, org_id
		) VALUES ('W1->W2', 'issue', 'W1', 'issue', 'W2', 'relates_to', '11111111-1111-4111-8111-111111111111', 'linear', 'native', 1.0, 'seed', ?, ?, ?, ?)`,
		within, within, within, shadowActivationOrg); err != nil {
		t.Fatal(err)
	}

	count := func(query string) uint64 {
		var n uint64
		if err := conn.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	names := []string{
		investment.EnvServedDecisionOrgIDs,
		investment.EnvShadowProvider, investment.EnvShadowOrgIDs, investment.EnvShadowSamplePercent,
		investment.EnvShadowConcurrency, investment.EnvShadowMaxSeconds, investment.EnvShadowMaxUSDPerRun,
		"TYPESAFE_API_KEY", "TYPESAFE_BASE_URL", "TYPESAFE_MODEL",
	}
	claim := workgraph.Claim{
		Request: workgraph.Request{
			ID: "served-activation-request", OrganizationID: shadowActivationOrg,
			Kind: workgraph.KindMaterialize, Scope: []byte(`{"llm_provider":"mock","force":true}`),
		},
		Token: "claim-of-the-served-activation-test",
	}
	specs := []jobruntime.HandlerSpec{{Kind: jobcontract.KindInvestmentMaterialize}}
	cfg := config.Config{Service: "dev-health-worker", ClickHouseURI: secrets.NewValue(instance.URI)}
	const key = "plain words used as a test value"

	for _, tc := range []struct {
		name         string
		env          map[string]string
		wantErr      bool
		wantSends    int
		wantDecision uint64 // served rows with the decision stamp
		wantMock     uint64 // served rows with the mock stamp
	}{
		{name: "every flag off", wantMock: 1},
		{name: "the key alone is not a switch", env: map[string]string{"TYPESAFE_API_KEY": key}, wantMock: 1},
		{name: "the shadow switch is not the served switch", env: map[string]string{
			"TYPESAFE_API_KEY": key, investment.EnvShadowProvider: "typesafe", investment.EnvShadowOrgIDs: shadowActivationOrg,
		}, wantSends: 1, wantMock: 1},
		{name: "another org on the served list", env: map[string]string{
			"TYPESAFE_API_KEY": key, investment.EnvServedDecisionOrgIDs: "another-org",
		}, wantMock: 1},
		{name: "only the served switch on", env: map[string]string{
			"TYPESAFE_API_KEY": key, investment.EnvServedDecisionOrgIDs: shadowActivationOrg,
		}, wantSends: 1, wantDecision: 1},
		{name: "served and shadow on: one request, no shadow row", env: map[string]string{
			"TYPESAFE_API_KEY": key, investment.EnvServedDecisionOrgIDs: shadowActivationOrg,
			investment.EnvShadowProvider: "typesafe", investment.EnvShadowOrgIDs: "*",
		}, wantSends: 1, wantDecision: 1},
		{name: "the served switch on with no key is an error, not a generative run", env: map[string]string{
			investment.EnvServedDecisionOrgIDs: shadowActivationOrg,
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range names {
				t.Setenv(name, "")
			}
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			for _, table := range []string{"work_unit_investment_shadow", "llm_categorization_attempts", "work_unit_investments", "work_unit_investment_quotes", "work_unit_repo_effort", "llm_token_usage"} {
				if err := conn.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
					t.Fatal(err)
				}
			}
			collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
			if err != nil {
				t.Fatal(err)
			}
			logs := &bytes.Buffer{}
			built, err := buildNativeInvestmentExecutor(cfg, specs, collector, slog.New(slog.NewTextHandler(logs, nil)))
			if err != nil || built == nil {
				t.Fatalf("buildNativeInvestmentExecutor: %v %v", built, err)
			}
			executor := built.(*investment.NativeExecutor)
			recorder := &systemOneRecorder{}
			executor.SetShadowHTTPClientForTest(&http.Client{Transport: recorder})

			_, err = executor.Execute(ctx, claim)
			if tc.wantErr {
				var deterministic *workgraph.DeterministicError
				if !errors.As(err, &deterministic) || deterministic.Class != workgraph.ClassLLMProviderInvalid {
					t.Fatalf("Execute err = %v, want a deterministic refusal", err)
				}
				if n := count("SELECT count() FROM work_unit_investments"); n != 0 {
					t.Fatalf("a refused run wrote %d served rows", n)
				}
				if recorder.count() != 0 {
					t.Fatal("a refused run sent a request")
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute: %v\n%s", err, logs.String())
			}
			if recorder.count() != tc.wantSends {
				t.Fatalf("sends = %d, want %d\n%s", recorder.count(), tc.wantSends, logs.String())
			}
			decisionRows := count(`SELECT count() FROM work_unit_investments WHERE categorization_model_version LIKE 'provider=typesafe;api=systemone;model=jev-1.13.0;%' AND categorization_status = 'ok'`)
			mockRows := count(`SELECT count() FROM work_unit_investments WHERE categorization_model_version LIKE 'provider=mock;%'`)
			if decisionRows != tc.wantDecision || mockRows != tc.wantMock {
				t.Fatalf("served rows: %d with the decision stamp, %d with the mock stamp; want %d and %d\n%s", decisionRows, mockRows, tc.wantDecision, tc.wantMock, logs.String())
			}
			servedAttempts := count(`SELECT count() FROM llm_categorization_attempts WHERE role = 'served'`)
			usage := count(`SELECT count() FROM llm_token_usage WHERE provider = 'typesafe' AND model = 'jev-1.13.0' AND input_tokens = 1200 AND calls = 1`)
			shadowRows := count(`SELECT count() FROM work_unit_investment_shadow`)
			if tc.wantDecision == 0 {
				if servedAttempts != 0 || usage != 0 || strings.Contains(logs.String(), "served decision") {
					t.Fatalf("an off switch left a trace: %d attempt rows, %d usage rows\n%s", servedAttempts, usage, logs.String())
				}
				return
			}
			if servedAttempts != 1 || usage != 1 || shadowRows != 0 {
				t.Fatalf("attempt rows (served) = %d, typesafe usage rows = %d, shadow rows = %d; want 1, 1, 0", servedAttempts, usage, shadowRows)
			}
			if n := count(`SELECT count() FROM work_unit_investment_quotes`); n != 1 {
				t.Fatalf("quote rows = %d, want 1", n)
			}
			if recorder.requests[0].URL.String() != "https://api.typesafe.ai/v1/systemone" {
				t.Fatalf("the send went to %s", recorder.requests[0].URL)
			}
			if strings.Count(logs.String(), `msg="investment served decision complete"`) != 1 || strings.Contains(logs.String(), "shadow phase complete") {
				t.Fatalf("want one served run line and no shadow line:\n%s", logs.String())
			}
		})
	}
}
