//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
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

// systemOneRecorder is a fake HTTP transport for the TypeSafe client that the
// worker builds from its configuration. It records each request and answers
// from the questions of that request. Nothing leaves the process.
type systemOneRecorder struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (recorder *systemOneRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(request.Body)
	recorder.mu.Lock()
	recorder.requests = append(recorder.requests, request)
	recorder.mu.Unlock()
	var parsed struct {
		Model     string `json:"model"`
		Questions map[string]struct {
			Type     string          `json:"type"`
			Criteria json.RawMessage `json:"criteria"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	answers := map[string]any{}
	for id, question := range parsed.Questions {
		switch question.Type {
		case "score":
			var levels []string
			_ = json.Unmarshal(question.Criteria, &levels)
			level := 0
			switch {
			case id == "support__quality__bugfix":
				level = 3
			case !strings.HasPrefix(id, "support__"):
				level = len(levels) - 1
			}
			probabilities := map[string]float64{}
			for index := range levels {
				probabilities[string(rune('0'+index))] = 0
			}
			probabilities[string(rune('0'+level))] = 1
			answers[id] = map[string]any{"type": "score", "score": float64(level), "confidence": 0.9, "probabilities": probabilities}
		case "choice":
			var options map[string]any
			_ = json.Unmarshal(question.Criteria, &options)
			names := make([]string, 0, len(options))
			for name := range options {
				names = append(names, name)
			}
			sort.Strings(names)
			choice := ""
			probabilities := map[string]float64{}
			for _, name := range names {
				probabilities[name] = 0
				if choice == "" && name != "none" {
					choice = name
				}
			}
			probabilities[choice] = 1
			answers[id] = map[string]any{"type": "choice", "choice": choice, "confidence": 0.9, "probabilities": probabilities}
		}
	}
	encoded, _ := json.Marshal(map[string]any{
		"model": parsed.Model, "answers": answers, "usage": map[string]any{"input_tokens": 1200, "output_tokens": 300},
	})
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(bytes.NewReader(encoded)), Request: request,
	}, nil
}

func (recorder *systemOneRecorder) count() int {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return len(recorder.requests)
}

const shadowActivationOrg = "70d529e0-3c06-4597-8480-794fd02328b6"

// The executor that the WORKER builds (buildNativeInvestmentExecutor), with
// only the shadow switch changed between the rows of the table:
//
//   - every flag off (R0): zero requests, zero rows in both shadow tables;
//   - only the shadow switch on (R1 prerequisite): the executor constructs the
//     completer and reaches a send, and the shadow and attempt rows exist.
//
// The served result is the same in both: the ledger evidence bytes are equal,
// so nothing of the phase is in output_evidence.
func TestTheWorkersInvestmentExecutorReachesAShadowSendWithOnlyItsOwnSwitch(t *testing.T) {
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

	count := func(table string) uint64 {
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		return n
	}
	shadowEnv := []string{
		investment.EnvShadowProvider, investment.EnvShadowOrgIDs, investment.EnvShadowSamplePercent,
		investment.EnvShadowConcurrency, investment.EnvShadowMaxSeconds, investment.EnvShadowMaxUSDPerRun,
		"TYPESAFE_API_KEY", "TYPESAFE_BASE_URL", "TYPESAFE_MODEL",
	}
	claim := workgraph.Claim{
		Request: workgraph.Request{
			ID: "6f1a3c2e-4b5d-4e6f-8a9b-0c1d2e3f4a5b", OrganizationID: shadowActivationOrg,
			Kind: workgraph.KindMaterialize, Scope: []byte(`{"llm_provider":"mock","force":true}`),
		},
		Token: "claim-of-the-activation-test",
	}
	specs := []jobruntime.HandlerSpec{{Kind: jobcontract.KindInvestmentMaterialize}}
	cfg := config.Config{Service: "dev-health-worker", ClickHouseURI: secrets.NewValue(instance.URI)}

	evidence := map[string]string{}
	for _, tc := range []struct {
		name         string
		env          map[string]string
		wantSends    int
		wantShadow   uint64
		wantAttempts uint64
	}{
		{name: "every flag off", env: nil},
		{name: "the key alone is not a switch", env: map[string]string{"TYPESAFE_API_KEY": "plain words used as a test value"}},
		{name: "only the shadow switch on", env: map[string]string{
			investment.EnvShadowProvider: "typesafe", investment.EnvShadowOrgIDs: shadowActivationOrg,
			"TYPESAFE_API_KEY": "plain words used as a test value",
		}, wantSends: 1, wantShadow: 1, wantAttempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range shadowEnv {
				t.Setenv(name, "")
			}
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			for _, table := range []string{"work_unit_investment_shadow", "llm_categorization_attempts", "work_unit_investments", "work_unit_investment_quotes", "work_unit_repo_effort"} {
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
			executor, ok := built.(*investment.NativeExecutor)
			if !ok {
				t.Fatalf("the worker built a %T", built)
			}
			recorder := &systemOneRecorder{}
			executor.SetShadowHTTPClientForTest(&http.Client{Transport: recorder})

			out, err := executor.Execute(ctx, claim)
			if err != nil {
				t.Fatalf("Execute: %v\n%s", err, logs.String())
			}
			evidence[tc.name] = string(out)
			if count("work_unit_investments") != 1 {
				t.Fatalf("served rows = %d, want 1: the run measured nothing", count("work_unit_investments"))
			}
			if recorder.count() != tc.wantSends {
				t.Fatalf("sends = %d, want %d\n%s", recorder.count(), tc.wantSends, logs.String())
			}
			if got := count("work_unit_investment_shadow"); got != tc.wantShadow {
				t.Fatalf("shadow rows = %d, want %d", got, tc.wantShadow)
			}
			if got := count("llm_categorization_attempts"); got != tc.wantAttempts {
				t.Fatalf("attempt rows = %d, want %d", got, tc.wantAttempts)
			}
			rendered := collector.PrometheusText()
			if tc.wantSends == 0 {
				if strings.Contains(logs.String(), "shadow") {
					t.Fatalf("an off phase logged:\n%s", logs.String())
				}
				if !strings.Contains(rendered, `dev_health_investment_shadow_phase_stops_total{reason="done"} 0`) {
					t.Fatal("an off phase moved a shadow counter")
				}
				return
			}
			request := recorder.requests[0]
			if request.URL.String() != "https://api.typesafe.ai/v1/systemone" {
				t.Fatalf("the send went to %s", request.URL)
			}
			var state string
			var tokens uint32
			if err := conn.QueryRow(ctx, `SELECT state FROM work_unit_investment_shadow LIMIT 1`).Scan(&state); err != nil || state != "ok" {
				t.Fatalf("shadow state = %q (%v)", state, err)
			}
			if err := conn.QueryRow(ctx, `SELECT input_tokens FROM llm_categorization_attempts LIMIT 1`).Scan(&tokens); err != nil || tokens != 1200 {
				t.Fatalf("attempt tokens = %d (%v)", tokens, err)
			}
			// The worker wired the collector: the phase reached the metrics.
			for _, want := range []string{
				`dev_health_investment_shadow_phase_stops_total{reason="done"} 1`,
				`dev_health_investment_shadow_attempts_total{role="shadow",provider="typesafe",model="jev-1.13.0",state="ok"} 1`,
			} {
				if !strings.Contains(rendered, want+"\n") {
					t.Errorf("the collector of the worker did not record: %s", want)
				}
			}
			if strings.Count(logs.String(), `msg="investment shadow phase complete"`) != 1 {
				t.Fatalf("want one shadow run line:\n%s", logs.String())
			}
		})
	}
	// The ledger evidence is the same bytes with the phase on and off: no
	// shadow object is in output_evidence.
	if evidence["every flag off"] == "" || evidence["every flag off"] != evidence["only the shadow switch on"] {
		t.Fatalf("the ledger evidence differs with the phase on:\n off %s\n on  %s", evidence["every flag off"], evidence["only the shadow switch on"])
	}
	if strings.Contains(evidence["only the shadow switch on"], "shadow") {
		t.Fatal("the ledger evidence names the shadow phase")
	}
	if n := count("llm_token_usage"); n != 0 {
		var providers []string
		rows, _ := conn.Query(ctx, `SELECT DISTINCT provider FROM llm_token_usage`)
		for rows.Next() {
			var provider string
			_ = rows.Scan(&provider)
			providers = append(providers, provider)
		}
		for _, provider := range providers {
			if provider == "typesafe" {
				t.Fatal("llm_token_usage holds a typesafe row")
			}
		}
	}
}
