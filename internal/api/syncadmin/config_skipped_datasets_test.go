package syncadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func skipTestConfig(provider string, active bool, keys ...string) *syncConfig {
	targets, options := `[]`, `{}`
	integration := uuid.New()
	return &syncConfig{
		ID: uuid.New(), Name: provider, Provider: provider, SyncTargets: &targets, SyncOptions: &options,
		IsActive: active, IntegrationID: &integration, enabledDatasetKeys: keys,
	}
}

func skipTestBody(t *testing.T, config *syncConfig) string {
	t.Helper()
	body, err := syncConfigResponse(config, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return marshal(t, body)
}

func TestConfigStatusServesTheReasonWhenEveryDatasetIsGated(t *testing.T) {
	calls := 0
	h := &handlers{features: fixedFeatures{allowed: false, called: &calls}, logger: quiet()}
	config := skipTestConfig("jira", true, "incidents")
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), config); err != nil {
		t.Fatal(err)
	}
	text := skipTestBody(t, config)
	for _, want := range []string{`"skipped_datasets":[`, `"dataset_key":"incidents"`, `"reason":"feature_disabled"`, `"message":"skipped: feature off"`} {
		if !strings.Contains(text, want) {
			t.Errorf("the config response lacks %s: %s", want, text)
		}
	}
}

func TestConfigStatusServesNoReasonWhenTheFeatureIsOn(t *testing.T) {
	h := &handlers{features: fixedFeatures{allowed: true}, logger: quiet()}
	config := skipTestConfig("jira", true, "incidents")
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), config); err != nil {
		t.Fatal(err)
	}
	if text := skipTestBody(t, config); strings.Contains(text, "skipped_datasets") {
		t.Errorf("feature on, yet the response names a skip: %s", text)
	}
}

// With only some datasets gated, the config read names only those. The config
// read is the configuration's current state; a run's own result (served on
// the run routes) is the history of that run. Each response has one source,
// so a dataset is never listed twice in one response.
func TestConfigStatusNamesOnlyTheGatedDatasetsOnceEvenWhenTheRunCarriesThem(t *testing.T) {
	h := &handlers{features: fixedFeatures{allowed: false}, logger: quiet()}
	config := skipTestConfig("gitlab", true, "commits", "incidents", "work-items")
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), config); err != nil {
		t.Fatal(err)
	}
	text := skipTestBody(t, config)
	if got := strings.Count(text, `"dataset_key"`); got != 1 || !strings.Contains(text, `"dataset_key":"incidents"`) {
		t.Errorf("want exactly the incidents entry, got %d: %s", got, text)
	}
	result := skippedDatasetsResult
	run, err := syncRunResponse(&syncRun{ID: uuid.Nil, Status: "planned", Result: &result})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(marshal(t, run), `"dataset_key"`); got != 1 {
		t.Errorf("the run response lists the entry %d times", got)
	}
}

func TestConfigStatusReadsTheDecisionOnceAndOnlyForGatedDatasets(t *testing.T) {
	calls := 0
	h := &handlers{features: fixedFeatures{allowed: false, called: &calls}, logger: quiet()}
	plain := skipTestConfig("github", true, "commits", "prs")
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), plain); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || strings.Contains(skipTestBody(t, plain), "skipped_datasets") {
		t.Errorf("no gated dataset: %d decisions", calls)
	}
	first, second := skipTestConfig("jira", true, "incidents"), skipTestConfig("gitlab", true, "incidents")
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), first, second, plain); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("want one decision for the request, got %d", calls)
	}
	if len(first.skippedDatasets) != 1 || len(second.skippedDatasets) != 1 || len(plain.skippedDatasets) != 0 {
		t.Errorf("skips: %v %v %v", first.skippedDatasets, second.skippedDatasets, plain.skippedDatasets)
	}
}

func TestConfigStatusServesNoReasonForAnInactiveConfig(t *testing.T) {
	h := &handlers{features: fixedFeatures{allowed: false}, logger: quiet()}
	config := skipTestConfig("jira", false, "incidents")
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), config); err != nil {
		t.Fatal(err)
	}
	if len(config.skippedDatasets) != 0 {
		t.Errorf("inactive config carries a skip: %v", config.skippedDatasets)
	}
}

type keysReader struct {
	faultReader
	keys  []string
	reads int
}

func (r *keysReader) enabledDatasetKeys(_ context.Context, _ string, ids []uuid.UUID) (map[uuid.UUID][]string, error) {
	r.reads++
	out := map[uuid.UUID][]string{}
	for _, id := range ids {
		out[id] = r.keys
	}
	return out, nil
}

func skipTestSourceConfig(provider, targets string, keys ...string) *syncConfig {
	config := skipTestConfig(provider, true)
	integration, source := uuid.New(), uuid.New()
	config.IntegrationID, config.SourceID = &integration, &source
	config.SyncTargets = &targets
	return config
}

// A source-scoped config is not owned by dataset rows, yet the scheduler plans
// the enabled rows its stored targets name and gates them: the config read
// serves the same reason, from the same filter.
func TestConfigStatusServesTheReasonForASourceScopedConfig(t *testing.T) {
	reader := &keysReader{keys: []string{"incidents", "work-items"}}
	h := &handlers{store: reader, features: fixedFeatures{allowed: false}, logger: quiet()}
	incidents := skipTestSourceConfig("jira", `["incidents"]`)
	workItems := skipTestSourceConfig("jira", `["work-items"]`)
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), incidents, workItems); err != nil {
		t.Fatal(err)
	}
	text := skipTestBody(t, incidents)
	if got := strings.Count(text, `"dataset_key"`); got != 1 || !strings.Contains(text, `"dataset_key":"incidents"`) {
		t.Errorf("incidents-scoped config: want the incidents entry once, got %d: %s", got, text)
	}
	if len(workItems.skippedDatasets) != 0 {
		t.Errorf("a work-items-scoped config carries a skip: %v", workItems.skippedDatasets)
	}
	if reader.reads != 1 {
		t.Errorf("want one enabled-rows read for the request, got %d", reader.reads)
	}
}

func TestConfigStatusServesNoReasonForASourceScopedConfigWhenTheFeatureIsOn(t *testing.T) {
	h := &handlers{store: &keysReader{keys: []string{"incidents"}}, features: fixedFeatures{allowed: true}, logger: quiet()}
	config := skipTestSourceConfig("jira", `["incidents"]`)
	if err := h.deriveSkippedDatasets(context.Background(), uuid.NewString(), config); err != nil {
		t.Fatal(err)
	}
	if len(config.skippedDatasets) != 0 {
		t.Errorf("feature on, yet a skip: %v", config.skippedDatasets)
	}
}
