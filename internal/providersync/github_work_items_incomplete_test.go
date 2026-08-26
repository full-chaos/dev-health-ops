package providersync

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGitHubWorkItemsIncompletePolicyPreservesCompleteWatermark(t *testing.T) {
	t.Parallel()
	watermark := time.Date(2026, 8, 8, 12, 0, 0, 123, time.FixedZone("fixture", 3600))
	result := map[string]any{
		"records":    16,
		"incomplete": []GitHubWorkItemsIncomplete{},
	}

	normalized, gotWatermark, err := applyGitHubWorkItemsIncompletePolicy(
		"github", "work-items", result, &watermark,
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotWatermark == nil || !gotWatermark.Equal(watermark) || gotWatermark.Location() != time.UTC {
		t.Fatalf("watermark=%v want=%v normalized to UTC", gotWatermark, watermark)
	}
	if got, ok := normalized[githubWorkItemsIncompleteResultKey].([]GitHubWorkItemsIncomplete); !ok || len(got) != 0 {
		t.Fatalf("normalized incomplete=%#v", normalized[githubWorkItemsIncompleteResultKey])
	}
	if normalized == nil || reflect.ValueOf(normalized).Pointer() == reflect.ValueOf(result).Pointer() {
		t.Fatal("policy returned the caller-owned result map")
	}
}

func TestGitHubWorkItemsIncompletePolicyWithholdsWatermarkAfterSnapshotJSONRoundTrip(t *testing.T) {
	t.Parallel()
	watermark := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	// json.Unmarshal of the prepared snapshot produces []any/map[string]any,
	// not []GitHubWorkItemsIncomplete. This is the real recovery-side shape.
	result := map[string]any{
		"records": float64(16),
		"incomplete": []any{
			map[string]any{"component": "milestones", "cause": "transient"},
			map[string]any{
				"component": "issue_comments", "subject_id": "42", "cause": "transient",
			},
			map[string]any{"component": githubProjectsV2IncompleteComponent, "cause": githubProjectsV2NullOrganization},
		},
	}

	normalized, gotWatermark, err := applyGitHubWorkItemsIncompletePolicy(
		"github", "work-items", result, &watermark,
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotWatermark != nil {
		t.Fatalf("durable incomplete result retained watermark %v", gotWatermark)
	}
	want := []GitHubWorkItemsIncomplete{
		{Component: "milestones", Cause: "transient"},
		{Component: "issue_comments", SubjectID: "42", Cause: "transient"},
		{Component: githubProjectsV2IncompleteComponent, Cause: githubProjectsV2NullOrganization},
	}
	if got, ok := normalized[githubWorkItemsIncompleteResultKey].([]GitHubWorkItemsIncomplete); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized incomplete=%#v want=%+v", normalized[githubWorkItemsIncompleteResultKey], want)
	}
	if _, stillUntyped := result[githubWorkItemsIncompleteResultKey].([]any); !stillUntyped {
		t.Fatalf("input result was mutated: %#v", result)
	}
}

func TestGitHubWorkItemsIncompletePolicyFailsClosedOnMalformedOrBlockingEvidence(t *testing.T) {
	t.Parallel()
	var typedNil *[]GitHubWorkItemsIncomplete
	tooMany := make([]GitHubWorkItemsIncomplete, maxEffectRows+1)
	for index := range tooMany {
		tooMany[index] = GitHubWorkItemsIncomplete{Component: "milestones", Cause: "transient"}
	}
	tests := map[string]any{
		"nil result":  nil,
		"missing key": map[string]any{},
		"null":        map[string]any{"incomplete": nil},
		"encoded null": map[string]any{
			"incomplete": typedNil,
		},
		"not json encodable": map[string]any{
			"incomplete": make(chan int),
		},
		"wrong shape": map[string]any{"incomplete": map[string]any{}},
		"unknown field": map[string]any{"incomplete": []any{
			map[string]any{"component": "milestones", "cause": "transient", "detail": "secret"},
		}},
		"too many entries": map[string]any{"incomplete": tooMany},
		"blank component": map[string]any{"incomplete": []any{
			map[string]any{"component": "", "cause": "transient"},
		}},
		"padded component": map[string]any{"incomplete": []any{
			map[string]any{"component": " milestones", "cause": "transient"},
		}},
		"unknown component": map[string]any{"incomplete": []any{
			map[string]any{"component": "required_donor", "cause": "transient"},
		}},
		"blocking cause": map[string]any{"incomplete": []any{
			map[string]any{"component": "pr_social", "cause": "pagination_cap"},
		}},
		"blank cause": map[string]any{"incomplete": []any{
			map[string]any{"component": "pr_social", "cause": ""},
		}},
		"padded cause": map[string]any{"incomplete": []any{
			map[string]any{"component": "pr_social", "cause": " transient"},
		}},
		"padded subject": map[string]any{"incomplete": []any{
			map[string]any{"component": "issue_comments", "subject_id": " 42", "cause": "transient"},
		}},
	}
	watermark := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	for name, resultValue := range tests {
		t.Run(name, func(t *testing.T) {
			result, _ := resultValue.(map[string]any)
			_, _, err := applyGitHubWorkItemsIncompletePolicy(
				"github", "work-items", result, &watermark,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want=%v", err, ErrInvalidConfiguration)
			}
		})
	}
}

func TestGitHubWorkItemsIncompletePolicyDoesNotAffectOtherRoutes(t *testing.T) {
	t.Parallel()
	for name, route := range map[string]struct {
		provider string
		dataset  string
	}{
		"other provider": {provider: "gitlab", dataset: "work-items"},
		"other dataset":  {provider: "github", dataset: "prs"},
	} {
		t.Run(name, func(t *testing.T) {
			watermark := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
			result := map[string]any{"records": 1}
			normalized, gotWatermark, err := applyGitHubWorkItemsIncompletePolicy(
				route.provider, route.dataset, result, &watermark,
			)
			if err != nil || reflect.ValueOf(normalized).Pointer() != reflect.ValueOf(result).Pointer() ||
				gotWatermark != &watermark {
				t.Fatalf("result=%v watermark=%v error=%v", normalized, gotWatermark, err)
			}
		})
	}
}

// TestGitHubWorkItemsIncompletePolicyIsAFixedPoint pins CHAOS-3940.
//
// The policy is not applied once. CompleteRouteExecutor.Execute applies it to
// the collected batch, PostgresRepository.Complete applies it again to the
// payload that batch produced, and the prepared-snapshot recovery path applies
// it a third time to the JSON decode of the same value. Its output must
// therefore be accepted by its own input contract.
//
// Before the fix the empty case -- the healthy GitHub work-items unit, where
// no optional enrichment failed -- normalized to a NIL typed slice. json.Marshal
// renders that as `null`, the reader's fail-closed "no durable evidence" shape,
// so the second application refused a unit whose sixteen ClickHouse effects had
// already been committed. Length-only assertions cannot see this: a nil slice
// and an empty slice both have len 0 and only differ once they are marshalled.
func TestGitHubWorkItemsIncompletePolicyIsAFixedPoint(t *testing.T) {
	t.Parallel()
	for name, seed := range map[string]any{
		// Exactly what GitHubWorkItemsRouteHandler.Collect emits when every
		// optional phase succeeded (github_work_items_route.go:238,402).
		"healthy empty batch": []GitHubWorkItemsIncomplete{},
		"durable evidence": []GitHubWorkItemsIncomplete{
			{Component: "milestones", Cause: "transient"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			watermark := time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC)
			result := map[string]any{
				"records":                          3,
				githubWorkItemsIncompleteResultKey: seed,
			}

			// 1. CompleteRouteExecutor.Execute (complete_route.go).
			executed, executedWatermark, err := applyGitHubWorkItemsIncompletePolicy(
				"github", "work-items", result, &watermark,
			)
			if err != nil {
				t.Fatalf("executor application rejected a well-formed batch: %v", err)
			}
			// A nil slice marshals to `null`; an empty slice marshals to `[]`.
			// Assert the wire shape, not the length.
			encoded, err := json.Marshal(executed[githubWorkItemsIncompleteResultKey])
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(encoded, []byte("null")) {
				t.Fatalf(
					"normalized incomplete evidence marshals to null: %#v",
					executed[githubWorkItemsIncompleteResultKey],
				)
			}

			// 2. PostgresRepository.Complete (repository_postgres.go:109),
			//    on the very map the executor produced.
			completed, _, err := applyGitHubWorkItemsIncompletePolicy(
				"github", "work-items", executed, executedWatermark,
			)
			if err != nil {
				t.Fatalf("completion application refused the executor's own output: %v", err)
			}

			// 3. Prepared-snapshot recovery: the executor's batch is persisted
			//    as JSON and decoded back before the policy runs on it again
			//    (complete_route.go:249). A failure here does not even surface
			//    as this error -- the recovery path converts it to
			//    ErrEffectLedgerConflict, which is what wedged every retry.
			raw, err := json.Marshal(executed)
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if _, _, err := applyGitHubWorkItemsIncompletePolicy(
				"github", "work-items", decoded, executedWatermark,
			); err != nil {
				t.Fatalf("recovery application refused the persisted snapshot: %v", err)
			}
			if _, present := completed[githubWorkItemsIncompleteResultKey]; !present {
				t.Fatal("completion application dropped the evidence key")
			}
		})
	}
}
