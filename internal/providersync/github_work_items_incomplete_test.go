package providersync

import (
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
