package providersync

import (
	"errors"
	"reflect"
	"testing"
)

func TestWorkItemAliasCompletionMetadata(t *testing.T) {
	tests := []struct {
		name      string
		provider  string
		dataset   string
		flags     map[string]bool
		result    map[string]any
		wantKeys  []string
		wantAudit []string
		wantErr   error
	}{
		{
			name:     "plain unit preserves its own identity",
			provider: "github", dataset: "commits",
			flags:  map[string]bool{"sync_commits": true},
			result: map[string]any{"records": 2}, wantKeys: []string{"commits"},
		},
		{
			name:     "all enabled aliases use canonical order",
			provider: "github", dataset: "work-items",
			flags: map[string]bool{
				"sync_prs":                          true,
				"family_dataset_work_item_comments": true,
				"family_dataset_work_items":         true,
				"family_dataset_work_item_history":  true,
				"family_dataset_work_item_projects": true,
				"family_dataset_work_item_labels":   true,
			},
			result: map[string]any{"records": 7},
			wantKeys: []string{
				"work-items", "work-item-labels", "work-item-projects",
				"work-item-history", "work-item-comments",
			},
			wantAudit: []string{
				"work-items", "work-item-labels", "work-item-projects",
				"work-item-history", "work-item-comments",
			},
		},
		{
			name:     "enabled subset excludes false aliases",
			provider: "github", dataset: "work-items",
			flags: map[string]bool{
				"family_dataset_work_items":         false,
				"family_dataset_work_item_labels":   true,
				"family_dataset_work_item_projects": false,
				"family_dataset_work_item_history":  true,
				"family_dataset_work_item_comments": false,
			},
			result:    map[string]any{"records": 3},
			wantKeys:  []string{"work-item-labels", "work-item-history"},
			wantAudit: []string{"work-item-labels", "work-item-history"},
		},
		{
			name:     "missing family encoding fails closed",
			provider: "github", dataset: "work-items",
			flags: map[string]bool{"sync_prs": true}, result: map[string]any{"records": 1},
			wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "all aliases disabled fails closed",
			provider: "github", dataset: "work-items",
			flags:  map[string]bool{"family_dataset_work_items": false},
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "unknown enabled alias fails closed",
			provider: "github", dataset: "work-items",
			flags: map[string]bool{
				"family_dataset_work_items": true,
				"family_dataset_unknown":    true,
			},
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "unknown disabled alias also fails closed",
			provider: "github", dataset: "work-items",
			flags: map[string]bool{
				"family_dataset_work_items": true,
				"family_dataset_unknown":    false,
			},
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "family flag on noncanonical claim fails closed",
			provider: "github", dataset: "work-item-comments",
			flags:  map[string]bool{"family_dataset_work_item_comments": true},
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "handler cannot predeclare completion audit",
			provider: "github", dataset: "work-items",
			flags: map[string]bool{"family_dataset_work_items": true},
			result: map[string]any{
				"records": 1, "family_datasets": []string{"work-items"},
			},
			wantErr: ErrInvalidConfiguration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := cloneStringAnyMap(test.result)
			keys, audited, err := workItemAliasCompletionMetadata(
				test.provider, test.dataset, test.flags, test.result,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error=%v want=%v", err, test.wantErr)
			}
			if test.wantErr != nil {
				if keys != nil || audited != nil {
					t.Fatalf("failed metadata returned keys=%v result=%v", keys, audited)
				}
				return
			}
			if !reflect.DeepEqual(keys, test.wantKeys) {
				t.Fatalf("keys=%v want=%v", keys, test.wantKeys)
			}
			if !reflect.DeepEqual(test.result, original) {
				t.Fatalf("input result mutated: got=%v want=%v", test.result, original)
			}
			if test.wantAudit == nil {
				if _, exists := audited[workItemFamilyAuditResultKey]; exists {
					t.Fatalf("plain result unexpectedly gained family audit: %v", audited)
				}
				return
			}
			gotAudit, ok := audited[workItemFamilyAuditResultKey].([]string)
			if !ok || !reflect.DeepEqual(gotAudit, test.wantAudit) {
				t.Fatalf("audit=%#v want=%v", audited[workItemFamilyAuditResultKey], test.wantAudit)
			}
		})
	}
}

func TestWorkItemAliasProcessorFlagsRejectNonBooleanEncoding(t *testing.T) {
	var flags map[string]bool
	err := decodeClaimJSON(
		[]byte(`{"family_dataset_work_items":"true"}`),
		&flags,
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want=%v", err, ErrInvalidConfiguration)
	}
}

func cloneStringAnyMap(input map[string]any) map[string]any {
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
