package providersync

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
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
			flags:     allWorkItemFamilyFlags(true),
			result:    map[string]any{"records": 7},
			wantKeys:  workitemcontract.FamilyDatasets(),
			wantAudit: workitemcontract.FamilyDatasets(),
		},
		{
			name:     "non github subset excludes false aliases",
			provider: "gitlab", dataset: "work-items",
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
			provider: "gitlab", dataset: "work-items",
			flags:  map[string]bool{"family_dataset_work_items": false},
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "unknown enabled alias fails closed",
			provider: "github", dataset: "work-items",
			flags:  workItemFamilyFlagsWithUnknown(true),
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "unknown disabled alias also fails closed",
			provider: "github", dataset: "work-items",
			flags:  workItemFamilyFlagsWithUnknown(false),
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
			flags: allWorkItemFamilyFlags(true),
			result: map[string]any{
				"records": 1, "family_datasets": []string{"work-items"},
			},
			wantErr: ErrInvalidConfiguration,
		},
		// CHAOS-4078: PR-social and TestOps folds are non-atomic -- unlike
		// work-items, a subset of members present is the NORMAL shape, not a
		// malformed claim.
		{
			name:     "pr-social fold fans back only the enabled alias",
			provider: "github", dataset: "prs",
			flags:     map[string]bool{"family_dataset_pr_comments": true, "sync_prs": true},
			result:    map[string]any{"prs_synced": 3, "pr_reviews_synced": 0},
			wantKeys:  []string{"pr-comments"},
			wantAudit: []string{"pr-comments"},
		},
		{
			name:     "pr-social fold fans back multiple enabled aliases in family order",
			provider: "gitlab", dataset: "prs",
			flags: map[string]bool{
				"family_dataset_pr_reviews":  true,
				"family_dataset_pr_comments": true,
			},
			result:    map[string]any{"records": 4},
			wantKeys:  []string{"pr-reviews", "pr-comments"},
			wantAudit: []string{"pr-reviews", "pr-comments"},
		},
		{
			name:     "prs claim with no fold flags fans back only its own identity",
			provider: "github", dataset: "prs",
			flags:  map[string]bool{"sync_prs": true},
			result: map[string]any{"records": 1}, wantKeys: []string{"prs"},
		},
		{
			name:     "testops fold fans back only the enabled alias",
			provider: "github", dataset: "cicd",
			flags:     map[string]bool{"family_dataset_tests": true, "sync_cicd": true},
			result:    map[string]any{"records": 5},
			wantKeys:  []string{"tests"},
			wantAudit: []string{"tests"},
		},
		{
			name:     "testops fold predeclared audit fails closed",
			provider: "gitlab", dataset: "cicd",
			flags: map[string]bool{"family_dataset_tests": true},
			result: map[string]any{
				"records": 1, "family_datasets": []string{"cicd"},
			},
			wantErr: ErrInvalidConfiguration,
		},
		{
			name:     "unknown fold flag fails closed",
			provider: "github", dataset: "prs",
			flags:  map[string]bool{"family_dataset_bogus": true},
			result: map[string]any{"records": 1}, wantErr: ErrInvalidConfiguration,
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

func TestGitHubWorkItemAliasCompletionRequiresAtomicFamily(t *testing.T) {
	allEnabled := allWorkItemFamilyFlags(true)

	for _, familyDataset := range workitemcontract.FamilyDatasets() {
		flag := expectedWorkItemFamilyFlag(familyDataset)
		for _, mutation := range []struct {
			name  string
			apply func(map[string]bool)
		}{
			{
				name: "omitted",
				apply: func(flags map[string]bool) {
					delete(flags, flag)
				},
			},
			{
				name: "disabled",
				apply: func(flags map[string]bool) {
					flags[flag] = false
				},
			},
		} {
			mutation := mutation
			t.Run(flag+"_"+mutation.name, func(t *testing.T) {
				flags := make(map[string]bool, len(allEnabled))
				for flag, enabled := range allEnabled {
					flags[flag] = enabled
				}
				mutation.apply(flags)

				keys, audited, err := workItemAliasCompletionMetadata(
					"github", "work-items", flags, map[string]any{"records": 1},
				)
				if !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("error=%v want=%v flags=%v", err, ErrInvalidConfiguration, flags)
				}
				if keys != nil || audited != nil {
					t.Fatalf("failed metadata returned keys=%v result=%v", keys, audited)
				}
			})
		}
	}
}

func TestWorkItemFamilyFlagDerivationMatchesProcessorContract(t *testing.T) {
	t.Parallel()
	for _, dataset := range workitemcontract.FamilyDatasets() {
		if got, want := workItemFamilyFlagForDataset(dataset), expectedWorkItemFamilyFlag(dataset); got != want {
			t.Fatalf("family flag for %q=%q want=%q", dataset, got, want)
		}
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

func allWorkItemFamilyFlags(enabled bool) map[string]bool {
	flags := make(map[string]bool, len(workitemcontract.FamilyDatasets()))
	for _, dataset := range workitemcontract.FamilyDatasets() {
		flags[expectedWorkItemFamilyFlag(dataset)] = enabled
	}
	return flags
}

func workItemFamilyFlagsWithUnknown(enabled bool) map[string]bool {
	flags := allWorkItemFamilyFlags(true)
	flags["family_dataset_unknown"] = enabled
	return flags
}

func expectedWorkItemFamilyFlag(dataset string) string {
	return "family_dataset_" + strings.ReplaceAll(dataset, "-", "_")
}
