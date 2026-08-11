package providersync

import (
	"errors"
	"maps"
	"testing"
)

func TestValidateGitHubWorkItemExecutionClaimRequiresTheAtomicFamily(t *testing.T) {
	t.Parallel()
	complete := map[string]bool{
		"family_dataset_work_items":         true,
		"family_dataset_work_item_labels":   true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history":  true,
		"family_dataset_work_item_comments": true,
	}
	for name, test := range map[string]struct {
		provider string
		dataset  string
		flags    map[string]bool
		valid    bool
	}{
		"canonical all five flags": {
			provider: "github", dataset: "work-items", flags: complete, valid: true,
		},
		"canonical permits unrelated producer flags": {
			provider: "github", dataset: "work-items",
			flags: func() map[string]bool {
				flags := maps.Clone(complete)
				flags["sync_prs"] = false
				return flags
			}(),
			valid: true,
		},
		"direct work item labels alias": {
			provider: "github", dataset: "work-item-labels", flags: complete,
		},
		"direct work item projects alias": {
			provider: "github", dataset: "work-item-projects", flags: complete,
		},
		"direct work item history alias": {
			provider: "github", dataset: "work-item-history", flags: complete,
		},
		"direct work item comments alias": {
			provider: "github", dataset: "work-item-comments", flags: complete,
		},
		"canonical missing flag": {
			provider: "github", dataset: "work-items",
			flags: map[string]bool{
				"family_dataset_work_items":         true,
				"family_dataset_work_item_labels":   true,
				"family_dataset_work_item_projects": true,
				"family_dataset_work_item_history":  true,
			},
		},
		"canonical false flag": {
			provider: "github", dataset: "work-items",
			flags: map[string]bool{
				"family_dataset_work_items":         true,
				"family_dataset_work_item_labels":   true,
				"family_dataset_work_item_projects": true,
				"family_dataset_work_item_history":  true,
				"family_dataset_work_item_comments": false,
			},
		},
		"canonical unexpected family flag": {
			provider: "github", dataset: "work-items",
			flags: func() map[string]bool {
				flags := maps.Clone(complete)
				flags["family_dataset_unknown"] = true
				return flags
			}(),
		},
		"other GitHub route remains independent": {
			provider: "github", dataset: "repo-metadata", flags: nil, valid: true,
		},
		"other provider work item family remains independent": {
			provider: "gitlab", dataset: "work-items", flags: nil, valid: true,
		},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := ValidateGitHubWorkItemExecutionClaim(test.provider, test.dataset, test.flags)
			if test.valid {
				if err != nil {
					t.Fatalf("error=%v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}
