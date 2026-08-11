package providerfamilycontract

import (
	"errors"
	"maps"
	"testing"
)

func TestAtomicWorkItemPoliciesAreProviderNeutralAndExact(t *testing.T) {
	t.Parallel()
	complete := map[string]bool{
		"family_dataset_work_items":         true,
		"family_dataset_work_item_labels":   true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history":  true,
		"family_dataset_work_item_comments": true,
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		provider := provider
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			if err := ValidateClaim(provider, "work-items", complete, true); err != nil {
				t.Fatalf("complete claim error=%v", err)
			}
			for _, alias := range []string{
				"work-item-labels", "work-item-projects",
				"work-item-history", "work-item-comments",
			} {
				if err := ValidateClaim(provider, alias, complete, true); !errors.Is(err, ErrInvalidClaim) {
					t.Fatalf("direct alias %q error=%v", alias, err)
				}
			}
			for flag := range complete {
				missing := maps.Clone(complete)
				delete(missing, flag)
				if err := ValidateClaim(provider, "work-items", missing, true); !errors.Is(err, ErrInvalidClaim) {
					t.Fatalf("missing %q error=%v", flag, err)
				}
				falseFlag := maps.Clone(complete)
				falseFlag[flag] = false
				if err := ValidateClaim(provider, "work-items", falseFlag, true); !errors.Is(err, ErrInvalidClaim) {
					t.Fatalf("false %q error=%v", flag, err)
				}
			}
			unknown := maps.Clone(complete)
			unknown["family_dataset_unknown"] = true
			if err := ValidateClaim(provider, "work-items", unknown, true); !errors.Is(err, ErrInvalidClaim) {
				t.Fatalf("unknown family flag error=%v", err)
			}
		})
	}
}

func TestDefaultOffAtomicPoliciesPreserveLegacyClaims(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"gitlab", "jira", "linear"} {
		if err := ValidateClaim(
			provider,
			"work-items",
			map[string]bool{"family_dataset_work_items": true},
			false,
		); err != nil {
			t.Fatalf("provider=%s error=%v", provider, err)
		}
	}
}

func TestPagerDutyIncidentPolicyPreservesIndependentD16Claims(t *testing.T) {
	t.Parallel()
	for _, dataset := range []string{
		"incidents", "incident-alerts", "incident-log-entries", "incident-notes",
	} {
		policy, ok := PolicyFor("pagerduty", dataset)
		if !ok || policy.Mode != Independent {
			t.Fatalf("dataset=%s policy=%+v ok=%t", dataset, policy, ok)
		}
		if err := ValidateClaim("pagerduty", dataset, nil, true); err != nil {
			t.Fatalf("dataset=%s error=%v", dataset, err)
		}
	}
}

func TestIndependentProviderRoutesRemainOutsideFamilyAdmission(t *testing.T) {
	t.Parallel()
	for _, dataset := range []string{"prs", "cicd", "tests"} {
		if _, ok := PolicyFor("github", dataset); ok {
			t.Fatalf("github/%s unexpectedly acquired a family policy", dataset)
		}
		if err := ValidateClaim("github", dataset, nil, true); err != nil {
			t.Fatalf("github/%s error=%v", dataset, err)
		}
	}
}
