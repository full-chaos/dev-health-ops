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
	// CHAOS-4078 gave prs/cicd/tests their own FoldContributing policy (see
	// TestFoldContributingPRSocialAndTestOpsPoliciesValidateSubsetsAndRejectCrossFamilyFlags
	// below); commits/security/blame remain genuinely outside any family.
	for _, dataset := range []string{"commits", "security", "blame"} {
		if _, ok := PolicyFor("github", dataset); ok {
			t.Fatalf("github/%s unexpectedly acquired a family policy", dataset)
		}
		if err := ValidateClaim("github", dataset, nil, true); err != nil {
			t.Fatalf("github/%s error=%v", dataset, err)
		}
	}
}

func TestFoldContributingPRSocialAndTestOpsPoliciesValidateSubsetsAndRejectCrossFamilyFlags(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"github", "gitlab"} {
		provider := provider
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			for _, family := range []struct {
				canonical string
				aliases   []string
			}{
				{canonical: "prs", aliases: []string{"pr-reviews", "pr-comments"}},
				{canonical: "cicd", aliases: []string{"tests"}},
			} {
				policy, ok := PolicyFor(provider, family.canonical)
				if !ok || policy.Mode != FoldContributing || policy.CanonicalDataset != family.canonical {
					t.Fatalf("%s policy=%+v ok=%t", family.canonical, policy, ok)
				}
				// A canonical-only claim with no family flags at all is valid
				// -- no aliases were enabled, so nothing folded.
				if err := ValidateClaim(provider, family.canonical, nil, true); err != nil {
					t.Fatalf("%s bare canonical error=%v", family.canonical, err)
				}
				// A subset -- ANY non-empty subset, unlike the atomic family
				// -- is valid, one alias at a time.
				for _, alias := range family.aliases {
					subset := map[string]bool{familyDatasetFlag(alias): true}
					if err := ValidateClaim(provider, family.canonical, subset, true); err != nil {
						t.Fatalf("%s subset=%q error=%v", family.canonical, alias, err)
					}
				}
				// Every alias enabled at once is valid too.
				all := map[string]bool{}
				for _, alias := range family.aliases {
					all[familyDatasetFlag(alias)] = true
				}
				if err := ValidateClaim(provider, family.canonical, all, true); err != nil {
					t.Fatalf("%s all-aliases error=%v", family.canonical, err)
				}
				// A direct alias-keyed claim is always malformed -- folding
				// happens onto the canonical identity only.
				for _, alias := range family.aliases {
					if err := ValidateClaim(provider, alias, all, true); !errors.Is(err, ErrInvalidClaim) {
						t.Fatalf("direct alias %q error=%v", alias, err)
					}
				}
				// An unknown family_dataset_* flag fails closed.
				unknown := map[string]bool{"family_dataset_unknown": true}
				if err := ValidateClaim(provider, family.canonical, unknown, true); !errors.Is(err, ErrInvalidClaim) {
					t.Fatalf("%s unknown flag error=%v", family.canonical, err)
				}
				// A known family flag explicitly present but set to false fails
				// closed too -- a bare `map[string]bool` can store that value,
				// and completion must never silently treat it as "absent"
				// (codex round 3 finding #2).
				for _, alias := range family.aliases {
					explicitFalse := map[string]bool{familyDatasetFlag(alias): false}
					if err := ValidateClaim(provider, family.canonical, explicitFalse, true); !errors.Is(err, ErrInvalidClaim) {
						t.Fatalf("%s explicit-false flag=%q error=%v", family.canonical, alias, err)
					}
				}
			}
			// A canonical "prs" claim carrying "cicd"'s own flag (or vice
			// versa) is cross-family contamination and must fail closed --
			// this is the exact CHAOS-4078 review finding: without this
			// check the flag would sail through to provider execution.
			crossFamily := map[string]bool{"family_dataset_tests": true}
			if err := ValidateClaim(provider, "prs", crossFamily, true); !errors.Is(err, ErrInvalidClaim) {
				t.Fatalf("prs claim with cicd's own flag error=%v", err)
			}
			crossFamilyReverse := map[string]bool{"family_dataset_pr_comments": true}
			if err := ValidateClaim(provider, "cicd", crossFamilyReverse, true); !errors.Is(err, ErrInvalidClaim) {
				t.Fatalf("cicd claim with prs's own flag error=%v", err)
			}
		})
	}
}
