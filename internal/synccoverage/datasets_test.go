package synccoverage

import (
	"encoding/json"
	"testing"
)

// CHAOS-4393 red-on-baseline repro: effectiveDatasetKeys/queryDatasetKeys
// hand-maintained a work-items-only fold (CHAOS-2721) and never learned the
// CHAOS-4078/PR #1945 PR-social (prs/pr-reviews/pr-comments) and TestOps
// (cicd/tests) folds that internal/providerfamilycontract already declares.
// A composite "cicd" SUCCESS unit carrying family_dataset_tests=true must
// close the "tests" coverage gap the same way a composite "work-items" unit
// closes a work-item-family child gap.

func TestEffectiveDatasetKeysExpandsTestOpsFold(t *testing.T) {
	flags, err := json.Marshal(map[string]bool{"family_dataset_tests": true})
	if err != nil {
		t.Fatalf("marshal flags: %v", err)
	}
	got := effectiveDatasetKeys("cicd", flags)
	if len(got) != 1 || got[0] != "tests" {
		t.Fatalf("effectiveDatasetKeys(\"cicd\", family_dataset_tests=true) = %v, want [tests]", got)
	}
}

func TestEffectiveDatasetKeysExpandsPRSocialFold(t *testing.T) {
	flags, err := json.Marshal(map[string]bool{"family_dataset_pr_comments": true})
	if err != nil {
		t.Fatalf("marshal flags: %v", err)
	}
	got := effectiveDatasetKeys("prs", flags)
	if len(got) != 1 || got[0] != "pr-comments" {
		t.Fatalf("effectiveDatasetKeys(\"prs\", family_dataset_pr_comments=true) = %v, want [pr-comments]", got)
	}
}

func TestEffectiveDatasetKeysFallsBackToRawKeyWhenNoTestOpsFlagTrue(t *testing.T) {
	flags, err := json.Marshal(map[string]bool{"family_dataset_tests": false})
	if err != nil {
		t.Fatalf("marshal flags: %v", err)
	}
	got := effectiveDatasetKeys("cicd", flags)
	if len(got) != 1 || got[0] != "cicd" {
		t.Fatalf("effectiveDatasetKeys(\"cicd\", family_dataset_tests=false) = %v, want [cicd]", got)
	}
}

func TestEffectiveDatasetKeysStillExpandsWorkItemsFold(t *testing.T) {
	// CHAOS-2721 behavior must survive the generalization.
	flags, err := json.Marshal(map[string]bool{"family_dataset_work_item_comments": true})
	if err != nil {
		t.Fatalf("marshal flags: %v", err)
	}
	got := effectiveDatasetKeys("work-items", flags)
	if len(got) != 1 || got[0] != "work-item-comments" {
		t.Fatalf("effectiveDatasetKeys(\"work-items\", ...) = %v, want [work-item-comments]", got)
	}
}

func TestQueryDatasetKeysIncludesCanonicalCicdForTestsChild(t *testing.T) {
	got := queryDatasetKeys([]string{"tests"})
	if !containsString(got, "cicd") || !containsString(got, "tests") {
		t.Fatalf("queryDatasetKeys([\"tests\"]) = %v, want to include both \"cicd\" and \"tests\"", got)
	}
}

func TestQueryDatasetKeysIncludesCanonicalPrsForPrCommentsChild(t *testing.T) {
	got := queryDatasetKeys([]string{"pr-comments"})
	if !containsString(got, "prs") || !containsString(got, "pr-comments") {
		t.Fatalf("queryDatasetKeys([\"pr-comments\"]) = %v, want to include both \"prs\" and \"pr-comments\"", got)
	}
}

func TestQueryDatasetKeysStillIncludesCanonicalWorkItemsForFamilyChild(t *testing.T) {
	got := queryDatasetKeys([]string{"work-item-comments"})
	if !containsString(got, "work-items") || !containsString(got, "work-item-comments") {
		t.Fatalf("queryDatasetKeys([\"work-item-comments\"]) = %v, want to include both \"work-items\" and \"work-item-comments\"", got)
	}
}

func TestQueryDatasetKeysUnchangedForNonFamilyScope(t *testing.T) {
	got := queryDatasetKeys([]string{"commits", "blame"})
	if len(got) != 2 || !containsString(got, "commits") || !containsString(got, "blame") {
		t.Fatalf("queryDatasetKeys([\"commits\",\"blame\"]) = %v, want unchanged [blame commits]", got)
	}
}
