package investmentexplain

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

type attributionGolden struct {
	MajorityTeamForIssues map[string]struct {
		Input struct {
			IssueIDs []string `json:"issue_ids"`
			TeamMap  map[string]struct {
				TeamID   string `json:"team_id"`
				TeamName string `json:"team_name"`
			} `json:"team_map"`
		} `json:"input"`
		TeamID   string `json:"team_id"`
		TeamName string `json:"team_name"`
	} `json:"majority_team_for_issues"`

	PRRefWorkItemID map[string]struct {
		Input struct {
			PRRef          string               `json:"pr_ref"`
			RepoIdentities map[string][2]string `json:"repo_identities"`
		} `json:"input"`
		Result *string `json:"result"`
	} `json:"pr_ref_work_item_id"`

	ExtractIssueIDs map[string]struct {
		Input struct {
			Payload string `json:"payload"`
		} `json:"input"`
		Result []string `json:"result"`
	} `json:"extract_issue_ids"`

	ExtractPRRefs map[string]struct {
		Input struct {
			Payload string `json:"payload"`
		} `json:"input"`
		Result []string `json:"result"`
	} `json:"extract_pr_refs"`

	ParseDistribution map[string]struct {
		Input struct {
			Value string `json:"value"`
		} `json:"input"`
		Result map[string]float64 `json:"result"`
	} `json:"parse_distribution"`

	MatchesCategoryFilter map[string]struct {
		Input struct {
			ThemeDistribution       map[string]float64 `json:"theme_distribution"`
			SubcategoryDistribution map[string]float64 `json:"subcategory_distribution"`
			Themes                  []string           `json:"themes"`
			Subcategories           []string           `json:"subcategories"`
		} `json:"input"`
		Result bool `json:"result"`
	} `json:"matches_category_filter"`

	SplitCategoryFilters map[string]struct {
		Input struct {
			WorkCategory []string `json:"work_category"`
		} `json:"input"`
		Themes        []string `json:"themes"`
		Subcategories []string `json:"subcategories"`
	} `json:"split_category_filters"`

	DeriveWorkItemID map[string]struct {
		Input struct {
			System       string  `json:"system"`
			Instance     string  `json:"instance"`
			ExternalKey  string  `json:"external_key"`
			WorkItemType *string `json:"work_item_type"`
		} `json:"input"`
		Result string `json:"result"`
	} `json:"derive_work_item_id"`
}

func loadAttributionGolden(t *testing.T) attributionGolden {
	t.Helper()
	data, err := os.ReadFile("testdata/workunit_attribution.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden attributionGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return golden
}

func TestMajorityTeamForIssuesMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.MajorityTeamForIssues {
		t.Run(name, func(t *testing.T) {
			teamMap := map[string]teamAssignment{}
			for id, v := range tc.Input.TeamMap {
				teamMap[id] = teamAssignment{TeamID: v.TeamID, TeamName: v.TeamName}
			}
			gotID, gotLabel := majorityTeamForIssues(tc.Input.IssueIDs, teamMap)
			if gotID != tc.TeamID || gotLabel != tc.TeamName {
				t.Fatalf("case %q: want (%q, %q), got (%q, %q)", name, tc.TeamID, tc.TeamName, gotID, gotLabel)
			}
		})
	}
}

func TestPRRefWorkItemIDMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.PRRefWorkItemID {
		t.Run(name, func(t *testing.T) {
			identities := map[string]repoIdentity{}
			for id, v := range tc.Input.RepoIdentities {
				identities[id] = repoIdentity{Slug: v[0], Provider: v[1]}
			}
			got, ok := prRefWorkItemID(tc.Input.PRRef, identities)
			if tc.Result == nil {
				if ok {
					t.Fatalf("case %q: want no result, got %q", name, got)
				}
				return
			}
			if !ok || got != *tc.Result {
				t.Fatalf("case %q: want %q, got %q (ok=%v)", name, *tc.Result, got, ok)
			}
		})
	}
}

func TestExtractIssueIDsMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.ExtractIssueIDs {
		t.Run(name, func(t *testing.T) {
			got := extractIssueIDs(tc.Input.Payload)
			want := tc.Result
			if len(got) == 0 && len(want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("case %q: want %v, got %v", name, want, got)
			}
		})
	}
}

func TestExtractPRRefsMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.ExtractPRRefs {
		t.Run(name, func(t *testing.T) {
			got := extractPRRefs(tc.Input.Payload)
			want := tc.Result
			if len(got) == 0 && len(want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("case %q: want %v, got %v", name, want, got)
			}
		})
	}
}

func TestParseDistributionMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.ParseDistribution {
		t.Run(name, func(t *testing.T) {
			got := parseDistribution(tc.Input.Value)
			if !reflect.DeepEqual(got, tc.Result) {
				t.Fatalf("case %q: want %v, got %v", name, tc.Result, got)
			}
		})
	}
}

func TestMatchesCategoryFilterMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.MatchesCategoryFilter {
		t.Run(name, func(t *testing.T) {
			got := matchesCategoryFilter(tc.Input.ThemeDistribution, tc.Input.SubcategoryDistribution, tc.Input.Themes, tc.Input.Subcategories)
			if got != tc.Result {
				t.Fatalf("case %q: want %v, got %v", name, tc.Result, got)
			}
		})
	}
}

func TestSplitCategoryFiltersMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.SplitCategoryFilters {
		t.Run(name, func(t *testing.T) {
			gotThemes, gotSubcategories := splitCategoryFilters(tc.Input.WorkCategory)
			// Order-sensitive: Python's list(dict.fromkeys(...)) preserves
			// first-sighting order, and dedupeStrings does too -- both
			// planes should produce the identical sequence, not just the
			// same set.
			if (len(gotThemes) != 0 || len(tc.Themes) != 0) && !reflect.DeepEqual(gotThemes, tc.Themes) {
				t.Fatalf("case %q themes: want %v, got %v", name, tc.Themes, gotThemes)
			}
			if (len(gotSubcategories) != 0 || len(tc.Subcategories) != 0) && !reflect.DeepEqual(gotSubcategories, tc.Subcategories) {
				t.Fatalf("case %q subcategories: want %v, got %v", name, tc.Subcategories, gotSubcategories)
			}
		})
	}
}

func TestDeriveWorkItemIDMatchesPythonGolden(t *testing.T) {
	golden := loadAttributionGolden(t)
	for name, tc := range golden.DeriveWorkItemID {
		t.Run(name, func(t *testing.T) {
			workItemType := ""
			if tc.Input.WorkItemType != nil {
				workItemType = *tc.Input.WorkItemType
			}
			got := deriveWorkItemID(tc.Input.System, tc.Input.Instance, tc.Input.ExternalKey, workItemType)
			if got != tc.Result {
				t.Fatalf("case %q: want %q, got %q", name, tc.Result, got)
			}
		})
	}
}
