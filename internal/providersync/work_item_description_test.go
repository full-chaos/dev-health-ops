package providersync

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

func TestBoundedWorkItemDescriptionCutsOnARuneBoundary(t *testing.T) {
	multibyte := strings.Repeat("é", workItemDescriptionMaxRunes+5)
	exact := strings.Repeat("a", workItemDescriptionMaxRunes)
	empty := ""
	cases := []struct {
		name  string
		in    *string
		want  *string
		runes int
	}{
		{"nil", nil, nil, 0},
		{"empty is no description", &empty, nil, 0},
		{"exactly the bound is kept whole", &exact, &exact, workItemDescriptionMaxRunes},
		{"multibyte is cut at the bound in runes", &multibyte, nil, workItemDescriptionMaxRunes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := boundedWorkItemDescription(tc.in)
			if tc.want == nil && tc.runes == 0 {
				if got != nil {
					t.Fatalf("got %q, want nil", *got)
				}
				return
			}
			if got == nil || !utf8.ValidString(*got) || utf8.RuneCountInString(*got) != tc.runes {
				t.Fatalf("got %v, want %d valid runes", got, tc.runes)
			}
			if tc.want != nil && *got != *tc.want {
				t.Fatal("a description within the bound changed")
			}
		})
	}
}

func TestEveryWorkItemProjectionBoundsTheDescription(t *testing.T) {
	long := strings.Repeat("x", workItemDescriptionMaxRunes+300)
	github := projectWorkItem(githubWorkItemRow{WorkItemID: "gh:acme/api#1", Provider: "github", Description: &long})
	linear := projectLinearWorkItem(linearWorkItemRow{WorkItemID: "lin:1", Provider: "linear", Description: &long})
	for name, row := range map[string]workItemStoredRow{"github/gitlab/jira": github, "linear": linear} {
		if row.Description == nil || utf8.RuneCountInString(*row.Description) != workItemDescriptionMaxRunes {
			t.Errorf("%s: stored description not cut to %d runes: %v", name, workItemDescriptionMaxRunes, row.Description)
		}
	}
}

func heldWorkItemVersion(description string) func([]any) (map[string]any, bool) {
	return func([]any) (map[string]any, bool) {
		return map[string]any{"description": description, "priority_raw": "p0", "service_class": "expedite", "due_at": nil}, true
	}
}

// A sync row that states a description overwrites the held one; a row with
// none keeps the one a push wrote. Driven through the production row builder.
func TestWorkItemSyncKeepsAHeldDescriptionOnlyWhenTheRowStatesNone(t *testing.T) {
	stated := "synced body"
	for _, tc := range []struct {
		name string
		in   *string
		want any
	}{
		{"stated overwrites", &stated, "synced body"},
		{"nil keeps the push-written value", nil, "pushed body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := workItemStoredRow{WorkItemID: "gh:acme/api#1", Provider: "github", Description: tc.in}
			rows := workItemStoredVersionRows([]workItemStoredRow{row})
			if _, err := workItemsContract.Fold(withHeldWorkItemColumns(gitHubWorkItemsInsert), rows, heldWorkItemVersion("pushed body")); err != nil {
				t.Fatal(err)
			}
			positions, err := storedversion.Positions(withHeldWorkItemColumns(gitHubWorkItemsInsert))
			if err != nil {
				t.Fatal(err)
			}
			got := rows[0].Values[positions["description"]]
			if p, ok := got.(*string); ok && p != nil {
				got = *p
			}
			if got != tc.want {
				t.Fatalf("description = %v, want %v", got, tc.want)
			}
			if rows[0].Values[positions["priority_raw"]] != "p0" {
				t.Fatalf("priority_raw not carried: %v", rows[0].Values[positions["priority_raw"]])
			}
		})
	}
}
