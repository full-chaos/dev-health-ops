package providersync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The three derived surfaces this file proves share one case shape, because in
// production they share one resolver cascade and one work-item list. Each pair
// selects its own destination's rows from the same case, so a case that
// exercises a team-resolution edge exercises it for all three at once rather
// than three fixtures drifting apart.
//
// Cases are COLUMN-ORIENTED on both sides: the Python pair transposes its
// record list into one key per production field, so row ORDER is compared (a
// reordering diverges every column at once) and the field set is reflected
// from the production dataclass rather than hand-listed.

const (
	githubDerivedOracleOrg   = "org-acme"
	githubDerivedOracleScope = "acme/api"
)

func githubDerivedOracleCases() []oracleCase {
	return []oracleCase{
		{
			// Baseline: one estimated, one unestimated, both still open at the
			// window end, so both land in the backlog and the ratio is a real
			// fraction rather than the null-on-empty arm.
			ID: "open_backlog_mixed_estimates",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#1", map[string]any{"story_points": 3}),
					githubDerivedOracleItem("acme/api#2", nil),
				},
				"Transitions": []any{},
			},
		},
		{
			// D16 PIN: the coverage bucket is created BEFORE the terminal-item
			// skip, so an item completed before the window end still
			// materialises an all-zero group with a NULL ratio. Removing that
			// ordering in Go would drop this row entirely.
			ID: "terminal_only_group",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#3", map[string]any{
						"story_points": 5, "completed_at": "2026-08-02T09:00:00Z",
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			// closed_at alone (no completed_at) must count as terminal via the
			// earliest-of-the-two rule, and an item created after the window
			// end must not appear at all.
			ID: "closed_only_and_future_creation",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-06T00:30:00Z", "AsOf": "2026-08-06T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#4", map[string]any{
						"closed_at": "2026-08-03T09:00:00Z", "story_points": 2,
					}),
					githubDerivedOracleItem("acme/api#5", map[string]any{
						"created_at": "2026-08-05T09:00:00Z",
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			// D16 PIN: with no attribution fact of any kind the resolver yields
			// a single "unassigned" candidate whose team_id and team_name are
			// NULL. The coverage rollup normalises the same resolution to
			// "unassigned"/"Unassigned"; the attribution row does not. Both
			// pairs read this one case, so the asymmetry is asserted, not
			// assumed.
			ID: "unassigned_candidate",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#6", map[string]any{
						"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
					}),
				},
				"Transitions": []any{},
			},
		},
		{
			// Two statuses inside one day, split by a mid-day transition. The
			// day boundary must clip the pre-window part of the first segment
			// rather than counting it.
			ID: "state_split_across_one_day",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#7", nil),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#7", "2026-08-04T06:00:00Z", "todo", "in_progress"),
				},
			},
		},
		{
			// A transition at or before the segment start rewrites the open
			// segment instead of closing one, so a pre-creation transition
			// moves the item's start backwards. Also covers the stable-sort
			// requirement: two transitions share an occurred_at, and Python's
			// sorted() keeps their input order.
			ID: "pre_creation_and_tied_transitions",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#8", nil),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#8", "2026-07-30T00:00:00Z", "todo", "in_progress"),
					githubDerivedOracleTransition("acme/api#8", "2026-08-04T08:00:00Z", "in_progress", "review"),
					githubDerivedOracleTransition("acme/api#8", "2026-08-04T08:00:00Z", "review", "done"),
				},
			},
		},
		{
			// An item with transitions but no overlap with the window, beside
			// an item with no transitions at all -- the latter contributes no
			// state-duration row AND never registers its team name, because
			// Python's `continue` precedes the team-name write.
			ID: "no_transitions_and_no_overlap",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#9", nil),
					githubDerivedOracleItem("acme/api#10", map[string]any{
						"completed_at": "2026-08-02T00:00:00Z",
					}),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#10", "2026-08-01T06:00:00Z", "todo", "done"),
				},
			},
		},
		{
			// A transition occurring EXACTLY at the segment start takes the
			// rewrite arm, not the close arm: `tr_at <= current_start`. Without
			// this case a Go `Before` reads identically to Python's `<=`,
			// because no other case puts a transition on the boundary.
			ID: "transition_exactly_at_creation",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#12", nil),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#12", "2026-08-01T00:00:00Z", "todo", "in_progress"),
				},
			},
		},
		{
			// TWO items land in the SAME (provider, scope, team, status) key,
			// so items_touched must be a DISTINCT count of work-item ids rather
			// than a count of contributing segments. Every other case has one
			// item per key, where the two are indistinguishable.
			ID: "two_items_share_one_state_key",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#13", nil),
					githubDerivedOracleItem("acme/api#14", nil),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#13", "2026-08-04T04:00:00Z", "todo", "in_progress"),
					githubDerivedOracleTransition("acme/api#14", "2026-08-04T10:00:00Z", "todo", "in_progress"),
					// One item re-enters a status it already held today, so the
					// same item contributes TWO segments to one key.
					githubDerivedOracleTransition("acme/api#13", "2026-08-04T14:00:00Z", "in_progress", "todo"),
					githubDerivedOracleTransition("acme/api#13", "2026-08-04T18:00:00Z", "todo", "in_progress"),
				},
			},
		},
		{
			// The ONLY case that puts two items in one group under DIFFERENT
			// team names: same work_scope_id (project_id), same resolved
			// team_id, but two repo-ownership facts naming that team
			// differently. Without it, first-writer-wins and last-writer-wins
			// are indistinguishable -- and Python uses OPPOSITE rules in the
			// two builders (coverage keeps the first, state durations keep the
			// last), so both rules would sit unasserted.
			ID: "one_group_two_team_names",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": map[string]any{
					"Teams": []any{}, "Projects": []any{}, "Members": []any{},
					"ManualFallbacks": []any{},
					"Repos": []any{
						githubDerivedOracleRepoFact(
							"11111111-1111-4111-8111-111111111111", "acme/api-a", "t1", "Team One",
						),
						githubDerivedOracleRepoFact(
							"22222222-2222-4222-8222-222222222222", "acme/api-b", "t1", "Team Uno",
						),
					},
				},
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#15", map[string]any{
						"repo_id": "11111111-1111-4111-8111-111111111111",
					}),
					githubDerivedOracleItem("acme/api#16", map[string]any{
						"repo_id": "22222222-2222-4222-8222-222222222222",
					}),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#15", "2026-08-04T04:00:00Z", "todo", "in_progress"),
					githubDerivedOracleTransition("acme/api#16", "2026-08-04T05:00:00Z", "todo", "in_progress"),
				},
			},
		},
		{
			// POPULATED-FACT class, and the only case that reaches the team
			// and membership fact tables at all. One item resolves through
			// assignee_membership (a Members fact matching its assignee), the
			// other through issue_project (a Teams fact matching its
			// project_key) -- two different sources, two different team ids,
			// both at "high" confidence, sharing one work_scope_id. The
			// repo-ownership case above covers a third source; project
			// ownership, manual fallback and linked-issue inheritance remain
			// the resolver pair's business, not these three destinations'.
			//
			// The Python pair asserts the production loader actually issued
			// all four fact queries, so a case that silently stopped feeding
			// facts fails rather than quietly narrowing to the empty class.
			ID: "populated_facts_membership_and_project",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": map[string]any{
					"Projects": []any{}, "Repos": []any{}, "ManualFallbacks": []any{},
					"Teams": []any{map[string]any{
						"TeamID": "platform", "TeamName": "Platform",
						"ProjectKeys": []any{"PLAT"},
					}},
					"Members": []any{map[string]any{
						"Provider": "github", "TeamID": "payments",
						"TeamName": "Payments", "MemberID": "m1",
						"RawProviderUserID": "octocat", "RawEmail": "octo@example.com",
						"IdentityFacets": []any{"octocat"},
						"IsPrimary":      1, "Specificity": 50, "Priority": 20,
						"UpdatedAt": "2026-07-01T00:00:00Z",
					}},
				},
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#20", map[string]any{
						"assignees": []any{"octocat"}, "story_points": 8,
					}),
					githubDerivedOracleItem("acme/api#21", map[string]any{
						"project_key": "PLAT",
					}),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#20", "2026-08-04T03:00:00Z", "todo", "in_progress"),
					githubDerivedOracleTransition("acme/api#21", "2026-08-04T09:00:00Z", "todo", "review"),
				},
			},
		},
		{
			// TIMEZONE: the window is UTC, and this case is chosen so the
			// local and UTC calendar dates DISAGREE. 2026-08-04T23:30:00Z is
			// still 2026-08-04 in UTC but already 2026-08-05 at +02:00, and
			// 2026-08-04T00:30:00Z is 2026-08-03 at -05:00. A builder that
			// bucketed on a local date would put these on the wrong day.
			ID: "utc_window_disagrees_with_local_date",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg, "Day": "2026-08-04",
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#11", nil),
				},
				"Transitions": []any{
					githubDerivedOracleTransition("acme/api#11", "2026-08-04T00:30:00Z", "todo", "in_progress"),
					githubDerivedOracleTransition("acme/api#11", "2026-08-04T23:30:00Z", "in_progress", "done"),
				},
			},
		},
	}
}

func TestGitHubEstimateCoverageMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/estimate-coverage",
		githubDerivedOracleCases(),
		func(t *testing.T, input map[string]any) githubEstimateCoverageColumns {
			t.Helper()
			surfaces := buildGitHubDerivedOracleSurfaces(t, input)
			return newGitHubEstimateCoverageColumns(surfaces.EstimateCoverage)
		},
		nil,
	)
}

func TestGitHubWorkItemTeamAttributionsMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/team-attributions",
		githubDerivedOracleCases(),
		func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
			t.Helper()
			surfaces := buildGitHubDerivedOracleSurfaces(t, input)
			return newGitHubTeamAttributionColumns(surfaces.TeamAttributions)
		},
		nil,
	)
}

func TestGitHubWorkItemStateDurationsMatchLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/state-durations",
		githubDerivedOracleCases(),
		func(t *testing.T, input map[string]any) githubStateDurationColumns {
			t.Helper()
			surfaces := buildGitHubDerivedOracleSurfaces(t, input)
			return newGitHubStateDurationColumns(surfaces.StateDurations)
		},
		nil,
	)
}

// The column structs below are the Go half of the column-oriented comparison.
// They are STRUCTS, not maps, on purpose: typedEncode walks a struct's fields
// exhaustively, so a field the production row declares and this projection
// forgets is a compile-time or comparison failure, never a silent narrowing.

type githubEstimateCoverageColumns struct {
	Day              []githubWorkItemDerivedDay `json:"day"`
	Provider         []string                   `json:"provider"`
	WorkScopeID      []string                   `json:"work_scope_id"`
	TeamID           []*string                  `json:"team_id"`
	TeamName         []*string                  `json:"team_name"`
	EstimatedCount   []int                      `json:"estimated_count"`
	UnestimatedCount []int                      `json:"unestimated_count"`
	BacklogSize      []int                      `json:"backlog_size"`
	Ratio            []*float64                 `json:"ratio"`
	ComputedAt       []time.Time                `json:"computed_at"`
	OrgID            []string                   `json:"org_id"`
}

func newGitHubEstimateCoverageColumns(
	rows []githubEstimateCoverageMetricsDailyRow,
) githubEstimateCoverageColumns {
	columns := githubEstimateCoverageColumns{
		Day: []githubWorkItemDerivedDay{}, Provider: []string{},
		WorkScopeID: []string{}, TeamID: []*string{}, TeamName: []*string{},
		EstimatedCount: []int{}, UnestimatedCount: []int{}, BacklogSize: []int{},
		Ratio: []*float64{}, ComputedAt: []time.Time{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.Day = append(columns.Day, row.Day)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.WorkScopeID = append(columns.WorkScopeID, row.WorkScopeID)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.EstimatedCount = append(columns.EstimatedCount, row.EstimatedCount)
		columns.UnestimatedCount = append(columns.UnestimatedCount, row.UnestimatedCount)
		columns.BacklogSize = append(columns.BacklogSize, row.BacklogSize)
		columns.Ratio = append(columns.Ratio, row.Ratio)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

type githubTeamAttributionColumns struct {
	WorkItemID []string     `json:"work_item_id"`
	Provider   []string     `json:"provider"`
	Source     []string     `json:"source"`
	IsPrimary  []int        `json:"is_primary"`
	Confidence []string     `json:"confidence"`
	Evidence   []string     `json:"evidence"`
	ComputedAt []time.Time  `json:"computed_at"`
	RepoID     []*uuid.UUID `json:"repo_id"`
	TeamID     []*string    `json:"team_id"`
	TeamName   []*string    `json:"team_name"`
	OrgID      []string     `json:"org_id"`
}

func newGitHubTeamAttributionColumns(
	rows []githubWorkItemTeamAttributionRow,
) githubTeamAttributionColumns {
	columns := githubTeamAttributionColumns{
		WorkItemID: []string{}, Provider: []string{}, Source: []string{},
		IsPrimary: []int{}, Confidence: []string{}, Evidence: []string{},
		ComputedAt: []time.Time{}, RepoID: []*uuid.UUID{}, TeamID: []*string{},
		TeamName: []*string{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.WorkItemID = append(columns.WorkItemID, row.WorkItemID)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.Source = append(columns.Source, row.Source)
		columns.IsPrimary = append(columns.IsPrimary, row.IsPrimary)
		columns.Confidence = append(columns.Confidence, row.Confidence)
		columns.Evidence = append(columns.Evidence, row.Evidence)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.RepoID = append(columns.RepoID, row.RepoID)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

type githubStateDurationColumns struct {
	Day           []githubWorkItemDerivedDay `json:"day"`
	Provider      []string                   `json:"provider"`
	WorkScopeID   []string                   `json:"work_scope_id"`
	TeamID        []string                   `json:"team_id"`
	TeamName      []string                   `json:"team_name"`
	Status        []string                   `json:"status"`
	DurationHours []float64                  `json:"duration_hours"`
	ItemsTouched  []int                      `json:"items_touched"`
	ComputedAt    []time.Time                `json:"computed_at"`
	AvgWIP        []float64                  `json:"avg_wip"`
	OrgID         []string                   `json:"org_id"`
}

func newGitHubStateDurationColumns(
	rows []githubWorkItemStateDurationDailyRow,
) githubStateDurationColumns {
	columns := githubStateDurationColumns{
		Day: []githubWorkItemDerivedDay{}, Provider: []string{},
		WorkScopeID: []string{}, TeamID: []string{}, TeamName: []string{},
		Status: []string{}, DurationHours: []float64{}, ItemsTouched: []int{},
		ComputedAt: []time.Time{}, AvgWIP: []float64{}, OrgID: []string{},
	}
	for _, row := range rows {
		columns.Day = append(columns.Day, row.Day)
		columns.Provider = append(columns.Provider, row.Provider)
		columns.WorkScopeID = append(columns.WorkScopeID, row.WorkScopeID)
		columns.TeamID = append(columns.TeamID, row.TeamID)
		columns.TeamName = append(columns.TeamName, row.TeamName)
		columns.Status = append(columns.Status, row.Status)
		columns.DurationHours = append(columns.DurationHours, row.DurationHours)
		columns.ItemsTouched = append(columns.ItemsTouched, row.ItemsTouched)
		columns.ComputedAt = append(columns.ComputedAt, row.ComputedAt)
		columns.AvgWIP = append(columns.AvgWIP, row.AvgWIP)
		columns.OrgID = append(columns.OrgID, row.OrgID)
	}
	return columns
}

func buildGitHubDerivedOracleSurfaces(
	t *testing.T, input map[string]any,
) githubWorkItemDerivedSurfaces {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["OrgID"].(string)
	day, err := time.Parse("2006-01-02", input["Day"].(string))
	if err != nil {
		t.Fatal(err)
	}
	computedAt, err := time.Parse(time.RFC3339Nano, input["ComputedAt"].(string))
	if err != nil {
		t.Fatal(err)
	}
	rows := githubWorkItemRows{}
	for _, raw := range input["WorkItems"].([]any) {
		rows.WorkItems = append(rows.WorkItems, githubDerivedOracleGoItem(t, raw.(map[string]any)))
	}
	for _, raw := range input["Transitions"].([]any) {
		rows.StatusTransitions = append(
			rows.StatusTransitions, githubDerivedOracleGoTransition(t, raw.(map[string]any)),
		)
	}
	// Facts decode from the SAME case JSON the Python pair reads, so neither
	// side can be handed a fact set the other never saw. An empty fact set
	// still goes through the real context constructor: the resolver's
	// unassigned fallback is production behaviour, not a shortcut this test
	// may take on Python's behalf.
	encodedFacts, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	var facts githubWorkItemDerivationFacts
	if err := json.Unmarshal(encodedFacts, &facts); err != nil {
		t.Fatal(err)
	}
	derived := newGitHubWorkItemDerivationContext(facts)
	surfaces, err := buildGitHubWorkItemDerivedSurfaces(claim, rows, day, computedAt, derived)
	if err != nil {
		t.Fatal(err)
	}
	return surfaces
}

func githubDerivedOracleEmptyFacts() map[string]any {
	return map[string]any{
		"Teams": []any{}, "Projects": []any{}, "Repos": []any{},
		"Members": []any{}, "ManualFallbacks": []any{},
	}
}

func githubDerivedOracleItem(id string, overrides map[string]any) map[string]any {
	item := map[string]any{
		"work_item_id": id, "provider": "github", "title": id,
		"project_id": githubDerivedOracleScope,
		"created_at": "2026-08-01T00:00:00Z", "updated_at": "2026-08-04T00:00:00Z",
		"org_id": githubDerivedOracleOrg,
	}
	for key, value := range overrides {
		item[key] = value
	}
	return item
}

func githubDerivedOracleTransition(id, occurredAt, from, to string) map[string]any {
	return map[string]any{
		"work_item_id": id, "provider": "github", "occurred_at": occurredAt,
		"from_status": from, "to_status": to, "org_id": githubDerivedOracleOrg,
	}
}

func githubDerivedOracleGoItem(t *testing.T, raw map[string]any) githubWorkItemRow {
	t.Helper()
	row := githubWorkItemRow{
		WorkItemID: raw["work_item_id"].(string),
		Provider:   raw["provider"].(string),
		Title:      raw["title"].(string),
		Type:       "issue",
		Status:     githubDerivedOracleString(raw, "status", "todo"),
		ProjectID:  stringPointer(raw["project_id"].(string)),
		CreatedAt:  githubDerivedOracleTime(t, raw["created_at"].(string)),
		UpdatedAt:  githubDerivedOracleTime(t, raw["updated_at"].(string)),
		OrgID:      raw["org_id"].(string),
	}
	if value, ok := raw["completed_at"].(string); ok {
		parsed := githubDerivedOracleTime(t, value)
		row.CompletedAt = &parsed
	}
	if value, ok := raw["closed_at"].(string); ok {
		parsed := githubDerivedOracleTime(t, value)
		row.ClosedAt = &parsed
	}
	if value, ok := raw["story_points"].(int); ok {
		points := float64(value)
		row.StoryPoints = &points
	}
	if value, ok := raw["project_key"].(string); ok {
		row.ProjectKey = stringPointer(value)
	}
	if values, ok := raw["assignees"].([]any); ok {
		for _, value := range values {
			row.Assignees = append(row.Assignees, value.(string))
		}
	}
	if value, ok := raw["repo_id"].(string); ok {
		parsed, err := uuid.Parse(value)
		if err != nil {
			t.Fatal(err)
		}
		row.RepoID = &parsed
	}
	return row
}

func githubDerivedOracleGoTransition(
	t *testing.T, raw map[string]any,
) githubWorkItemTransitionRow {
	t.Helper()
	return githubWorkItemTransitionRow{
		WorkItemID: raw["work_item_id"].(string),
		Provider:   raw["provider"].(string),
		OccurredAt: githubDerivedOracleTime(t, raw["occurred_at"].(string)),
		FromStatus: raw["from_status"].(string),
		ToStatus:   raw["to_status"].(string),
		OrgID:      raw["org_id"].(string),
	}
}

func githubDerivedOracleString(raw map[string]any, key, fallback string) string {
	if value, ok := raw[key].(string); ok && value != "" {
		return value
	}
	return fallback
}

func githubDerivedOracleTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.UTC()
}

// The two facts MUST carry different repo_full_name values. With the same
// full name both facts match BOTH items (the resolver indexes by name as well
// as by id), every item then sees both candidates, and the primary resolution
// picks the same one for each -- which is exactly why the first version of
// this case left the first-writer/last-writer mutants alive.
func githubDerivedOracleRepoFact(repoID, repoFullName, teamID, teamName string) map[string]any {
	return map[string]any{
		"Provider": "github", "TeamID": teamID, "TeamName": teamName,
		"RepoID": repoID, "RepoFullName": repoFullName,
		"IsPrimary": 1, "Specificity": 100, "Priority": 10,
		"UpdatedAt": "2026-07-01T00:00:00Z",
	}
}
