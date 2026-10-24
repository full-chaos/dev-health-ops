package providersync

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

type emptyGitHubWorkItemDerivationRows struct{}

func (emptyGitHubWorkItemDerivationRows) Next() bool                       { return false }
func (emptyGitHubWorkItemDerivationRows) Scan(...any) error                { return nil }
func (emptyGitHubWorkItemDerivationRows) ScanStruct(any) error             { return nil }
func (emptyGitHubWorkItemDerivationRows) ColumnTypes() []driver.ColumnType { return nil }
func (emptyGitHubWorkItemDerivationRows) Totals(...any) error              { return nil }
func (emptyGitHubWorkItemDerivationRows) Columns() []string                { return nil }
func (emptyGitHubWorkItemDerivationRows) Close() error                     { return nil }
func (emptyGitHubWorkItemDerivationRows) Err() error                       { return nil }
func (emptyGitHubWorkItemDerivationRows) HasData() bool                    { return false }

func TestGitHubWorkItemDerivationTrimsManualScopeAndPreservesZeroPriority(t *testing.T) {
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		ManualFallbacks: []teamattribution.GithubWorkItemDerivationManualFallback{{
			Provider: "github", ScopeType: "repo", ScopeID: "  " + repoID + "  ",
			TeamID: "team-manual", TeamName: "Manual Team", Reason: "explicit", Priority: 0,
		}},
	})
	teamID, _, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#manual", Provider: "github", RepoID: &repoID, OrgID: "org-acme",
	})
	if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-manual" || len(candidates) != 1 {
		t.Fatalf("manual attribution = team %v candidates %+v", teamID, candidates)
	}
	candidate := candidates[0]
	if candidate.Source != "manual_fallback" || candidate.Priority != 0 ||
		candidate.Evidence != "manual_fallback:repo=  "+repoID+"   (explicit)" {
		t.Fatalf("manual provenance = %+v", candidate)
	}
}

func TestGitHubWorkItemDerivationManualFallbackTieBreakIsStable(t *testing.T) {
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	projectID := "acme/api"
	subject := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#manual-tie", Provider: "github",
		RepoID: &repoID, ProjectID: &projectID, OrgID: "org-acme",
	}
	rules := []teamattribution.GithubWorkItemDerivationManualFallback{
		{
			Provider: "github", ScopeType: "repo", ScopeID: repoID,
			TeamID: "team-manual", TeamName: "Manual Team", Reason: "repo rule", Priority: 0,
		},
		{
			Provider: "github", ScopeType: "project", ScopeID: projectID,
			TeamID: "team-manual", TeamName: "Manual Team", Reason: "project rule", Priority: 0,
		},
	}
	resolve := func(input []teamattribution.GithubWorkItemDerivationManualFallback) []teamattribution.GithubWorkItemDerivationCandidate {
		t.Helper()
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			ManualFallbacks: input,
		})
		_, _, candidates := derived.Resolve(subject)
		return candidates
	}

	forward := resolve(rules)
	reversed := resolve([]teamattribution.GithubWorkItemDerivationManualFallback{rules[1], rules[0]})
	if !reflect.DeepEqual(forward, reversed) {
		t.Fatalf("manual fallback order changed attribution:\nforward=%+v\nreversed=%+v", forward, reversed)
	}
	if len(forward) != 2 || forward[0].IsPrimary != 1 ||
		forward[0].Evidence != "manual_fallback:project=acme/api (project rule)" ||
		forward[1].IsPrimary != 0 {
		t.Fatalf("manual fallback deterministic ranking=%+v", forward)
	}
}

func TestGitHubWorkItemDerivationRequiresAnUnambiguousActualEdge(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	projectLinear := "linear-project"
	projectJira := "jira-project"
	source := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#9", Provider: "github", OrgID: "org-acme",
	}
	linearDonor := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "linear:CHAOS-9", Provider: "linear", ProjectID: &projectLinear, OrgID: "org-acme",
	}
	jiraDonor := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "jira:CHAOS-9", Provider: "jira", ProjectID: &projectJira, OrgID: "org-acme",
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{Projects: []teamattribution.GithubWorkItemDerivationProjectFact{
		{Provider: "linear", ProjectID: projectLinear, TeamID: "team-linear", TeamName: "Linear Team", IsPrimary: 1, UpdatedAt: now},
		{Provider: "jira", ProjectID: projectJira, TeamID: "team-jira", TeamName: "Jira Team", IsPrimary: 1, UpdatedAt: now},
	}})
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{
		source.WorkItemID: source, linearDonor.WorkItemID: linearDonor, jiraDonor.WorkItemID: jiraDonor,
	}

	t.Run("ambiguous provider-neutral key", func(t *testing.T) {
		candidateContext := derived
		candidateContext.LinkedIssue, _, _ = candidateContext.BuildLinkedIssueIndex(
			"github", subjects, []teamattribution.GithubWorkItemDerivationDependencyEdge{{
				SourceWorkItemID: source.WorkItemID, TargetWorkItemID: "extkey:CHAOS-9",
				RelationshipType: "external_issue_key", LastSynced: now, OrgID: "org-acme",
			}}, nil,
		)
		teamID, _, candidates := candidateContext.Resolve(source)
		if teamID != nil || len(candidates) != 1 || candidates[0].Source != "unassigned" {
			t.Fatalf("ambiguous key resolved: team=%v candidates=%+v", teamID, candidates)
		}
	})

	t.Run("key-shaped donor without edge", func(t *testing.T) {
		candidateContext := derived
		candidateContext.LinkedIssue, _, _ = candidateContext.BuildLinkedIssueIndex(
			"github", subjects, []teamattribution.GithubWorkItemDerivationDependencyEdge{{
				SourceWorkItemID: "gh:acme/api#other", TargetWorkItemID: linearDonor.WorkItemID,
				RelationshipType: "relates_to", LastSynced: now, OrgID: "org-acme",
			}}, nil,
		)
		teamID, _, candidates := candidateContext.Resolve(source)
		if teamID != nil || len(candidates) != 1 || candidates[0].Source != "unassigned" {
			t.Fatalf("unlinked donor resolved: team=%v candidates=%+v", teamID, candidates)
		}
	})
}

func TestGitHubWorkItemDerivationRanksProjectRepoAndMemberFacts(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	projectID := "acme/api"
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Projects: []teamattribution.GithubWorkItemDerivationProjectFact{{
			Provider: "github", ProjectID: projectID, TeamID: "team-project", TeamName: "Project Team",
			IsPrimary: 1, Specificity: 80, UpdatedAt: now,
		}},
		Repos: []teamattribution.GithubWorkItemDerivationRepoFact{{
			Provider: "github", RepoID: &repoID, RepoFullName: projectID,
			TeamID: "team-repo", TeamName: "Repo Team", IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", MemberID: "dev@example.com", TeamID: "team-member", TeamName: "Member Team",
			IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	teamID, _, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#10", Provider: "github", RepoID: &repoID,
		ProjectID: &projectID, Assignees: []string{"DEV@EXAMPLE.COM"}, OrgID: "org-acme",
	})
	if got := teamattribution.GithubWorkItemDerivationStringValue(teamID); got != "team-project" {
		t.Fatalf("primary team = %q, want team-project", got)
	}
	bySource := map[string]teamattribution.GithubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if candidate := bySource["project_ownership"]; candidate.IsPrimary != 1 || candidate.Confidence != "high" || candidate.Evidence != "project_ownership=acme/api" {
		t.Fatalf("project provenance = %+v", candidate)
	}
	for _, source := range []string{"repo_ownership", "assignee_membership"} {
		if candidate := bySource[source]; candidate.IsPrimary != 0 {
			t.Fatalf("%s unexpectedly primary: %+v", source, candidate)
		}
	}
}

// fakeMembersSplitConn feeds loadMembers one identities row and one teams
// row. It refuses a teams read that selects the stored roster column
// `members` (CHAOS-9087): a query that names it fails the load.
type fakeMembersSplitConn struct {
	driver.Conn
}

var rosterColumnRead = regexp.MustCompile(`(^|[^_a-z])members\b`)

func (fakeMembersSplitConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM identities FINAL"):
		return &fakeMembersSplitRows{
			rows: [][]any{
				{"alice-id", (*string)(nil), `{"github":["lead"]}`, []string{}, time.Time{}},
			},
		}, nil
	case strings.Contains(query, "FROM teams FINAL"):
		if rosterColumnRead.MatchString(query) {
			return nil, fmt.Errorf("fakeMembersSplitConn: the teams query reads the roster column: %s", query)
		}
		return &fakeMembersSplitRows{
			rows: [][]any{
				{"team-eng", "Engineering", []string{"lead", "alice@example.com"}},
			},
		}, nil
	default:
		return emptyGitHubWorkItemDerivationRows{}, nil
	}
}

type fakeMembersSplitRows struct {
	rows [][]any
	idx  int
}

func (r *fakeMembersSplitRows) Next() bool { return r.idx < len(r.rows) }
func (r *fakeMembersSplitRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	for i, d := range dest {
		switch target := d.(type) {
		case *string:
			*target, _ = row[i].(string)
		case **string:
			*target, _ = row[i].(*string)
		case *[]string:
			*target, _ = row[i].([]string)
		case *time.Time:
			*target, _ = row[i].(time.Time)
		default:
			return fmt.Errorf("fakeMembersSplitRows: unsupported scan dest %T", d)
		}
	}
	return nil
}
func (r *fakeMembersSplitRows) ScanStruct(any) error             { return nil }
func (r *fakeMembersSplitRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *fakeMembersSplitRows) Totals(...any) error              { return nil }
func (r *fakeMembersSplitRows) Columns() []string                { return nil }
func (r *fakeMembersSplitRows) Close() error                     { return nil }
func (r *fakeMembersSplitRows) Err() error                       { return nil }
func (r *fakeMembersSplitRows) HasData() bool                    { return len(r.rows) > 0 }

// TestGitHubWorkItemLoadMembersReadsManualMembersOnly pins what loadMembers
// takes from `teams`: the admin-exclusive manual_members roster, as untyped
// facts. There is no roster fallback tier any more (the stored roster column
// `members` is gone, CHAOS-9087): the fake conn fails a teams read of that
// column, and the load returns the two manual facets and nothing else.
func TestGitHubWorkItemLoadMembersReadsManualMembersOnly(t *testing.T) {
	source := teamattribution.ClickHouseFactSource{Conn: fakeMembersSplitConn{}}
	typed, untyped, err := source.LoadMembers(context.Background(), "org-acme", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(untyped) != 2 || untyped[0].Facet != "lead" || untyped[1].Facet != "alice@example.com" ||
		untyped[0].TeamID != "team-eng" || untyped[1].TeamID != "team-eng" {
		t.Fatalf("untyped = %+v, want the two manual_members facets of team-eng and nothing from a roster", untyped)
	}
	_ = typed
}

// fakeDonorRow holds one donor row's column values, in the EXACT order
// loadDonors's SELECT list produces them.
type fakeDonorRow struct {
	values []any
}

// fakeDonorRows is a driver.Rows that actually returns data through Scan --
// codex round-6 finding (2026-08-25, BLOCK): every other Rows double in this
// package (emptyGitHubWorkItemDerivationRows included) has Next() return
// false immediately, so loadDonors's Scan() destination list -- which must
// stay in the same order as its SELECT column list -- has never actually
// been exercised. A column reordering that shifted "type" into the wrong
// destination would compile and pass every existing test while silently
// corrupting production donor Type propagation.
type fakeDonorRows struct {
	rows []fakeDonorRow
	idx  int
}

// CHAOS-4321 (chris's ruling, 2026-08-26): only the CHAOS-4244 author_membership
// path (a PR/MR's reporter walked through team_memberships) is removed here.
// assignee_membership -- the pre-4244, rank-4 mechanism -- is UNCHANGED and
// stays legitimate under the manual-override basis chris confirmed (see
// docs/contribute/architecture/team-attribution.md §0); its own coverage
// (TestGitHubWorkItemDerivationPreservesPrecedenceAndProvenance,
// TestGitHubWorkItemDerivationRanksProjectRepoAndMemberFacts, both above,
// untouched) is not repeated here. These replace the CHAOS-4244 author-path
// test suite that asserted the now-removed behavior with red-first coverage
// of the ruling's author-only scenarios: every case below failed against the
// pre-CHAOS-4321 resolver, which stamped a primary author_membership
// candidate from team_memberships alone.
// TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped
// covers CHAOS-4321 (chris's ruling, 2026-08-26 07:09 PT): membership-based
// attribution (assignee AND author alike) is legitimate ONLY when it
// resolves to EXACTLY one admin-authored team -- `derived.memberByID` is
// itself sourced only from `identities`/`teams` now (see loadMembers), never
// provider auto-import, so every candidate reaching resolve() is already
// admin-mapped; "no membership" here means no ADMIN membership, and
// "ambiguous" means mapped to 2+ teams, never "auto-imported vs curated".
func TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	for _, tt := range []struct {
		name       string
		facts      teamattribution.GithubWorkItemDerivationFacts
		subject    func() teamattribution.GithubWorkItemDerivationSubject
		wantSource string
		wantTeam   string
		wantEvid   string
	}{
		{
			name:  "author with no admin mapping is unassigned",
			facts: teamattribution.GithubWorkItemDerivationFacts{},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				reporter := "alice"
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#1", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:no_membership",
		},
		{
			name: "author admin-mapped to one team is attributed",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				reporter := "alice"
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#2", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "author_membership", wantTeam: "team-ops",
		},
		{
			name: "author admin-mapped to two teams is unassigned, no arbitrary pick",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{
					{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
					{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
				},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				reporter := "alice"
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#3", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:ambiguous_admin_membership:team-ops,team-platform",
		},
		{
			name: "bot author never attributed even when admin-mapped",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "dependabot[bot]", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				reporter := "github:dependabot[bot]"
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "ghpr:acme/api#5", Provider: "github", Type: "pr",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:bot_author",
		},
		{
			name: "plain GitHub issue, admin-mapped reporter -- author path stays PR/MR-only",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				reporter := "alice"
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#9", Provider: "github", Type: "issue",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate",
		},
		{
			name: "GitLab MR, admin-mapped reporter -- author path is provider-neutral",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
					Provider: "gitlab", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				reporter := "alice"
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "gitlab:acme/api!9", Provider: "gitlab", Type: "merge_request",
					Reporter: &reporter, OrgID: "org-acme",
				}
			},
			wantSource: "author_membership", wantTeam: "team-ops",
		},
		{
			name:  "assignee with no admin mapping is unassigned",
			facts: teamattribution.GithubWorkItemDerivationFacts{},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#20", Provider: "github", Type: "issue",
					Assignees: []string{"alice"}, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:no_membership",
		},
		{
			name: "assignee admin-mapped to one team is attributed",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
					Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
					MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
				}},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#21", Provider: "github", Type: "issue",
					Assignees: []string{"alice"}, OrgID: "org-acme",
				}
			},
			wantSource: "assignee_membership", wantTeam: "team-ops",
		},
		{
			name: "assignee admin-mapped to two teams is unassigned, no arbitrary pick",
			facts: teamattribution.GithubWorkItemDerivationFacts{
				Members: []teamattribution.GithubWorkItemDerivationMemberFact{
					{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
					{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
				},
			},
			subject: func() teamattribution.GithubWorkItemDerivationSubject {
				return teamattribution.GithubWorkItemDerivationSubject{
					WorkItemID: "gh:acme/api#22", Provider: "github", Type: "issue",
					Assignees: []string{"alice"}, OrgID: "org-acme",
				}
			},
			wantSource: "unassigned", wantEvid: "no_candidate:ambiguous_admin_membership:team-ops,team-platform",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			derived := teamattribution.NewGitHubWorkItemDerivationContext(tt.facts)
			subject := tt.subject()
			teamID, teamName, candidates := derived.Resolve(subject)
			if tt.wantSource == "assignee_membership" || tt.wantSource == "author_membership" {
				// CHAOS-9414: membership is provenance only. The member's
				// team is a non-primary row of wantSource; the primary row
				// is unassigned and carries no team.
				if teamID != nil || teamName != nil {
					t.Fatalf("primary team = (%v, %v), want none (membership is provenance only)",
						teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
				}
				found := false
				for _, candidate := range candidates {
					if candidate.Source == tt.wantSource &&
						teamattribution.GithubWorkItemDerivationStringValue(candidate.TeamID) == tt.wantTeam &&
						candidate.IsPrimary == teamattribution.AttributionNotPrimary {
						found = true
					}
					if candidate.Source == "unassigned" && candidate.IsPrimary != teamattribution.AttributionPrimary {
						t.Fatalf("unassigned is_primary = %d, want 1", candidate.IsPrimary)
					}
				}
				if !found {
					t.Fatalf("candidates = %+v, want a non-primary %s row for %s", candidates, tt.wantSource, tt.wantTeam)
				}
				return
			}
			if tt.wantTeam == "" {
				if teamID != nil || teamName != nil {
					t.Fatalf("team = (%v, %v), want (nil, nil)",
						teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
				}
			} else if teamattribution.GithubWorkItemDerivationStringValue(teamID) != tt.wantTeam {
				t.Fatalf("team = %v, want %v", teamattribution.GithubWorkItemDerivationStringValue(teamID), tt.wantTeam)
			}
			var primary *teamattribution.GithubWorkItemDerivationCandidate
			for index := range candidates {
				if candidates[index].IsPrimary == 1 {
					primary = &candidates[index]
					break
				}
			}
			if primary == nil || primary.Source != tt.wantSource {
				t.Fatalf("candidates = %+v, want primary source %q", candidates, tt.wantSource)
			}
			if tt.wantEvid != "" && primary.Evidence != tt.wantEvid {
				t.Fatalf("evidence = %q, want %q", primary.Evidence, tt.wantEvid)
			}
		})
	}
}

func TestGitHubWorkItemDerivationOwnershipWinsOverAssigneeAndAuthorMembership(t *testing.T) {
	// CHAOS-4321: ownership always wins as primary, even when BOTH assignee
	// and author are admin-mapped to a DIFFERENT team.
	//
	// CHAOS-4320 (this test UPDATED, not merely re-asserted -- Trap #116):
	// before this ticket, "team-other" not owning repoID had no consequence
	// beyond losing the primary slot -- assignee_membership/author_membership
	// still appeared as non-primary provenance rows, laundering "any team
	// this person belongs to" into a persisted attribution the repo-
	// ownership tier itself would never grant. The OLD assertion here
	// (`!sources["assignee_membership"] || !sources["author_membership"]`
	// failing the test) was pinning that defective behavior. The repo-
	// ownership gate now drops both candidates entirely instead: they fall
	// through the cascade exactly like a non-member would, so neither
	// source appears at all, and the run resolves to "unassigned" evidence
	// naming MembershipOwnershipReasonNotOwned had repo_ownership itself not
	// already supplied a primary.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Repos: []teamattribution.GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{
			{Provider: "github", TeamID: "team-other", TeamName: "Other Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
			{Provider: "github", TeamID: "team-other", TeamName: "Other Team", MemberID: "bob", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
		},
	})
	reporter := "alice"
	teamID, teamName, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#6", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, Assignees: []string{"bob"}, OrgID: "org-acme",
	})
	if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-repo" || teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Repository Team" {
		t.Fatalf("team = (%v, %v), want team-repo/Repository Team",
			teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
	}
	sources := map[string]bool{}
	var primarySource string
	for _, candidate := range candidates {
		sources[candidate.Source] = true
		if candidate.IsPrimary == 1 {
			primarySource = candidate.Source
		}
	}
	if primarySource != "repo_ownership" {
		t.Fatalf("primary source = %q, want repo_ownership", primarySource)
	}
	if sources["assignee_membership"] || sources["author_membership"] {
		t.Fatalf("candidates = %+v, want assignee_membership AND author_membership ABSENT (team-other does not own repoID, CHAOS-4320 gate)", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorOnlyDonorNeverPropagatesATeam(t *testing.T) {
	// CHAOS-4321: author_membership resolves the donor's OWN attribution
	// (when unambiguous, restored by the 07:09 PT ruling) but stays OUT of
	// allowedDonorSources (see buildLinkedIssueIndex) -- a person-shaped
	// signal must never be laundered into a rank-5 linked_issue donor fact
	// for a DIFFERENT item, ranked or not.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	reporter := "alice"
	donor := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#100", Provider: "github", Type: "pr",
		Reporter: &reporter, OrgID: "org-acme",
	}
	dependent := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#101", Provider: "github", Type: "pr", OrgID: "org-acme",
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-donor-only", TeamName: "Donor Only Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	donorTeamID, _, donorCandidates := resolveViaMembership(t, derived, donor)
	if teamattribution.GithubWorkItemDerivationStringValue(donorTeamID) != "team-donor-only" {
		t.Fatalf("donor team id = %v, want team-donor-only (its OWN attribution resolves via author_membership)",
			teamattribution.GithubWorkItemDerivationStringValue(donorTeamID))
	}
	for _, candidate := range donorCandidates {
		if candidate.IsPrimary == 1 && candidate.Source != "author_membership" {
			t.Fatalf("donor primary source = %q, want author_membership", candidate.Source)
		}
	}
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{
		donor.WorkItemID: donor, dependent.WorkItemID: dependent,
	}
	linkedIssue, _, _ := derived.BuildLinkedIssueIndex(
		"github", subjects, []teamattribution.GithubWorkItemDerivationDependencyEdge{{
			SourceWorkItemID: dependent.WorkItemID, TargetWorkItemID: donor.WorkItemID,
			RelationshipType: "relates_to", LastSynced: now, OrgID: "org-acme",
		}}, nil,
	)
	if _, ok := linkedIssue[dependent.WorkItemID]; ok {
		t.Fatalf("dependent inherited a team from an author-only donor: %+v", linkedIssue[dependent.WorkItemID])
	}
}

// TestGitHubWorkItemDerivationTwoLayerMembershipResolution covers CHAOS-4321
// (chris, 08:30 PT: "manual is override -- if the override exists, use it,
// else use attribution from providers"): (e) a bare teams.manual_members facet with
// no backing identities row still resolves via memberByUntypedFacet, (f) no
// admin mapping at all falls through to the provider auto-import layer, (g)
// an admin mapping wins outright over a conflicting provider membership for
// the SAME identity, and (h) an AMBIGUOUS admin mapping does not fall
// through to a clean provider answer.
func TestGitHubWorkItemDerivationTwoLayerMembershipResolution(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	t.Run("(e) a teams.manual_members-only mapping is attributed, no identities row", func(t *testing.T) {
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			UntypedMembers: []teamattribution.GithubWorkItemDerivationUntypedMemberFact{{
				TeamID: "team-ops", TeamName: "Ops Team", Facet: "alice@example.com", UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#30", Provider: "github", Type: "issue",
			Assignees: []string{"alice@example.com"}, OrgID: "org-acme",
		})
		if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-ops" || teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Ops Team" {
			t.Fatalf("team = (%v, %v), want team-ops/Ops Team", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row", candidates)
		}
	})

	t.Run("(f) provider-only single team is attributed via fallback layer", func(t *testing.T) {
		reporter := "alice"
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			ProviderMembers: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "ghpr:acme/api#31", Provider: "github", Type: "pr",
			Reporter: &reporter, OrgID: "org-acme",
		})
		if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-ops" || teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Ops Team" {
			t.Fatalf("team = (%v, %v), want team-ops/Ops Team", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "author_membership" || candidates[0].Evidence != "reporter=alice" {
			t.Fatalf("candidates = %+v, want exactly one author_membership row", candidates)
		}
	})

	t.Run("(g) admin mapping overrides a conflicting provider membership", func(t *testing.T) {
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
			}},
			ProviderMembers: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-other", TeamName: "Other Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#32", Provider: "github", Type: "issue",
			Assignees: []string{"alice"}, OrgID: "org-acme",
		})
		if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-ops" || teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Ops Team" {
			t.Fatalf("team = (%v, %v), want team-ops/Ops Team", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row (provider layer must not even appear)", candidates)
		}
	})

	t.Run("(h) ambiguous admin mapping does not fall through to provider", func(t *testing.T) {
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			Members: []teamattribution.GithubWorkItemDerivationMemberFact{
				{Provider: "github", TeamID: "team-ops", TeamName: "Ops Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
				{Provider: "github", TeamID: "team-platform", TeamName: "Platform Team", MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now},
			},
			ProviderMembers: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-clean", TeamName: "Clean Team",
				MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			}},
		})
		teamID, teamName, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#33", Provider: "github", Type: "issue",
			Assignees: []string{"alice"}, OrgID: "org-acme",
		})
		if teamID != nil || teamName != nil {
			t.Fatalf("team = (%v, %v), want (nil, nil)", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "unassigned" ||
			candidates[0].Evidence != "no_candidate:ambiguous_admin_membership:team-ops,team-platform" {
			t.Fatalf("candidates = %+v, want exactly one unassigned row naming the colliding admin teams", candidates)
		}
	})

	// (k)/(l)/(n) below pin that a provider-tagged fact of ProviderMembers
	// (a team_memberships row) does not cross providers: the provider-scoped
	// lookup key. (The roster column that once fed this pool is gone,
	// CHAOS-9087.)
	t.Run("(k) a provider-tagged roster login never attributes a DIFFERENT provider's item sharing the same string", func(t *testing.T) {
		// A GitHub team's membership holds bare login "lead", a
		// GitHub identity, so it lands in ProviderMembers keyed to
		// Provider: "github", not in the untyped pool.
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			ProviderMembers: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-eng", TeamName: "Engineering",
				MemberID: "lead", IsPrimary: 1, Specificity: 50, Priority: 10,
			}},
		})
		teamID, teamName, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "jira:PROJ-1", Provider: "jira", Type: "issue",
			Assignees: []string{"lead"}, OrgID: "org-acme",
		})
		if teamID != nil || teamName != nil {
			t.Fatalf("team = (%v, %v), want (nil, nil): a github-tagged roster login must not attribute a jira item sharing the same raw string", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "unassigned" || candidates[0].Evidence != "no_candidate:no_membership" {
			t.Fatalf("candidates = %+v, want exactly one unassigned/no_membership row", candidates)
		}
	})

	t.Run("(l) the SAME provider-tagged roster login still attributes ITS OWN provider's item (positive control for (k))", func(t *testing.T) {
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			ProviderMembers: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-eng", TeamName: "Engineering",
				MemberID: "lead", IsPrimary: 1, Specificity: 50, Priority: 10,
			}},
		})
		teamID, teamName, candidates := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#40", Provider: "github", Type: "issue",
			Assignees: []string{"lead"}, OrgID: "org-acme",
		})
		if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-eng" || teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Engineering" {
			t.Fatalf("team = (%v, %v), want team-eng/Engineering", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row", candidates)
		}
	})

	t.Run("(n) a provider-tagged roster facet with irregular internal whitespace still matches a work item assignee with DIFFERENT irregular whitespace", func(t *testing.T) {
		// Python/Go parity companion (team-lead ruling, 2026-08-26): Go's
		// resolve() already collapses internal whitespace correctly here --
		// normalizeDerivationIdentity (via strings.Fields+Join) runs on
		// every ProviderMembers identity at the SAME point real
		// team_memberships rows go through, so this pool's MemberID does
		// not need to be pre-normalized by loadMembers's split logic to
		// resolve correctly. This test is the Go half of the pair pinning
		// that fact -- the Python half
		// (test_loader_scopes_teams_members_fallback_facet_normalizes_internal_whitespace)
		// needed an actual code fix; this one is a regression pin, not a
		// fix, proving both languages now agree.
		derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
			ProviderMembers: []teamattribution.GithubWorkItemDerivationMemberFact{{
				Provider: "github", TeamID: "team-eng", TeamName: "Engineering",
				MemberID: "john   doe", IsPrimary: 1, Specificity: 50, Priority: 10,
			}},
		})
		teamID, teamName, candidates := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: "gh:acme/api#41", Provider: "github", Type: "issue",
			Assignees: []string{"john  doe"}, OrgID: "org-acme",
		})
		if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-eng" || teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Engineering" {
			t.Fatalf("team = (%v, %v), want team-eng/Engineering", teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
		}
		if len(candidates) != 1 || candidates[0].Source != "assignee_membership" {
			t.Fatalf("candidates = %+v, want exactly one assignee_membership row", candidates)
		}
	})
}

// The tests below restore CHAOS-4244 author-path coverage that lane-4321's
// Round-1 commit (cbe8f65fe, "remove person-membership as a team source")
// deleted under that round's original wider scope, and which was never
// brought back when the scope narrowed to "membership stays, but only
// through admin-authored data, else provider fallback" (chris, 08:30 PT).
// Confirmed via `git worktree add ... origin/main` that origin/main still
// carries all 19 of these as of f26cf55e0 -- this is a genuine coverage
// gap on this branch, not a stale golden-count pin. Each is adapted to the
// two-layer resolveMembership design: admin-authored member facts now live
// in Facts.Members at Specificity 60 (the CHAOS-4321 admin-layer
// convention), not the pre-4321 Specificity 50 the deleted originals used.

func TestGitHubWorkItemDerivationNoAssigneeNoReporterStaysUnassigned(t *testing.T) {
	// RED CONTROL: same member facts as a positive author_membership case,
	// but subject.Reporter is nil. Must stay unassigned -- proves the
	// positive cases elsewhere in this file are not passing for some other,
	// accidental reason (e.g. an assignee fallback silently covering for a
	// broken reporter path).
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	teamID, _, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#4244", Provider: "github", Type: "pr",
		OrgID: "org-acme",
	})
	if teamID != nil {
		t.Fatalf("team id = %v, want nil", teamID)
	}
	// No Reporter and no Assignees means neither membership path is even
	// attempted (resolveMembership is never called), so no membership skip
	// reason exists to report -- evidence stays the bare "no_candidate", not
	// "no_candidate:no_membership" (that suffix is only for a path that WAS
	// attempted and found zero candidates).
	if len(candidates) != 1 || candidates[0].Source != "unassigned" ||
		candidates[0].Evidence != "no_candidate" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationUnambiguousReporterMembershipStillResolves(t *testing.T) {
	// Positive control: multiple candidate ROWS for the same identity that
	// all name the SAME team_id (e.g. matched via member_id and an email
	// facet separately) must still resolve -- the two-layer admin gate
	// counts DISTINCT team_ids, not candidate rows.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{
			{
				Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
				MemberID: "alice", RawEmail: teamattribution.GithubWorkItemDerivationStringPointer("alice"),
				IsPrimary: 1, Specificity: 60, UpdatedAt: now,
			},
		},
	})
	reporter := "alice"
	teamID, teamName, _ := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#4244", Provider: "github", Type: "pr",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-ops" {
		t.Fatalf("team id = %v, want team-ops", teamID)
	}
	if teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Ops Team" {
		t.Fatalf("team name = %v, want Ops Team", teamName)
	}
}

func TestGitHubWorkItemDerivationAuthorNeverOutranksALinkedIssueDonor(t *testing.T) {
	// CHAOS-4244 precedence ruling (chris, 2026-08-24): a PR with an
	// admin-mapped author AND a linked_issue donor for a DIFFERENT team must
	// resolve to the linked issue's team -- author_membership (rank 6) sits
	// BELOW linked_issue (rank 5).
	//
	// CHAOS-5649 (R179 rule 1, chris 2026-09-12) supersedes this test's
	// ORIGINAL author-candidate assertion (present, non-primary, team-ops):
	// linked_issue already set primary to a DIFFERENT team (team-platform)
	// before the author_membership tier of `order` is reached, so rule 1
	// now drops the author_membership row for team-ops entirely -- it would
	// have been a second, non-primary team on this item, exactly prod's
	// measured 834-row double count.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	derived.LinkedIssue = map[string][3]string{
		"ghpr:acme/api#9": {"team-platform", "Platform Team"},
	}
	reporter := "alice"
	teamID, teamName, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#9", Provider: "github", Type: "pr",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-platform" {
		t.Fatalf("team id = %v, want team-platform (linked_issue must outrank the author)", teamID)
	}
	if teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Platform Team" {
		t.Fatalf("team name = %v, want Platform Team", teamName)
	}
	bySource := map[string]teamattribution.GithubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if _, present := bySource["author_membership"]; present {
		t.Fatalf("candidates = %+v, want NO author_membership row (CHAOS-5649 R179 rule 1: linked_issue already set a DIFFERENT primary team)", candidates)
	}
	if linked := bySource["linked_issue"]; linked.IsPrimary != 1 || teamattribution.GithubWorkItemDerivationStringValue(linked.TeamID) != "team-platform" {
		t.Fatalf("linked_issue candidate = %+v, want present, primary, team-platform", linked)
	}
}

func TestGitHubWorkItemDerivationCausalAuthorNeverOutranksARealLinkedIssueDonor(t *testing.T) {
	// The other half of the test above: rather than injecting the
	// linked_issue candidate directly, this drives the REAL production
	// builder end to end -- a Linear issue that resolves to team CHAOS via
	// its OWN project ownership fact (a first-class, donor-eligible source)
	// is linked to a GitHub PR whose author resolves to a DIFFERENT team via
	// the admin member layer. The PR must inherit the donor's team, not the
	// author's.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	donorProjectID := "linear-project-chaos"
	donor := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "linear:CHAOS-2400", Provider: "linear",
		ProjectID: &donorProjectID, OrgID: "org-acme",
	}
	pr := teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#100", Provider: "github", Type: "pr",
		OrgID: "org-acme",
	}
	reporter := "alice"
	pr.Reporter = &reporter
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Projects: []teamattribution.GithubWorkItemDerivationProjectFact{{
			Provider: "linear", ProjectID: donorProjectID, TeamID: "CHAOS", TeamName: "Chaos Team",
			IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{
		donor.WorkItemID: donor, pr.WorkItemID: pr,
	}
	derived.LinkedIssue, _, _ = derived.BuildLinkedIssueIndex(
		"github", subjects, []teamattribution.GithubWorkItemDerivationDependencyEdge{{
			SourceWorkItemID: pr.WorkItemID, TargetWorkItemID: donor.WorkItemID,
			RelationshipType: "relates_to", LastSynced: now, OrgID: "org-acme",
		}}, nil,
	)
	teamID, teamName, candidates := derived.Resolve(pr)
	if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "CHAOS" {
		t.Fatalf("team id = %v, want CHAOS (the real donor must outrank the author)", teamattribution.GithubWorkItemDerivationStringValue(teamID))
	}
	if teamattribution.GithubWorkItemDerivationStringValue(teamName) != "Chaos Team" {
		t.Fatalf("team name = %v, want Chaos Team", teamattribution.GithubWorkItemDerivationStringValue(teamName))
	}
	bySource := map[string]teamattribution.GithubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	if linked := bySource["linked_issue"]; linked.IsPrimary != 1 || teamattribution.GithubWorkItemDerivationStringValue(linked.TeamID) != "CHAOS" {
		t.Fatalf("linked_issue candidate = %+v, want present, primary, CHAOS", linked)
	}
	// CHAOS-5649 (R179 rule 1, chris 2026-09-12) supersedes this test's
	// ORIGINAL author-candidate assertion (present, non-primary, team-ops):
	// see TestGitHubWorkItemDerivationAuthorNeverOutranksALinkedIssueDonor
	// above for the full rationale -- the real donor already set a
	// DIFFERENT primary team (CHAOS), so author_membership for team-ops is
	// no longer recorded at all.
	if _, present := bySource["author_membership"]; present {
		t.Fatalf("candidates = %+v, want NO author_membership row (CHAOS-5649 R179 rule 1: the real donor already set a DIFFERENT primary team)", candidates)
	}
}

func TestGitHubWorkItemDerivationAuthorMembershipNeverAppliesToAGitLabIssue(t *testing.T) {
	// Negative control paired with the GitLab MR case in
	// TestGitHubWorkItemDerivationNeverInfersTeamFromPersonMembershipUnlessAdminMapped:
	// a GitLab ISSUE (WorkItemID "gitlab:acme/api#9", the "#" convention, not
	// "!") must stay unassigned on this signal alone, exactly like a GitHub
	// issue does -- the PR/MR gate is Provider+Type, not just Provider.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "gitlab", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, _, candidates := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "gitlab:acme/api#9", Provider: "gitlab", Type: "issue",
		Reporter: &reporter, OrgID: "org-acme",
	})
	if teamID != nil {
		t.Fatalf("team id = %v, want nil (GitLab issue must not gain author_membership)", teamattribution.GithubWorkItemDerivationStringValue(teamID))
	}
	if len(candidates) != 1 || candidates[0].Source != "unassigned" {
		t.Fatalf("candidates = %+v", candidates)
	}
}

func TestGitHubWorkItemDerivationReporterAndAssigneeSamePersonSameTeamStayDistinctProvenance(t *testing.T) {
	// When the author IS the assignee and both resolve to the SAME team,
	// resolve() keeps BOTH candidates as provenance -- one assignee_membership
	// row (rank 4, evidence "assignee=...") and one author_membership row
	// (rank 6, evidence "reporter=..."). Splitting the source (CHAOS-4244's
	// precedence ruling) makes them structurally distinct at the ClickHouse
	// storage key (source differs, not just evidence). The assignee_membership
	// row must win primary: it outranks author_membership even for the
	// identical team.
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{
		Members: []teamattribution.GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-ops", TeamName: "Ops Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	teamID, _, candidates := resolveViaMembership(t, derived, teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#77", Provider: "github", Type: "pr",
		Assignees: []string{"alice"}, Reporter: &reporter, OrgID: "org-acme",
	})
	if teamattribution.GithubWorkItemDerivationStringValue(teamID) != "team-ops" {
		t.Fatalf("team id = %v, want team-ops", teamID)
	}
	bySource := map[string]teamattribution.GithubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	assignee, ok := bySource["assignee_membership"]
	if !ok || assignee.IsPrimary != 0 || teamattribution.GithubWorkItemDerivationStringValue(assignee.TeamID) != "team-ops" {
		t.Fatalf("assignee candidate = %+v (present=%v), want present, non-primary (CHAOS-9414), team-ops", assignee, ok)
	}
	author, ok := bySource["author_membership"]
	if !ok || author.IsPrimary != 0 || teamattribution.GithubWorkItemDerivationStringValue(author.TeamID) != "team-ops" {
		t.Fatalf("author candidate = %+v (present=%v), want present, non-primary, team-ops", author, ok)
	}
	if assignee.Evidence == author.Evidence {
		t.Fatalf("expected distinct evidence (assignee= vs reporter=), got identical: %q", assignee.Evidence)
	}
}

// resolveViaMembership resolves a subject whose ONLY signal is a member's team.
// CHAOS-9414: membership is provenance only, so the primary team must be
// empty and the primary row unassigned; the returned team and candidates are
// the membership provenance row's team and the candidates without the
// unassigned row, so a test can still assert what membership resolved.
func resolveViaMembership(
	t *testing.T, derived teamattribution.GithubWorkItemDerivationContext,
	subject teamattribution.GithubWorkItemDerivationSubject,
) (*string, *string, []teamattribution.GithubWorkItemDerivationCandidate) {
	t.Helper()
	teamID, teamName, candidates := derived.Resolve(subject)
	if teamID != nil || teamName != nil {
		t.Fatalf("primary team = (%v, %v), want none: membership is provenance only (CHAOS-9414)",
			teamattribution.GithubWorkItemDerivationStringValue(teamID), teamattribution.GithubWorkItemDerivationStringValue(teamName))
	}
	var memberTeamID, memberTeamName *string
	var rest []teamattribution.GithubWorkItemDerivationCandidate
	unassigned := 0
	for _, candidate := range candidates {
		if candidate.Source == "unassigned" {
			unassigned++
			if candidate.IsPrimary != teamattribution.AttributionPrimary {
				t.Fatalf("unassigned is_primary = %d, want 1", candidate.IsPrimary)
			}
			continue
		}
		if candidate.IsPrimary != teamattribution.AttributionNotPrimary {
			t.Fatalf("%s is_primary = %d, want 0 (provenance only)", candidate.Source, candidate.IsPrimary)
		}
		if memberTeamID == nil {
			memberTeamID, memberTeamName = candidate.TeamID, candidate.TeamName
		}
		rest = append(rest, candidate)
	}
	if unassigned != 1 {
		t.Fatalf("candidates = %+v, want exactly one unassigned primary row", candidates)
	}
	return memberTeamID, memberTeamName, rest
}
