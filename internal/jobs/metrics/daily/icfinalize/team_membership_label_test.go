package icfinalize

import (
	"fmt"
	"testing"
)

// gitBaseTeamID is the team the repo/user family writes on every git row of user_metrics_daily
// (repouser/compute.go, pinned there by its own test): no ownership team ever reaches the column.
const gitBaseTeamID = "unassigned"

// membershipLabelViolations returns the rows whose team_id is neither the person's first membership team nor
// "unassigned": a team the column must never hold (an ownership team, or any other source).
func membershipLabelViolations(rows []ICUserMetric, firstMembership map[string]string) []string {
	var out []string
	for _, row := range rows {
		want := gitBaseTeamID
		if team, ok := firstMembership[row.IdentityID]; ok {
			want = team
		}
		if row.TeamID != want {
			out = append(out, fmt.Sprintf("%s: team_id %q, want %q", row.IdentityID, row.TeamID, want))
		}
	}
	return out
}

// membershipResolver is the shape computeForDay builds: it always answers, with the person's first membership team or
// "unassigned", so a stored or base team never survives the merge.
func membershipResolver(firstMembership map[string]string) TeamResolver {
	return func(identity string) (string, bool) {
		if team, ok := firstMembership[identity]; ok {
			return team, true
		}
		return gitBaseTeamID, true
	}
}

// user_metrics_daily.team_id is a membership label, not team attribution (docs/reference/metrics/definitions.md): the
// person's first team membership on the day, else "unassigned". A base row that carries an owner team must not survive
// the merge, and the check must catch a planted ownership source (the red pair).
func TestUserMetricsTeamIDIsTheMembershipLabel(t *testing.T) {
	firstMembership := map[string]string{"member@example.test": "team-member"}

	t.Run("membership team or unassigned", func(t *testing.T) {
		merged := MergeICUserMetrics([]GitUserMetric{
			{AuthorEmail: "member@example.test", TeamID: gitBaseTeamID},
			{AuthorEmail: "loner@example.test", TeamID: gitBaseTeamID},
		}, []WorkItemUserMetric{{UserIdentity: "items-only@example.test"}}, membershipResolver(firstMembership))
		if len(merged) != 3 {
			t.Fatalf("merged %d rows, want 3", len(merged))
		}
		if violations := membershipLabelViolations(merged, firstMembership); len(violations) != 0 {
			t.Fatalf("user_metrics_daily.team_id holds a team that is not the membership label: %v", violations)
		}
	})

	t.Run("a base row that carries an owner team does not survive", func(t *testing.T) {
		merged := MergeICUserMetrics([]GitUserMetric{
			{AuthorEmail: "loner@example.test", TeamID: "owner-team-of-the-repository"},
		}, nil, membershipResolver(firstMembership))
		if violations := membershipLabelViolations(merged, firstMembership); len(violations) != 0 {
			t.Fatalf("an owner team on the base row reached the column: %v", violations)
		}
	})

	t.Run("a planted ownership source is caught", func(t *testing.T) {
		// The red pair: a resolver backed by repository ownership instead of memberships.
		ownership := TeamResolver(func(string) (string, bool) { return "owner-team-of-the-repository", true })
		merged := MergeICUserMetrics([]GitUserMetric{
			{AuthorEmail: "member@example.test", TeamID: gitBaseTeamID},
			{AuthorEmail: "loner@example.test", TeamID: gitBaseTeamID},
		}, nil, ownership)
		if violations := membershipLabelViolations(merged, firstMembership); len(violations) != 2 {
			t.Fatalf("the check does not catch an ownership team on a person row: violations %v", violations)
		}
	})
}
