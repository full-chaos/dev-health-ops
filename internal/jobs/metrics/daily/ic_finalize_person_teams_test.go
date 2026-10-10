package daily

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// refusingConn is a connection whose every read fails. The embedded interface
// is nil: a write or any other call would panic, so the test also holds that
// nothing but a read was tried.
type refusingConn struct {
	driver.Conn
	queries []string
}

func (conn *refusingConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	return nil, errors.New("clickhouse refused the read")
}

// A failed read of the memberships fails the run before anything is written.
// With "no membership" in its place every person of the day would be written
// as unassigned and every point under a team would be retracted.
func TestTheLandscapeFinalizeFailsTheRunWhenTheMembershipsCannotBeRead(t *testing.T) {
	conn := &refusingConn{}
	rows, err := NewICFinalizeExecutor(conn).ComputeFinalizeFamily(context.Background(), Run{
		ID: "run-1", OrganizationID: "org-1", TargetDay: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC),
	})
	if err == nil || !strings.Contains(err.Error(), "load the team memberships of the organization") {
		t.Fatalf("err = %v, want the failed read of the memberships", err)
	}
	if rows != 0 || len(conn.queries) != 1 || !strings.Contains(conn.queries[0], "team_memberships") {
		t.Fatalf("rows = %d, reads = %d; want no row and the one read of team_memberships", rows, len(conn.queries))
	}
}

// TestPersonTeamsOfGivesEachTeamOnceInTheRankOrderOfTheAttribution holds the
// index the landscape finalize builds from membership facts: a person is
// found by an identity facet (compared as the attribution compares
// identities), gets each team one time, and the teams are in the rank order
// of the attribution, so the first team is not the order of a query.
func TestPersonTeamsOfGivesEachTeamOnceInTheRankOrderOfTheAttribution(t *testing.T) {
	email := "Solo@Example.com"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	member := func(provider, team, memberID string, primary, specificity, priority int, facets ...string) teamattribution.GithubWorkItemDerivationMemberFact {
		return teamattribution.GithubWorkItemDerivationMemberFact{
			Provider: provider, TeamID: team, TeamName: "Team " + team, MemberID: memberID,
			IdentityFacets: facets, IsPrimary: primary, Specificity: specificity, Priority: priority, UpdatedAt: at,
		}
	}
	teamsOf := personTeamsOf([]teamattribution.GithubWorkItemDerivationMemberFact{
		// The facts come in an order that is not the rank order.
		member("github", "github:zeta", "gh:dev", 0, 60, 20, "github:dev", "dev@example.com"),
		member("jira", "jira:ENG", "jira:dev", 1, 100, 10, "jira:accountid:1", "dev@example.com"),
		member("gitlab", "gitlab:alpha", "gl:dev", 0, 60, 20, "gitlab:dev", "dev@example.com"),
		// One team named by two rows of the person: listed one time.
		member("github", "github:zeta", "gh:dev-old", 0, 60, 30, "dev@example.com"),
		// A higher specificity outranks a lower one among the non-primary rows.
		member("linear", "linear:core", "linear:dev", 0, 80, 20, "dev@example.com"),
		// A row with no facet is found by its member id and its email.
		{Provider: "jira", TeamID: "jira:OPS", MemberID: "jira:solo", RawEmail: &email, IsPrimary: 1, UpdatedAt: at},
		// A row with no team names no team.
		member("jira", " ", "jira:lost", 1, 100, 10, "lost@example.com"),
	})

	for person, want := range map[string][]string{
		// primary first; then specificity 80; then the two of specificity 60
		// and priority 20 by team id.
		"dev@example.com":    {"jira:ENG", "linear:core", "github:zeta", "gitlab:alpha"},
		"  DEV@Example.COM ": {"jira:ENG", "linear:core", "github:zeta", "gitlab:alpha"},
		"github:dev":         {"github:zeta"},
		"jira:solo":          {"jira:OPS"},
		"solo@example.com":   {"jira:OPS"},
		"lost@example.com":   nil,
		"nobody@example.com": nil,
		"":                   nil,
	} {
		if got := teamsOf(person); !reflect.DeepEqual(got, want) {
			t.Errorf("teams of %q = %v, want %v", person, got, want)
		}
	}
}
