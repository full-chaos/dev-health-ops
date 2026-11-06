package linkeditems

import (
	"context"
	"errors"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

func day(d int) time.Time { return time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC) }

func linkedVersions() []Version {
	done := day(3).Add(2 * time.Hour)
	started := day(2)
	mk := func(id, assignee, status string, completed *time.Time) Version {
		return Version{
			Item: workitemmetrics.StoredItem{
				WorkItemID: id, Provider: "jira", Status: status, CreatedAt: day(1), CompletedAt: completed,
				StartedAt: &started, Assignees: []string{assignee}, Type: "task",
			},
			RepoID: "11111111-1111-1111-1111-111111111111", Synced: day(4),
		}
	}
	return []Version{mk("jira:A-1", "alice", "done", &done), mk("jira:A-2", "bob", "in_progress", nil)}
}

// The user grain is the same compute over the same days and items as the team grain: both come out of one triplet.
func TestUserRowsByDayKeepsTheDaysAndItemsOfRowsByDay(t *testing.T) {
	versions := linkedVersions()
	team := RowsByDay(day(1), day(6), versions, nil)
	user := UserRowsByDay(day(1), day(6), versions, nil)
	if len(user) == 0 {
		t.Fatal("no user rows")
	}
	if len(team) != len(user) {
		t.Fatalf("team days %d, user days %d", len(team), len(user))
	}
	completedTeam, completedUser := 0, 0
	for d, rows := range team {
		if _, ok := user[d]; !ok {
			t.Fatalf("day %s has team rows and no user rows", d)
		}
		for _, r := range rows {
			completedTeam += r.ItemsCompleted
		}
	}
	people := map[string]bool{}
	for _, rows := range user {
		for _, r := range rows {
			completedUser += r.ItemsCompleted
			people[r.UserIdentity] = true
		}
	}
	if completedTeam != 1 || completedUser != completedTeam {
		t.Fatalf("completed: team %d, user %d, want 1 each", completedTeam, completedUser)
	}
	if !people["alice"] || !people["bob"] || len(people) != 2 {
		t.Fatalf("people = %v, want alice and bob", people)
	}
}

type failingClient struct{ calls int }

func (c *failingClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.calls++
	return nil, errors.New("max_result_rows exceeded")
}

// C13: a bound hit is an error of the read; it is never an answer over more items.
func TestUserDailyBoundHitIsAnErrorEveryTime(t *testing.T) {
	client := &failingClient{}
	daily := NewUserDaily(Loader{Client: client, OrgID: "org", RepoIDs: []string{"r1"}, WindowStart: day(1), WindowEnd: day(6), Budget: UserBudget})
	for i := 0; i < 2; i++ {
		if rows, err := daily.Rows(context.Background()); err == nil || rows != nil {
			t.Fatalf("rows %v err %v, want the bound error and no rows", rows, err)
		}
	}
	if client.calls != 1 {
		t.Fatalf("%d statements, want the one read shared by the request", client.calls)
	}
}

// C6: no repository reads nothing.
func TestUserDailyWithNoRepositoryReadsNothing(t *testing.T) {
	client := &failingClient{}
	daily := NewUserDaily(Loader{Client: client, OrgID: "org", WindowStart: day(1), WindowEnd: day(6), Budget: UserBudget})
	rows, err := daily.Rows(context.Background())
	if err != nil || len(rows) != 0 || client.calls != 0 {
		t.Fatalf("rows %v err %v statements %d, want none", rows, err, client.calls)
	}
}
