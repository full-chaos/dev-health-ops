package providersync

import (
	"fmt"
	"testing"
)

// A raw status name the label map knows must give the same status on a
// Projects v2 board column: the served row and the board event, merged through
// settleGitHubProjectV2Status, agree with the label map.
func TestChaos14301BoardMapAcceptsEveryLabelName(t *testing.T) {
	labelNames := []string{
		"in progress", "doing", "wip", "in review", "needs review", "blocked",
		"todo", "to do", "ready", "done", "wontfix", "duplicate",
	}
	for _, name := range labelNames {
		want := githubIssueStatus("", []string{name})
		if want == "unknown" {
			t.Fatalf("%q: not a label name", name)
		}
		if got := githubProjectV2Status(name, nil, ""); got != want {
			t.Errorf("%q: board=%s label=%s", name, got, want)
		}
	}
}

func TestChaos14301BoardOnlyServedStatus(t *testing.T) {
	cases := []struct{ name, want string }{
		{"needs review", "in_review"}, {"wontfix", "canceled"}, {"duplicate", "canceled"},
		{" Needs  Review ", "in_review"}, {"WONTFIX", "canceled"},
	}
	at := "2026-10-02T00:00:00Z"
	for _, c := range cases {
		t.Run(fmt.Sprintf("%q open", c.name), func(t *testing.T) {
			board := chaos14184Board(t, chaos14184Item("OPEN", "", c.name, at), chaos14184Now())
			merged := chaos14184Merge(t, chaos14184EmptyRows(), board)
			chaos14184Check(t, merged, c.want, c.name)
			if len(merged.StatusTransitions) != 1 || merged.StatusTransitions[0].ToStatus != c.want {
				t.Fatalf("events=%+v, want one to %s", merged.StatusTransitions, c.want)
			}
		})
		t.Run(fmt.Sprintf("%q closed", c.name), func(t *testing.T) {
			board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-01T12:00:00Z", c.name, at), chaos14184Now())
			merged := chaos14184Merge(t, chaos14184EmptyRows(), board)
			chaos14184Check(t, merged, c.want, c.name)
			if len(merged.StatusTransitions) != 1 || merged.StatusTransitions[0].ToStatus != c.want {
				t.Fatalf("events=%+v, want one to %s", merged.StatusTransitions, c.want)
			}
		})
	}
}

func TestChaos14301LabelAndBoardInTheSameSecondGiveOneStatus(t *testing.T) {
	at := "2026-10-02T00:00:00Z"
	for _, c := range []struct{ name, want string }{
		{"needs review", "in_review"}, {"wontfix", "canceled"}, {"duplicate", "canceled"},
	} {
		repo := chaos14184Repository(t, chaos14301LabelIssue("open", "", c.name), chaos14301LabeledEvent(c.name, at))
		board := chaos14184Board(t, chaos14184Item("OPEN", "", c.name, at), chaos14184Now())
		merged := chaos14184Merge(t, repo, board)
		chaos14184Check(t, merged, c.want, c.name)
	}
}

func chaos14301LabelIssue(state, closedAt, name string) string {
	closed := ""
	if closedAt != "" {
		closed = `,"closed_at":"` + closedAt + `"`
	}
	return `{"number":9,"title":"T","state":"` + state + `"` + closed + `,
	"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-03T00:00:00Z","labels":[{"name":"` + name + `"}],"assignees":[]}`
}

func chaos14301LabeledEvent(name, at string) string {
	return `{"event":"labeled","created_at":"` + at + `","label":{"name":"` + name + `"}}`
}
