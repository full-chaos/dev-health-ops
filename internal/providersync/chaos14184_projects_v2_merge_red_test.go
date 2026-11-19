package providersync

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A GitHub issue on a Projects v2 board has two status sources: the
// repository (close, reopen, status labels) and the board (Status field
// changes). The merged item must end its stored transitions in its stored
// status, whichever source moved last.

const chaos14184RepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

func chaos14184Now() time.Time { return time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC) }

func chaos14184Repository(t *testing.T, issue string, events ...string) githubWorkItemRows {
	t.Helper()
	return chaos14184RepositoryAt(t, chaos14184Now(), issue, events...)
}

func chaos14184RepositoryAt(t *testing.T, normalizedAt time.Time, issue string, events ...string) githubWorkItemRows {
	t.Helper()
	raw := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		raw = append(raw, json.RawMessage(event))
	}
	rows, err := normalizeGitHubIssueBundle(
		nativeTestClaim("github", "work-items"), sameGitHubRepoNames("acme/api"), uuid.MustParse(chaos14184RepoID),
		json.RawMessage(issue), raw, nil, nil, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func chaos14184Board(t *testing.T, item string, normalizedAt time.Time) githubWorkItemRows {
	t.Helper()
	var payload gitHubProjectV2ItemPayload
	if err := json.Unmarshal([]byte(item), &payload); err != nil {
		t.Fatal(err)
	}
	row, transitions, kept, err := normalizeGitHubProjectV2Item(
		nativeTestClaim("github", "work-items"), payload, "ghprojv2:acme/1", nil, normalizedAt,
	)
	if err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	return githubWorkItemRows{WorkItems: []githubWorkItemRow{row}, StatusTransitions: transitions}
}

func chaos14184CheckLastMatches(t *testing.T, merged githubWorkItemRows) {
	t.Helper()
	if len(merged.WorkItems) != 1 {
		t.Fatalf("work items=%d, want 1", len(merged.WorkItems))
	}
	item := merged.WorkItems[0]
	transitions := make([]githubWorkItemTransitionRow, 0, len(merged.StatusTransitions))
	for _, transition := range merged.StatusTransitions {
		if transition.WorkItemID == item.WorkItemID {
			transitions = append(transitions, transition)
		}
	}
	if len(transitions) == 0 {
		t.Fatal("no transitions")
	}
	sort.SliceStable(transitions, func(i, j int) bool { return transitions[i].OccurredAt.Before(transitions[j].OccurredAt) })
	last := transitions[len(transitions)-1]
	if last.ToStatus != item.Status {
		t.Fatalf("item status=%q but last transition (%s) to_status=%q", item.Status, last.OccurredAt.Format(time.RFC3339), last.ToStatus)
	}
}

const chaos14184ClosedIssue = `{"number":9,"title":"T","state":"closed","closed_at":"2026-10-03T00:00:00Z",
	"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-03T00:00:00Z","labels":[],"assignees":[]}`

const chaos14184OpenIssue = `{"number":9,"title":"T","state":"open",
	"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-03T00:00:00Z","labels":[],"assignees":[]}`

// Board card moved to In Progress on 10-02 and not moved again; the issue is
// closed in the repository on 10-03.
const chaos14184BoardInProgressClosedIssue = `{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
	"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"CLOSED",
	"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z","closedAt":"2026-10-03T00:00:00Z",
	"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
	"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"In Progress","field":{"name":"Status"}}]},
	"changes":{"nodes":[{"field":{"name":"Status"},"previousValue":{"name":"Todo"},"newValue":{"name":"In Progress"},"createdAt":"2026-10-02T00:00:00Z"}]}}`

// RED (main): repository close, no board move. Status comes from the board
// (in_progress); the last transition is the repository close (done).
func TestChaos14184RedRepositoryCloseWithNoBoardMove(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue,
		`{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`)
	board := chaos14184Board(t, chaos14184BoardInProgressClosedIssue, chaos14184Now())
	chaos14184CheckLastMatches(t, mergeGitHubProjectV2Rows(repository, board))
}

// RED (main), the reverse direction: board card moved to Done on 10-02, the
// issue is closed on 10-02 and reopened on 10-03 with no board move. Status
// comes from the board (done); the last transition is the reopen (todo).
func TestChaos14184RedRepositoryReopenAfterBoardDone(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184OpenIssue,
		`{"event":"closed","created_at":"2026-10-02T00:00:00Z"}`,
		`{"event":"reopened","created_at":"2026-10-03T00:00:00Z"}`)
	board := chaos14184Board(t, `{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"OPEN",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z",
		"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
		"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"Done","field":{"name":"Status"}}]},
		"changes":{"nodes":[{"field":{"name":"Status"},"previousValue":{"name":"In Progress"},"newValue":{"name":"Done"},"createdAt":"2026-10-02T00:00:00Z"}]}}`,
		chaos14184Now())
	chaos14184CheckLastMatches(t, mergeGitHubProjectV2Rows(repository, board))
}

// Refute (green on main, must stay green): the board moves after the
// repository close, so the board change is the last event and the board
// status is the item status.
func TestChaos14184RefuteBoardMoveAfterRepositoryClose(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue,
		`{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`)
	board := chaos14184Board(t, `{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"CLOSED",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-04T00:00:00Z","closedAt":"2026-10-03T00:00:00Z",
		"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
		"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"In Review","field":{"name":"Status"}}]},
		"changes":{"nodes":[{"field":{"name":"Status"},"previousValue":{"name":"In Progress"},"newValue":{"name":"In Review"},"createdAt":"2026-10-04T00:00:00Z"}]}}`,
		chaos14184Now())
	chaos14184CheckLastMatches(t, mergeGitHubProjectV2Rows(repository, board))
}
