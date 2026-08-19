package providersync

import (
	"encoding/json"
	"testing"
)

func TestGitHubWorkItemPRSocialCommentAdapterMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/pr-social-comment",
		[]oracleCase{{
			ID: "database_id_unicode_body",
			Input: map[string]any{"raw_node": map[string]any{
				"id": "IC_kwfallback", "databaseId": 9007199254740993,
				"body": "Looks good 👋", "createdAt": "2026-08-03T08:30:00.123456Z",
				"author": map[string]any{"login": "reviewer"},
			}},
		}, {
			ID: "full_database_id_fallback",
			Input: map[string]any{"raw_node": map[string]any{
				"id": "IC_kwfallback", "databaseId": 0,
				"fullDatabaseId": "9223372036854775808",
				"body":           "Full database ID", "createdAt": "2026-08-03T08:31:00Z",
				"author": map[string]any{"login": "reviewer"},
			}},
		}, {
			ID: "node_id_fallback",
			Input: map[string]any{"raw_node": map[string]any{
				"id": "IC_kwfinal", "databaseId": 0, "fullDatabaseId": "",
				"body": "Node ID", "createdAt": "2026-08-03T08:32:00Z",
				"author": map[string]any{"login": "reviewer"},
			}},
		}},
		buildGitHubWorkItemPRSocialCommentOracleRow,
		nil,
	)
}

func TestGitHubWorkItemPRSocialEventAdapterMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/pr-social-event",
		[]oracleCase{
			{ID: "merged", Input: map[string]any{"raw_node": map[string]any{
				"__typename": "MergedEvent", "createdAt": "2026-08-01T09:00:00Z",
				"actor": map[string]any{"login": "merger"},
			}}},
			{ID: "closed", Input: map[string]any{"raw_node": map[string]any{
				"__typename": "ClosedEvent", "createdAt": "2026-08-02T08:00:00.654321Z",
				"actor": map[string]any{"login": "closer"},
			}}},
			{ID: "reopened", Input: map[string]any{"raw_node": map[string]any{
				"__typename": "ReopenedEvent", "createdAt": "2026-08-03T08:00:00Z",
				"actor": map[string]any{"login": "reopener"},
			}}},
		},
		buildGitHubWorkItemPRSocialEventOracleRow,
		nil,
	)
}

func TestGitHubWorkItemPRSocialCommentIDFallbackMatchesPythonTruthiness(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "full_database_id_fallback",
			raw:  `{"databaseId":0,"fullDatabaseId":"9223372036854775808","id":"IC_kwfallback"}`,
			want: "9223372036854775808",
		},
		{
			name: "node_id_fallback",
			raw:  `{"databaseId":0,"fullDatabaseId":"","id":"IC_kwfinal"}`,
			want: "IC_kwfinal",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			row, err := adaptGitHubWorkItemPRSocialComment(json.RawMessage(test.raw))
			if err != nil {
				t.Fatal(err)
			}
			got, ok := row.ID.(string)
			if !ok || got != test.want {
				t.Fatalf("id=%v (%T), want string %q", row.ID, row.ID, test.want)
			}
		})
	}
}

func buildGitHubWorkItemPRSocialCommentOracleRow(
	t *testing.T, input map[string]any,
) githubWorkItemPRSocialComment {
	t.Helper()
	raw, err := json.Marshal(input["raw_node"])
	if err != nil {
		t.Fatal(err)
	}
	row, err := adaptGitHubWorkItemPRSocialComment(raw)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func buildGitHubWorkItemPRSocialEventOracleRow(
	t *testing.T, input map[string]any,
) githubWorkItemPRSocialEvent {
	t.Helper()
	raw, err := json.Marshal(input["raw_node"])
	if err != nil {
		t.Fatal(err)
	}
	row, err := adaptGitHubWorkItemPRSocialEvent(raw)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
