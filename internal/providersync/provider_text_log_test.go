package providersync

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestJiraBoardSprintSkipLogsNoProviderText: a board whose sprint listing
// answers the documented 400 is skipped, and the line records the skip class
// and the board id, never Jira's own errorMessages text. Not parallel: it
// swaps the process default logger.
func TestJiraBoardSprintSkipLogsNoProviderText(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&output, slog.LevelInfo))
	defer slog.SetDefault(previous)
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
		jiraTeamCatalogProjectSearchURI: {body: `{"values":[{"key":"OPS","name":"Ops Project"}]}`},
		"/rest/api/3/project/OPS":       {body: `{"projectTypeKey":"software"}`},
		"/rest/agile/1.0/board?maxResults=100&projectKeyOrId=OPS&startAt=0": {
			body: `{"values":[{"id":81}],"isLast":true}`,
		},
		"/rest/agile/1.0/board/81/sprint?maxResults=100&startAt=0": {
			status: http.StatusBadRequest,
			body:   `{"errorMessages":["canary provider text for board"]}`,
		},
	}}
	_, err := JiraTeamCatalogRouteHandler{}.CollectTeamCatalog(context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1", Strict: true},
		providerfoundation.Credential{Provider: "jira"}, jiraTeamCatalogTestClient(t, doer),
		TeamCatalogSelections{Teams: true}, time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	line := output.String()
	if strings.Contains(line, "canary") || !strings.Contains(line, `"skip_reason":"board_rejects_sprints"`) || !strings.Contains(line, `"board_id":"81"`) {
		t.Fatalf("skip line: %s", line)
	}
}

// TestSkippedArtifactSampleLogsIDsOnlyInTheIDShape: the skipped-artifact
// sample renders the artifact name, run id and artifact id only when each
// passes logging.ProviderAssignedID.
func TestSkippedArtifactSampleLogsIDsOnlyInTheIDShape(t *testing.T) {
	t.Parallel()
	sample := githubTestsSkippedArtifactLogSample([]GitHubTestsSkippedArtifact{
		{Name: "test-results-1", Cause: "unreadable_archive", RunID: "12345", ArtifactID: "678"},
		{Name: "canary name with spaces", Cause: "oversized", RunID: "1e+100", ArtifactID: strings.Repeat("9", 65)},
	})
	joined := strings.Join(sample, "|")
	if !strings.Contains(joined, "test-results-1 (unreadable_archive, run=12345, artifact=678)") {
		t.Fatalf("well-shaped entry changed: %q", joined)
	}
	if strings.Contains(joined, "canary") || strings.Contains(joined, "1e+100") || strings.Contains(joined, strings.Repeat("9", 65)) ||
		!strings.Contains(joined, "[id_dropped] (oversized, run=[id_dropped], artifact=[id_dropped])") {
		t.Fatalf("malformed ids reached the sample: %q", joined)
	}
}
