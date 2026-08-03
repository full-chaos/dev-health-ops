package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var oracleGitLabRepositoryGoOnlyFields = map[string]string{
	"org_id": "carried from the tenant-scoped Go claim",
}

func buildGitLabRepositoryRowForOracle(
	t *testing.T,
	input map[string]any,
) repositoryRow {
	t.Helper()
	encoded, err := json.Marshal(input["project"])
	if err != nil {
		t.Fatal(err)
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (GitLabRepositoryRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "repo-metadata"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, &gitLabRepositoryDoer{
			t: t, body: string(encoded),
		}, input["gitlab_url"].(string)), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestGenericOracleMatchesLivePythonForGitLabRepositoryRow(t *testing.T) {
	cases := []oracleCase{
		{ID: "gitlab_dot_com", Input: map[string]any{
			"gitlab_url": "https://gitlab.com", "normalized_at": "2026-07-23T12:30:00Z",
			"project": map[string]any{
				"id": 123, "name": "api", "path_with_namespace": "Acme/API",
				"web_url": "https://gitlab.com/Acme/API", "default_branch": "main",
			},
		}},
		{ID: "self_managed", Input: map[string]any{
			"gitlab_url": "https://GITLAB.example:8443/api/v4", "normalized_at": "2026-07-23T12:30:00.123Z",
			"project": map[string]any{
				"id": 123, "name": "api", "path_with_namespace": "Acme/API",
				"web_url": nil, "default_branch": "main",
			},
		}},
	}
	compareRowsAgainstPythonOracle(
		t, "gitlab/repo-metadata/row", cases,
		buildGitLabRepositoryRowForOracle, oracleGitLabRepositoryGoOnlyFields,
	)
}
