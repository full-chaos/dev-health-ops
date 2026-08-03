package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitHubBlameFileOutcomesOracleRow struct {
	AttemptedPaths []string `json:"attempted_paths"`
	PersistedPaths []string `json:"persisted_paths"`
	Raised         bool     `json:"raised"`
}

func TestGitHubBlamePerFileFailureMatchesLivePython(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/blame/outcomes", []oracleCase{{
			ID: "middle_file_fails",
			Input: map[string]any{
				"file_paths": []string{
					"src/file-000.go", "src/file-001.go", "src/file-002.go",
				},
				"failed_paths": []string{"src/file-001.go"},
			},
		}},
		func(t *testing.T, input map[string]any) gitHubBlameFileOutcomesOracleRow {
			attempted := []string{}
			failed := map[string]bool{}
			for _, path := range stringsFromOracleInput(t, input["failed_paths"]) {
				failed[path] = true
			}
			client := gitHubRepositoryClient(t, gitHubBlameDoer{
				t: t, fileCount: len(stringsFromOracleInput(t, input["file_paths"])),
				blamePaths: &attempted, graphQLErrPaths: failed,
			}, "https://api.github.com")
			batch, err := (GitHubBlameRouteHandler{
				Coverage: staticGitHubBlameCoverage{}, MaxFiles: gitHubBlameMaxFiles,
			}).Collect(
				context.Background(), nativeTestClaim("github", "blame"),
				providerfoundation.Credential{}, client,
				time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
			)
			row := gitHubBlameFileOutcomesOracleRow{
				AttemptedPaths: attempted, Raised: err != nil,
			}
			if err != nil {
				return row
			}
			for _, effect := range batch.Effects {
				if effect.Destination != "git_blame" {
					continue
				}
				for _, raw := range effect.Rows {
					var blame gitBlameRow
					if err := json.Unmarshal(raw, &blame); err != nil {
						t.Fatal(err)
					}
					row.PersistedPaths = append(row.PersistedPaths, blame.Path)
				}
			}
			return row
		}, nil,
	)
}
