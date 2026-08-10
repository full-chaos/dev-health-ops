package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabBlameFileOutcomesOracleRow struct {
	AttemptedPaths []string `json:"attempted_paths"`
	PersistedPaths []string `json:"persisted_paths"`
	Raised         bool     `json:"raised"`
}

func TestGitLabBlamePerFileFailureMatchesLivePython(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/blame/outcomes", []oracleCase{{
			ID: "middle_file_fails",
			Input: map[string]any{
				"file_paths": []string{
					"src/file-000.go", "src/file-001.go", "src/file-002.go",
				},
				"failed_paths": []string{"src/file-001.go"},
			},
		}},
		func(t *testing.T, input map[string]any) gitLabBlameFileOutcomesOracleRow {
			filePaths := stringsFromOracleInput(t, input["file_paths"])
			failedPaths := stringsFromOracleInput(t, input["failed_paths"])
			failed := make(map[string]bool, len(failedPaths))
			for _, path := range failedPaths {
				failed[path] = true
			}
			attempted := []string{}
			client := gitLabRepositoryClient(t, &gitLabBlameDoer{
				t: t, fileCount: len(filePaths), paths: &attempted, failedPaths: failed,
				blameLines: 1,
			}, "https://gitlab.example")
			batch, err := (GitLabBlameRouteHandler{
				Coverage: staticGitLabBlameCoverage{}, MaxFiles: gitLabBlameMaxFiles,
			}).Collect(context.Background(), nativeTestClaim("gitlab", "blame"),
				providerfoundation.Credential{}, client,
				time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
			row := gitLabBlameFileOutcomesOracleRow{AttemptedPaths: attempted, Raised: err != nil}
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
