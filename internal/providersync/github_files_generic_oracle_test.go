package providersync

import (
	"testing"
	"time"
)

var oracleFilesGoOnlyFields = map[string]string{
	"last_synced": "stamped from normalizedAt by the Go complete-route handler after Python's backfill_file_records boundary",
	"org_id":      "carried from the Go claim to keep ClickHouse writes tenant-scoped",
}

func buildGitHubFileRowForOracle(t *testing.T, input map[string]any) gitFileRow {
	t.Helper()
	contents, _ := input["contents_by_path"].(map[string]any)
	content, _ := contents[input["path"].(string)].(string)
	return newGitHubFileRow(
		nativeTestClaim("github", "files"), input["repo_id"].(string), input["path"].(string), content,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
}

func TestGenericOracleMatchesLivePythonForGitHubFilesRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/files/row", []oracleCase{
		{ID: "scannable_content", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "src/main.go",
			"contents_by_path": map[string]any{"src/main.go": "package main\n"},
		}},
		{ID: "path_only", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "README.md",
		}},
	}, buildGitHubFileRowForOracle, oracleFilesGoOnlyFields)
}

func TestGitHubFilesOracleLoaderExecutesStubbedBaseGitProducer(t *testing.T) {
	caseInput := map[string]any{
		"repo_id":          "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"path":             "src/main.go",
		"contents_by_path": map[string]any{"src/main.go": "package main\n"},
	}
	divergences := oracleDivergences(
		t,
		"github/files/row",
		[]oracleCase{{ID: "stubbed_base_git", Input: caseInput}},
		func(t *testing.T, input map[string]any) any { return buildGitHubFileRowForOracle(t, input) },
		oracleFilesGoOnlyFields,
	)
	if len(divergences) != 0 {
		t.Fatalf("github/files oracle loader divergences=%v", divergences)
	}
}
