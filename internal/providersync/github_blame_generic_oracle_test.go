package providersync

import (
	"testing"
	"time"
)

var oracleBlameGoOnlyFields = map[string]string{
	"org_id": "carried from the Go claim because Python's ClickHouseStore auto-injects its tenant after the producer boundary",
}

func buildGitHubBlameRowForOracle(t *testing.T, input map[string]any) gitBlameRow {
	t.Helper()
	var blameRange gitHubBlameRange
	lineNumber := func(key string) uint32 {
		switch value := input[key].(type) {
		case int:
			return uint32(value)
		case float64:
			return uint32(value)
		default:
			t.Fatalf("%s has unexpected type %T", key, input[key])
			return 0
		}
	}
	blameRange.StartingLine = lineNumber("starting_line")
	blameRange.EndingLine = lineNumber("ending_line")
	blameRange.Commit.OID = input["commit_sha"].(string)
	if value, ok := input["author"].(string); ok {
		blameRange.Commit.Author.Name = &value
	}
	if value, ok := input["author_email"].(string); ok {
		blameRange.Commit.Author.Email = &value
	}
	return newGitHubBlameRow(
		nativeTestClaim("github", "blame"), input["repo_id"].(string),
		input["path"].(string), blameRange.StartingLine, blameRange,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
}

func TestGenericOracleMatchesLivePythonForGitHubBlameRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/blame/row", []oracleCase{
		{ID: "named_author", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "src/main.go",
			"starting_line": 1, "ending_line": 2, "commit_sha": "abc123",
			"author": "Ada", "author_email": "ada@example.com",
		}},
		{ID: "provider_defaults", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "README.md",
			"starting_line": 7, "ending_line": 7, "commit_sha": "",
		}},
	}, buildGitHubBlameRowForOracle, oracleBlameGoOnlyFields)
}

func TestGenericOracleRediscoversGitHubBlameAuthorDefaultDefect(t *testing.T) {
	cases := []oracleCase{{ID: "provider_defaults", Input: map[string]any{
		"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "README.md",
		"starting_line": 7, "ending_line": 7, "commit_sha": "",
	}}}
	divergences := oracleDivergences(
		t, "github/blame/row", cases,
		func(t *testing.T, input map[string]any) any {
			row := buildGitHubBlameRowForOracle(t, input)
			wrong := ""
			row.AuthorName = &wrong
			return row
		},
		oracleBlameGoOnlyFields,
	)
	if len(divergences) == 0 {
		t.Fatal("oracle failed to rediscover the pre-fix empty-author defect")
	}
}
