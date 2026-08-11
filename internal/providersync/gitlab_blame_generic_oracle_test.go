package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

func buildGitLabBlameRowForOracle(t *testing.T, input map[string]any) gitBlameRow {
	t.Helper()
	lines, ok := input["lines"].([]string)
	if !ok || len(lines) == 0 {
		t.Fatalf("lines has unexpected value %T or is empty", input["lines"])
	}
	commit := map[string]any{}
	for _, field := range []string{"commit_id", "author_name", "author_email"} {
		if value, exists := input[field]; exists {
			key := field
			if field == "commit_id" {
				key = "id"
			}
			commit[key] = value
		}
	}
	payload, err := json.Marshal([]map[string]any{{"lines": lines, "commit": commit}})
	if err != nil {
		t.Fatal(err)
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(payload, &rawItems); err != nil {
		t.Fatal(err)
	}
	ranges := normalizeGitLabBlameItems(rawItems)
	if len(ranges) != 1 {
		t.Fatalf("normalized ranges=%+v", ranges)
	}
	return newGitLabBlameRow(
		nativeTestClaim("gitlab", "blame"), input["repo_id"].(string),
		input["path"].(string), uint32(ranges[0].StartingLine), ranges[0],
		ranges[0].Lines[0], time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
}

func TestGenericOracleMatchesLivePythonForGitLabBlameRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "gitlab/blame/row", []oracleCase{
		{ID: "named_author", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "src/main.go",
			"lines": []string{"package main"}, "commit_id": "abc123",
			"author_name": "Ada", "author_email": "ada@example.com",
		}},
		{ID: "provider_defaults", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "README.md",
			"lines": []string{"readme"},
		}},
	}, buildGitLabBlameRowForOracle, oracleBlameGoOnlyFields)
}

func TestGenericOracleRediscoversGitLabBlameAuthorDefaultDefect(t *testing.T) {
	divergences := oracleDivergences(
		t, "gitlab/blame/row", []oracleCase{{ID: "provider_defaults", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "path": "README.md",
			"lines": []string{"readme"},
		}}},
		func(t *testing.T, input map[string]any) any {
			row := buildGitLabBlameRowForOracle(t, input)
			wrong := ""
			row.AuthorName = &wrong
			return row
		}, oracleBlameGoOnlyFields,
	)
	if len(divergences) == 0 {
		t.Fatal("oracle failed to rediscover the pre-fix empty-author defect")
	}
}
