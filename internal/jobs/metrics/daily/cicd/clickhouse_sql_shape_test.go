package cicd

import (
	"regexp"
	"testing"
)

var (
	sqlBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sqlLineComment  = regexp.MustCompile(`--[^\n]*`)
	finalClause     = regexp.MustCompile(`(?i)\bFROM\s+ci_pipeline_runs\s+FINAL\b`)
)

func TestPipelineRunsQueryReadsWithFinal(t *testing.T) {
	stripped := sqlLineComment.ReplaceAllString(sqlBlockComment.ReplaceAllString(pipelineRunsQuery, " "), "")
	if !finalClause.MatchString(stripped) {
		t.Fatalf("pipelineRunsQuery must read ci_pipeline_runs FINAL (ReplacingMergeTree), got:\n%s", stripped)
	}
}
