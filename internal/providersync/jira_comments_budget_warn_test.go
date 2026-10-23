package providersync

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

const jiraBudgetWarnMessage = "providersync.jira.comments_budget_reached"

func captureJiraWarnLogs(t *testing.T) *strings.Builder {
	t.Helper()
	var logs strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func TestJiraAtlassianPermanentOnlyLogsNoBudgetWarn(t *testing.T) {
	logs := captureJiraWarnLogs(t)
	batch := collectJiraComments(t, jiraCommentsClaim(nil), failOneIssueComments(t, jiraPermanentCommentCases[0]))
	if fmt.Sprint(batch.Result["incomplete_nonholding"]) != "[comments:permanent:1]" || batch.Result["comments_permanent_skipped_issues"] != 1 {
		t.Fatalf("markers=%#v", batch.Result["incomplete_nonholding"])
	}
	for _, key := range []string{"comments_budget_skipped_issues", "comments_time_skipped_issues"} {
		if _, present := batch.Result[key]; present {
			t.Fatalf("count key %s on a permanent-only run", key)
		}
	}
	line := logs.String()
	if strings.Contains(line, jiraBudgetWarnMessage) {
		t.Fatalf("budget warn on a permanent-only run: %s", line)
	}
	if !strings.Contains(line, "providersync.jira.comments_read_permanent") {
		t.Fatalf("permanent warn missing: %s", line)
	}
}

func TestJiraAtlassianRowBudgetOnlyKeepsWarnCauseRows(t *testing.T) {
	logs := captureJiraWarnLogs(t)
	doer := &jiraBudgetDoer{t: t, issues: 4, perIssue: 3}
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_row_budget": 7}), doer.Do)
	if batch.Result["comments_budget_skipped_issues"] == nil || batch.Result["comments_time_skipped_issues"] != 0 {
		t.Fatalf("keys=%#v", batch.Result)
	}
	line := logs.String()
	for _, want := range []string{jiraBudgetWarnMessage, "cause=rows"} {
		if !strings.Contains(line, want) {
			t.Fatalf("warn lacks %q: %s", want, line)
		}
	}
	if strings.Contains(line, "comments_read_permanent") {
		t.Fatalf("permanent warn without a permanent skip: %s", line)
	}
}

func TestJiraAtlassianTimeBudgetOnlyKeepsWarnCauseTime(t *testing.T) {
	logs := captureJiraWarnLogs(t)
	doer := &jiraBudgetDoer{t: t, issues: 60, perIssue: 3, commentDelay: 25 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	collectJiraCommentsCtx(t, ctx, jiraCommentsClaim(nil), doer.Do)
	line := logs.String()
	if !strings.Contains(line, jiraBudgetWarnMessage) || !strings.Contains(line, "cause=time") || strings.Contains(line, "cause=rows") {
		t.Fatalf("warn line: %s", line)
	}
}

func TestJiraAtlassianBudgetAndPermanentLogBothWarns(t *testing.T) {
	logs := captureJiraWarnLogs(t)
	inner := &jiraBudgetDoer{t: t, issues: 4, perIssue: 3}
	doer := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/rest/api/3/issue/OPS-1/comment" {
			return jiraPermanentCommentCases[0].respond(request)
		}
		return inner.Do(request)
	})
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_row_budget": 7}), doer)
	markers, _ := batch.Result["incomplete_nonholding"].([]string)
	if len(markers) != 2 || markers[0] != "comments:budget:1" || markers[1] != "comments:permanent:1" {
		t.Fatalf("markers=%#v", markers)
	}
	line := logs.String()
	for _, want := range []string{jiraBudgetWarnMessage, "cause=rows", "providersync.jira.comments_read_permanent"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log lacks %q: %s", want, line)
		}
	}
}
