package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-8806: Jira issue comments are collected when the dataset options carry
// no fetch_comments (new and existing configurations alike).

func jiraCommentsClaim(options map[string]any) Claim {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = options
	return claim
}

// jiraManyCommentsDoer serves `total` comments for OPS-201 in pages of
// maxResults, and every other path through the stock Atlassian test doer.
func jiraManyCommentsDoer(t *testing.T, total int, commentRequests *int) jiraAtlassianDoerFunc {
	return func(request *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/comment") {
			return (&jiraAtlassianDoer{t: t}).Do(request)
		}
		*commentRequests++
		query := request.URL.Query()
		start, _ := strconv.Atoi(query.Get("startAt"))
		size, _ := strconv.Atoi(query.Get("maxResults"))
		items := make([]string, 0, size)
		for index := start; index < start+size && index < total; index++ {
			items = append(items, fmt.Sprintf(`{"id":"c%d","created":"2026-08-02T10:00:00Z","author":{"accountId":"commenter"},"body":"x"}`, index))
		}
		body := `{"comments":[` + strings.Join(items, ",") + `],"isLast":` + strconv.FormatBool(start+size >= total) + `}`
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
}

func collectJiraComments(t *testing.T, claim Claim, doer jiraAtlassianDoerFunc) CompleteRouteBatch {
	t.Helper()
	return collectJiraCommentsCtx(t, context.Background(), claim, doer)
}

func collectJiraCommentsCtx(t *testing.T, ctx context.Context, claim Claim, doer jiraAtlassianDoerFunc) CompleteRouteBatch {
	t.Helper()
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		ctx, claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func jiraInteractionRows(t *testing.T, batch CompleteRouteBatch) []jiraWorkItemInteractionRow {
	t.Helper()
	var rows []jiraWorkItemInteractionRow
	for _, effect := range batch.Effects {
		if effect.Destination != "work_item_interactions" {
			continue
		}
		for _, raw := range effect.Rows {
			var row jiraWorkItemInteractionRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func TestJiraAtlassianCommentsOnWhenOptionAbsent(t *testing.T) {
	requests := 0
	batch := collectJiraComments(t, jiraCommentsClaim(nil), jiraManyCommentsDoer(t, 2, &requests))
	if requests != 1 {
		t.Fatalf("comment requests=%d want=1", requests)
	}
	rows := jiraInteractionRows(t, batch)
	if len(rows) != 2 || rows[0].InteractionID != "c0" || rows[1].InteractionID != "c1" ||
		rows[0].Provider != "jira" || rows[0].WorkItemID != "jira:OPS-201" {
		t.Fatalf("stored interaction rows=%+v", rows)
	}
	if batch.Watermark == nil {
		t.Fatal("watermark held on a clean comment fetch")
	}
}

func TestJiraAtlassianCommentsExplicitFalseStaysOff(t *testing.T) {
	requests := 0
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"fetch_comments": false}), jiraManyCommentsDoer(t, 2, &requests))
	if requests != 0 || len(jiraInteractionRows(t, batch)) != 0 {
		t.Fatalf("comments fetched with fetch_comments=false: requests=%d", requests)
	}
}

func TestJiraAtlassianCommentsDefaultCapIs500AndExplicitLimitWins(t *testing.T) {
	requests := 0
	batch := collectJiraComments(t, jiraCommentsClaim(nil), jiraManyCommentsDoer(t, 1200, &requests))
	if rows := jiraInteractionRows(t, batch); len(rows) != 500 {
		t.Fatalf("default cap: rows=%d want=500", len(rows))
	}
	if requests != 10 {
		t.Fatalf("default cap: comment requests=%d want=10 (500 at 50 per page)", requests)
	}
	requests = 0
	batch = collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_limit": 3}), jiraManyCommentsDoer(t, 1200, &requests))
	if rows := jiraInteractionRows(t, batch); len(rows) != 3 {
		t.Fatalf("explicit limit: rows=%d want=3", len(rows))
	}
}

func TestJiraAtlassianCommentsFetchErrorHoldsWatermarkAndKeepsUnit(t *testing.T) {
	doer := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/comment") {
			return nil, context.DeadlineExceeded
		}
		return (&jiraAtlassianDoer{t: t}).Do(request)
	})
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer)
	if batch.Watermark != nil {
		t.Fatalf("comment failure advanced watermark: %v", batch.Watermark)
	}
	incomplete, ok := batch.Result["incomplete"].([]string)
	if !ok || len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-201" {
		t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
	}
	if len(jiraInteractionRows(t, batch)) != 0 {
		t.Fatal("interaction rows written despite comment fetch failure")
	}
	if len(batch.Effects) == 0 {
		t.Fatal("work item effects dropped by a comment failure")
	}
}

// jiraBudgetDoer serves `issues` issues (OPS-1..OPS-n) from the search and
// `perIssue` comments for each one. It counts comment and worklog reads.
type jiraBudgetDoer struct {
	t         *testing.T
	issues    int
	perIssue  int
	comments  int
	worklogs  int
	failIssue string
	// commentDelay is slept (honouring the request context) in every comment
	// read; slowWorklog is slept in the worklog read of one issue key.
	commentDelay   time.Duration
	slowWorklog    string
	slowWorklogFor time.Duration
	// afterCommentCtx: a comment read blocks until its context ends, then
	// waits this long more before it returns (it ignores the context).
	afterCommentCtx time.Duration
	// searchDelay / changelogDelay are slept (honouring the request context)
	// in the issue search and in every changelog read. lastIssueComments, when
	// above 0, replaces perIssue for the LAST issue only.
	searchDelay       time.Duration
	changelogDelay    time.Duration
	lastIssueComments int
	// slowCommentFor is slept in the comment read of OPS-3 and OPS-4 only.
	slowCommentFor time.Duration
}

func jiraSleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (doer *jiraBudgetDoer) Do(request *http.Request) (*http.Response, error) {
	respond := func(body string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	path := request.URL.Path
	switch {
	case path == "/rest/api/3/search/jql":
		if err := jiraSleep(request.Context(), doer.searchDelay); err != nil {
			return nil, err
		}
		items := make([]string, 0, doer.issues)
		for index := 1; index <= doer.issues; index++ {
			items = append(items, fmt.Sprintf(`{"id":"%d","key":"OPS-%d","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-%d","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"Issue %d","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T08:00:00Z","updated":"2026-08-02T09:00:00Z","resolutiondate":"2026-08-02T08:30:00Z"}}`, 20000+index, index, index, index))
		}
		return respond(`{"issues":[` + strings.Join(items, ",") + `],"isLast":true}`)
	case strings.HasSuffix(path, "/changelog"):
		if err := jiraSleep(request.Context(), doer.changelogDelay); err != nil {
			return nil, err
		}
		return respond(`{"values":[],"total":0,"isLast":true}`)
	case strings.HasSuffix(path, "/worklog"):
		doer.worklogs++
		if strings.Contains(path, "/"+doer.slowWorklog+"/") {
			if err := jiraSleep(request.Context(), doer.slowWorklogFor); err != nil {
				return nil, err
			}
		}
		return respond(`{"startAt":0,"maxResults":100,"total":0,"worklogs":[]}`)
	case strings.HasSuffix(path, "/comment"):
		doer.comments++
		key := strings.TrimSuffix(strings.TrimPrefix(path, "/rest/api/3/issue/"), "/comment")
		if err := jiraSleep(request.Context(), doer.commentDelay); err != nil {
			return nil, err
		}
		if key == "OPS-3" || key == "OPS-4" {
			if err := jiraSleep(request.Context(), doer.slowCommentFor); err != nil {
				return nil, err
			}
		}
		if doer.afterCommentCtx > 0 {
			<-request.Context().Done()
			time.Sleep(doer.afterCommentCtx)
			return nil, request.Context().Err()
		}
		if key == doer.failIssue {
			return nil, context.DeadlineExceeded
		}
		query := request.URL.Query()
		start, _ := strconv.Atoi(query.Get("startAt"))
		size, _ := strconv.Atoi(query.Get("maxResults"))
		perIssue := doer.perIssue
		if doer.lastIssueComments > 0 && key == fmt.Sprintf("OPS-%d", doer.issues) {
			perIssue = doer.lastIssueComments
		}
		items := make([]string, 0, size)
		for index := start; index < start+size && index < perIssue; index++ {
			items = append(items, fmt.Sprintf(`{"id":"%s-c%d","created":"2026-08-02T10:00:00Z","author":{"accountId":"commenter"},"body":"x"}`, key, index))
		}
		return respond(`{"comments":[` + strings.Join(items, ",") + `],"isLast":` + strconv.FormatBool(start+size >= perIssue) + `}`)
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func jiraWorkItemEffectRows(t *testing.T, batch CompleteRouteBatch) int {
	t.Helper()
	count := 0
	for _, effect := range batch.Effects {
		if effect.Destination == "work_items" {
			count += len(effect.Rows)
		}
	}
	return count
}

// The vet's repro (CHAOS-8806): 250 issues x 600 comments with the default cap
// of 500 would be 125,000 interaction rows, above the 100,000 rows per table
// the effect ledger accepts, which failed the whole unit and lost every
// work-item effect. The row budget (50,000) keeps the unit whole.
func TestJiraAtlassianCommentsRowBudgetKeepsLargeProjectUnitWhole(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 250, perIssue: 600}
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer.Do)
	if got := jiraWorkItemEffectRows(t, batch); got != 250 {
		t.Fatalf("work item rows=%d want=250", got)
	}
	if rows := jiraInteractionRows(t, batch); len(rows) != jiraAtlassianDefaultCommentsRowBudget {
		t.Fatalf("interaction rows=%d want=%d", len(rows), jiraAtlassianDefaultCommentsRowBudget)
	}
	if got := batch.Result["incomplete_nonholding"]; fmt.Sprint(got) != "[comments:budget:150]" {
		t.Fatalf("budget marker=%#v", got)
	}
	if _, held := batch.Result["incomplete"]; held || batch.Watermark == nil {
		t.Fatalf("budget case held the watermark: incomplete=%#v watermark=%v", batch.Result["incomplete"], batch.Watermark)
	}
	if doer.comments != 100*10 {
		t.Fatalf("comment requests=%d want=1000 (100 issues x 10 pages; skipped issues are never read)", doer.comments)
	}
}

func TestJiraAtlassianCommentsRowBudgetBoundary(t *testing.T) {
	for _, tc := range []struct {
		name         string
		budget       int
		wantRows     int
		wantSkipped  int
		wantMarker   bool
		wantComments int
	}{
		{"exactly at the budget", 10, 10, 3, true, 2},
		{"one over the budget", 11, 10, 3, true, 5},
		{"budget above the data", 100, 25, 0, false, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doer := &jiraBudgetDoer{t: t, issues: 5, perIssue: 5}
			batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_row_budget": tc.budget, "fetch_worklogs": true}), doer.Do)
			if rows := jiraInteractionRows(t, batch); len(rows) != tc.wantRows {
				t.Fatalf("rows=%d want=%d", len(rows), tc.wantRows)
			}
			marker, present := batch.Result["incomplete_nonholding"]
			if present != tc.wantMarker || (present && fmt.Sprint(marker) != fmt.Sprintf("[comments:budget:%d]", tc.wantSkipped)) {
				t.Fatalf("marker=%#v present=%v want skipped=%d", marker, present, tc.wantSkipped)
			}
			if doer.comments != tc.wantComments {
				t.Fatalf("comment reads=%d want=%d", doer.comments, tc.wantComments)
			}
			// A skipped comment read must not skip the rest of the issue's work.
			if doer.worklogs != 5 || jiraWorkItemEffectRows(t, batch) != 5 {
				t.Fatalf("worklog reads=%d work items=%d want 5 and 5", doer.worklogs, jiraWorkItemEffectRows(t, batch))
			}
			if batch.Watermark == nil {
				t.Fatal("watermark held")
			}
		})
	}
}

func TestJiraAtlassianCommentsRowBudgetOptionCanOnlyLowerIt(t *testing.T) {
	for option, want := range map[any]int{
		5: 5, 0: jiraAtlassianDefaultCommentsRowBudget, -1: jiraAtlassianDefaultCommentsRowBudget,
		1_000_000: jiraAtlassianDefaultCommentsRowBudget, "abc": jiraAtlassianDefaultCommentsRowBudget,
	} {
		if got := jiraAtlassianCommentsRowBudget(jiraCommentsClaim(map[string]any{"comments_row_budget": option})); got != want {
			t.Fatalf("option %v: budget=%d want=%d", option, got, want)
		}
	}
	if got := jiraAtlassianCommentsRowBudget(jiraCommentsClaim(nil)); got != jiraAtlassianDefaultCommentsRowBudget {
		t.Fatalf("absent option: budget=%d", got)
	}
	if jiraAtlassianDefaultCommentsRowBudget*2 > 100_000 {
		t.Fatal("default budget must stay well under the effect ledger bound")
	}
}

func TestJiraAtlassianCommentsFetchErrorStillHoldsWatermarkNextToBudget(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 4, perIssue: 3, failIssue: "OPS-1"}
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_row_budget": 3}), doer.Do)
	incomplete, _ := batch.Result["incomplete"].([]string)
	if len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-1" || batch.Watermark != nil {
		t.Fatalf("incomplete=%#v watermark=%v", batch.Result["incomplete"], batch.Watermark)
	}
	if rows := jiraInteractionRows(t, batch); len(rows) != 3 {
		t.Fatalf("rows=%d want=3", len(rows))
	}
}

// The second vet's case (CHAOS-8806): +1 serial comment request per issue can
// run the unit into its deadline, which is terminal and loses every work item.
// A TIME budget stops the comment reads at half of the work window.
func TestJiraAtlassianCommentsTimeBudgetKeepsUnitBeforeDeadline(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 60, perIssue: 3, commentDelay: 25 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	batch := collectJiraCommentsCtx(t, ctx, jiraCommentsClaim(nil), doer.Do)
	if time.Since(started) >= time.Second {
		t.Fatalf("unit ran into its deadline: %v", time.Since(started))
	}
	if got := jiraWorkItemEffectRows(t, batch); got != 60 {
		t.Fatalf("work item rows=%d want=60", got)
	}
	rows := len(jiraInteractionRows(t, batch))
	if rows == 0 || rows >= 60*3 {
		t.Fatalf("interaction rows=%d want some, not all", rows)
	}
	skipped, _ := batch.Result["comments_time_skipped_issues"].(int)
	markers, _ := batch.Result["incomplete_nonholding"].([]string)
	if skipped == 0 || skipped+rows/3 != 60 || len(markers) != 1 || markers[0] != fmt.Sprintf("comments:time:%d", skipped) {
		t.Fatalf("skipped=%d rows=%d markers=%#v", skipped, rows, markers)
	}
	if _, held := batch.Result["incomplete"]; held || batch.Watermark == nil {
		t.Fatalf("time budget held the watermark: %#v", batch.Result["incomplete"])
	}
}

func TestJiraAtlassianCommentsNoDeadlineMeansNoTimeBudget(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 20, perIssue: 3, commentDelay: 5 * time.Millisecond}
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer.Do)
	if rows := len(jiraInteractionRows(t, batch)); rows != 60 {
		t.Fatalf("rows=%d want=60", rows)
	}
	if _, present := batch.Result["incomplete_nonholding"]; present {
		t.Fatalf("marker without budget: %#v", batch.Result["incomplete_nonholding"])
	}
}

// Both budgets apply in one unit (phase 2, window 1 s, stop time at about
// 0.5 s): issues 1-2 land 6 rows; issue 3 (a 300 ms read) is cut by the row
// budget (7: one slot left, 3 comments); issue 4 starts at 0.3 s and its
// 300 ms read ends at the stop time; issues 5-20 are past it. Every work item
// still lands.
func TestJiraAtlassianCommentsBothBudgetsAndWarnLine(t *testing.T) {
	var logs strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previous)

	doer := &jiraBudgetDoer{t: t, issues: 20, perIssue: 3, slowCommentFor: 300 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	claim := jiraCommentsClaim(map[string]any{"comments_row_budget": 7})
	batch := collectJiraCommentsCtx(t, ctx, claim, doer.Do)
	if rows := len(jiraInteractionRows(t, batch)); rows != 6 {
		t.Fatalf("rows=%d want=6", rows)
	}
	if got := jiraWorkItemEffectRows(t, batch); got != 20 {
		t.Fatalf("work item rows=%d want=20", got)
	}
	markers, _ := batch.Result["incomplete_nonholding"].([]string)
	if fmt.Sprint(markers) != "[comments:budget:1 comments:time:17]" {
		t.Fatalf("markers=%#v", markers)
	}
	if batch.Watermark == nil {
		t.Fatal("watermark held")
	}
	line := logs.String()
	for _, want := range []string{"level=WARN", "providersync.jira.comments_budget_reached", "cause=rows+time", "skipped_issues_rows=1", "skipped_issues_time=17", "interaction_rows=6"} {
		if !strings.Contains(line, want) {
			t.Fatalf("WARN line lacks %q: %s", want, line)
		}
	}
	if strings.Contains(line, "commenter") {
		t.Fatalf("WARN line carries comment content: %s", line)
	}
}

// Survivor of the second vet: an explicit comments_limit of 0 means "no
// per-issue cap", so only the row budget bounds the read.
func TestJiraAtlassianCommentsExplicitZeroLimitIsStillBoundedByBudget(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 4, perIssue: 5}
	batch := collectJiraComments(t, jiraCommentsClaim(map[string]any{"comments_limit": 0, "comments_row_budget": 7}), doer.Do)
	// Issue 1 fits (5 rows); issues 2-4 meet the 2 slots left: cut, no rows, marker.
	if rows := len(jiraInteractionRows(t, batch)); rows != 5 {
		t.Fatalf("rows=%d want=5", rows)
	}
	if fmt.Sprint(batch.Result["incomplete_nonholding"]) != "[comments:budget:3]" {
		t.Fatalf("markers=%#v", batch.Result["incomplete_nonholding"])
	}
}

// Round 1 of #3851 (CHAOS-8806): a comment read that STARTS before the stop
// time must not run past it. One read blocks until its context ends: its own
// deadline (stopAt) ends it, the issue is counted as time-skipped, and every
// work item still lands (before the fix the read ran to the unit deadline and
// the next changelog read failed: error, zero effects).
func TestJiraAtlassianCommentsOneBlockedReadEndsAtTheStopTime(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 5, perIssue: 3, commentDelay: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	batch := collectJiraCommentsCtx(t, ctx, jiraCommentsClaim(nil), doer.Do)
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("unit ran into its deadline: %v", elapsed)
	}
	if got := jiraWorkItemEffectRows(t, batch); got != 5 {
		t.Fatalf("work item rows=%d want=5", got)
	}
	markers, _ := batch.Result["incomplete_nonholding"].([]string)
	if fmt.Sprint(markers) != "[comments:time:5]" || len(jiraInteractionRows(t, batch)) != 0 {
		t.Fatalf("markers=%#v rows=%d", markers, len(jiraInteractionRows(t, batch)))
	}
	if _, held := batch.Result["incomplete"]; held || batch.Watermark == nil {
		t.Fatalf("watermark held: %#v", batch.Result["incomplete"])
	}
}

// Control: a provider failure while the read context is alive is a fetch error
// (watermark held), even when it carries a deadline error of its own.
func TestJiraAtlassianCommentsFetchErrorWithDeadlineStillHoldsWatermark(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 3, perIssue: 3, failIssue: "OPS-2"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	batch := collectJiraCommentsCtx(t, ctx, jiraCommentsClaim(nil), doer.Do)
	incomplete, _ := batch.Result["incomplete"].([]string)
	if len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-2" || batch.Watermark != nil {
		t.Fatalf("incomplete=%#v watermark=%v", batch.Result["incomplete"], batch.Watermark)
	}
	if _, present := batch.Result["incomplete_nonholding"]; present {
		t.Fatalf("fetch error counted as budget: %#v", batch.Result["incomplete_nonholding"])
	}
}

// When the UNIT deadline is already over by the time the read returns, the
// failure is not the time budget: it stays a fetch error (marker, held).
func TestJiraAtlassianCommentsOuterDeadlineDoneIsNotBudget(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 1, perIssue: 3, afterCommentCtx: 700 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := jiraWorkItemsTestClient(t, fakehttp.Client(jiraAtlassianDoerFunc(doer.Do)), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		ctx, jiraCommentsClaim(nil), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	// The read ends at the stop time, but the OUTER deadline is over by the
	// time it returns: a fetch error (marker, watermark held, work items kept),
	// never a budget skip and never a failed unit.
	if err != nil {
		t.Fatalf("unit failed: %v", err)
	}
	incomplete, _ := batch.Result["incomplete"].([]string)
	if len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-1" || batch.Watermark != nil {
		t.Fatalf("incomplete=%#v watermark=%v", batch.Result["incomplete"], batch.Watermark)
	}
	if _, present := batch.Result["incomplete_nonholding"]; present {
		t.Fatalf("outer deadline counted as budget: %#v", batch.Result["incomplete_nonholding"])
	}
	if got := jiraWorkItemEffectRows(t, batch); got != 1 {
		t.Fatalf("work item rows=%d want=1", got)
	}
}

// What an interaction row may carry from the provider (CHAOS-8806 round 1):
// the actor is cut at 256 runes and a comment id longer than 64 bytes is
// skipped as a missing id, so no provider string is unbounded in a row.
func TestJiraInteractionRowBoundsProviderStrings(t *testing.T) {
	long := strings.Repeat("a", 5000)
	comments := []map[string]any{
		{"id": "1", "created": "2026-08-02T10:00:00Z", "author": map[string]any{"accountId": long}, "body": "x"},
		{"id": strings.Repeat("9", 65), "created": "2026-08-02T10:00:00Z", "body": "x"},
		{"id": strings.Repeat("9", 64), "created": "2026-08-02T10:00:00Z", "body": "x"},
	}
	rows := normalizeJiraInteractions(jiraCommentsClaim(nil), "jira:OPS-1", comments, nil, time.Now())
	if len(rows) != 2 || rows[0].Actor == nil || utf8.RuneCountInString(*rows[0].Actor) != jiraInteractionActorMaxRunes || len(rows[1].InteractionID) != 64 {
		t.Fatalf("rows=%+v", rows)
	}
}

// Round 2 P1-1 (CHAOS-8806): the time window starts at the START of Collect,
// not after the issue search. Reviewer probe: 2 s deadline, 400 ms search,
// six 170 ms changelogs, 600 ms comment reads. Comments off ends OK in about
// 1.4 s; comments on must end OK too, with the work items and a time marker.
func TestJiraAtlassianCommentsWindowStartsBeforeSearch(t *testing.T) {
	for _, fetch := range []bool{false, true} {
		doer := &jiraBudgetDoer{t: t, issues: 6, perIssue: 3, searchDelay: 400 * time.Millisecond,
			changelogDelay: 170 * time.Millisecond, commentDelay: 600 * time.Millisecond}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		batch := collectJiraCommentsCtx(t, ctx, jiraCommentsClaim(map[string]any{"fetch_comments": fetch}), doer.Do)
		cancel()
		if got := jiraWorkItemEffectRows(t, batch); got != 6 {
			t.Fatalf("fetch_comments=%v work item rows=%d want=6", fetch, got)
		}
		if !fetch {
			continue
		}
		markers, _ := batch.Result["incomplete_nonholding"].([]string)
		if len(markers) != 1 || !strings.HasPrefix(markers[0], "comments:time:") {
			t.Fatalf("markers=%#v", batch.Result["incomplete_nonholding"])
		}
	}
}

// Round 3 (CHAOS-8806): comment reads run in phase 2, after all of the work.
// With a slow search and every comment read hanging, the first read ends at the
// stop time, the other issues are skipped without a read, all are counted.
func TestJiraAtlassianCommentsHangingReadsAfterWorkEndAtTheStopTime(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 3, perIssue: 3, searchDelay: 600 * time.Millisecond, commentDelay: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	batch := collectJiraCommentsCtx(t, ctx, jiraCommentsClaim(nil), doer.Do)
	if doer.comments != 1 || fmt.Sprint(batch.Result["incomplete_nonholding"]) != "[comments:time:3]" {
		t.Fatalf("comment reads=%d markers=%#v", doer.comments, batch.Result["incomplete_nonholding"])
	}
	if got := jiraWorkItemEffectRows(t, batch); got != 3 {
		t.Fatalf("work item rows=%d want=3", got)
	}
}

func jiraCollectOutcome(t *testing.T, doer *jiraBudgetDoer, window time.Duration, fetch bool) (CompleteRouteBatch, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	client := jiraWorkItemsTestClient(t, fakehttp.Client(jiraAtlassianDoerFunc(doer.Do)), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	return jiraAtlassianCompleteHandler(t).Collect(
		ctx, jiraCommentsClaim(map[string]any{"fetch_comments": fetch}), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
}

// The invariant (round 3 vet, B1): for ANY timing, a unit that ends OK with
// comments off ends OK with comments on, with the same work-item effects.
// Every row is designed so comments off ends OK (checked: else the row is void).
func TestJiraAtlassianCommentsOnNeverCostsAUnitThatPassesWithCommentsOff(t *testing.T) {
	ms := time.Millisecond
	for _, tc := range []struct {
		name                       string
		window                     time.Duration
		issues                     int
		search, changelog, comment time.Duration
	}{
		{"vet probe: work at 91 percent", 2000 * ms, 10, 200 * ms, 160 * ms, 500 * ms},
		{"round 2 probe: work at 71 percent", 2000 * ms, 6, 400 * ms, 170 * ms, 600 * ms},
		{"everything fast", 1000 * ms, 5, 5 * ms, 5 * ms, 5 * ms},
		{"comment read slower than the window", 1000 * ms, 5, 10 * ms, 50 * ms, 5000 * ms},
		{"work at 90 percent, slow reads", 2000 * ms, 10, 100 * ms, 170 * ms, 300 * ms},
		{"work at 50 percent", 2000 * ms, 10, 100 * ms, 90 * ms, 400 * ms},
		{"no work cost, hanging reads", 1000 * ms, 20, 0, 0, time.Hour},
		{"many small reads", 2000 * ms, 30, 100 * ms, 20 * ms, 60 * ms},
		{"work at 80 percent, reads hang", 2000 * ms, 8, 200 * ms, 175 * ms, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newDoer := func() *jiraBudgetDoer {
				return &jiraBudgetDoer{t: t, issues: tc.issues, perIssue: 3, searchDelay: tc.search,
					changelogDelay: tc.changelog, commentDelay: tc.comment}
			}
			off, offErr := jiraCollectOutcome(t, newDoer(), tc.window, false)
			if offErr != nil {
				t.Fatalf("row void: comments off failed: %v", offErr)
			}
			on, onErr := jiraCollectOutcome(t, newDoer(), tc.window, true)
			if onErr != nil {
				t.Fatalf("comments off OK but comments on failed: %v", onErr)
			}
			if jiraWorkItemEffectRows(t, on) != tc.issues || jiraWorkItemEffectRows(t, on) != jiraWorkItemEffectRows(t, off) {
				t.Fatalf("work items on=%d off=%d want=%d", jiraWorkItemEffectRows(t, on), jiraWorkItemEffectRows(t, off), tc.issues)
			}
			if _, held := on.Result["incomplete"]; held || on.Watermark == nil {
				t.Fatalf("comments held the watermark: %#v", on.Result["incomplete"])
			}
		})
	}
}

// Round 2 P1-2 (CHAOS-8806): 101 issues, the first 100 with 499 comments and
// the last with 500. The budget leaves 100 slots for the last issue. It lands
// NO comment rows (a set cut at the budget is not kept: a re-read of an issue
// replaces its whole set, so none + marker is the consistent state), carries
// the budget marker and counts as skipped. Pre-built binary under ulimit -v.
func TestJiraAtlassianCommentsRowBudgetCutsLastIssueWithMarker(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 101, perIssue: 499, lastIssueComments: 500}
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer.Do)
	if got := jiraWorkItemEffectRows(t, batch); got != 101 {
		t.Fatalf("work item rows=%d want=101", got)
	}
	rows := jiraInteractionRows(t, batch)
	if len(rows) != 100*499 {
		t.Fatalf("interaction rows=%d want=%d (last issue lands none)", len(rows), 100*499)
	}
	for _, row := range rows {
		if strings.HasPrefix(row.WorkItemID, "jira:OPS-101") {
			t.Fatalf("partial rows landed for the cut issue: %#v", row)
		}
	}
	if fmt.Sprint(batch.Result["incomplete_nonholding"]) != "[comments:budget:1]" ||
		batch.Result["comments_budget_skipped_issues"] != 1 {
		t.Fatalf("markers=%#v skipped=%#v", batch.Result["incomplete_nonholding"], batch.Result["comments_budget_skipped_issues"])
	}
	if _, held := batch.Result["incomplete"]; held || batch.Watermark == nil {
		t.Fatalf("budget case held the watermark")
	}
}

// Control: the last issue FITS the remaining slots exactly: kept, no marker.
func TestJiraAtlassianCommentsRowBudgetExactFitHasNoMarker(t *testing.T) {
	doer := &jiraBudgetDoer{t: t, issues: 101, perIssue: 499, lastIssueComments: 100}
	batch := collectJiraComments(t, jiraCommentsClaim(nil), doer.Do)
	if rows := len(jiraInteractionRows(t, batch)); rows != jiraAtlassianDefaultCommentsRowBudget {
		t.Fatalf("rows=%d want=%d", rows, jiraAtlassianDefaultCommentsRowBudget)
	}
	if _, present := batch.Result["incomplete_nonholding"]; present {
		t.Fatalf("marker on an exact fit: %#v", batch.Result["incomplete_nonholding"])
	}
}
