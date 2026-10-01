package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-7361: the history the acr touch reader sees must be complete from the
// item's creation. These tests run the REAL producers (Linear Collect and
// Jira Atlassian Collect over a faked HTTP wire) and apply the reader's own
// definition of a touch (q7349c.sql): per (subject, project) pair, touches in
// (occurred_at, event_id) order, the from side being a REMOVE and the to side
// an ADD; the state the system exists to reach is "no pair's first touch is a
// REMOVE".

// firstTouchRemovePairs returns the (subject, project) pairs whose first touch
// is a REMOVE, by the acr reader's definition.
func firstTouchRemovePairs(rows []projectmembership.Row) []string {
	type touch struct {
		at   time.Time
		id   string
		kind string
	}
	pairs := map[string][]touch{}
	for _, row := range rows {
		if row.FromProjectID != "" {
			key := row.SubjectID + "|" + row.FromProjectID
			pairs[key] = append(pairs[key], touch{row.OccurredAt, row.EventID, "R"})
		}
		if row.ToProjectID != "" {
			key := row.SubjectID + "|" + row.ToProjectID
			pairs[key] = append(pairs[key], touch{row.OccurredAt, row.EventID, "A"})
		}
	}
	bad := []string{}
	for key, touches := range pairs {
		sort.SliceStable(touches, func(i, j int) bool {
			if !touches[i].at.Equal(touches[j].at) {
				return touches[i].at.Before(touches[j].at)
			}
			return touches[i].id < touches[j].id
		})
		if touches[0].kind == "R" {
			bad = append(bad, key)
		}
	}
	sort.Strings(bad)
	return bad
}

func membershipRowsOf(t *testing.T, effects []EffectBatch) []projectmembership.Row {
	t.Helper()
	rows := []projectmembership.Row{}
	for _, effect := range effects {
		if effect.Destination != "project_membership_transitions" {
			continue
		}
		for _, raw := range effect.Rows {
			var row projectmembership.Row
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func linearCreationRows(t *testing.T, issueJSON string, fetchHistory bool) ([]projectmembership.Row, map[string]any) {
	t.Helper()
	batch := linearCreationBatch(t, issueJSON, fetchHistory)
	return membershipRowsOf(t, batch.Effects), batch.Result
}

func linearCreationBatch(t *testing.T, issueJSON string, fetchHistory bool) CompleteRouteBatch {
	t.Helper()
	doer := &linearWorkItemsDoer{responses: []string{linearTeamResponse(),
		`{"data":{"issues":{"nodes":[` + issueJSON + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	no := false
	handler := LinearWorkItemsRouteHandler{PerPage: 50, MaxPages: 10, FetchCycles: &no}
	if !fetchHistory {
		handler.FetchHistory = &no
	}
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func linearIssue(created, project, historyNodes string) string {
	projectJSON := "null"
	if project != "" {
		projectJSON = `{"id":"` + project + `","name":"Name ` + project + `"}`
	}
	return `{"id":"lin-1","identifier":"ENG-7","title":"t","description":"d","priority":2,
	"createdAt":"` + created + `","updatedAt":"2026-07-28T16:30:00Z","state":{"name":"Todo","type":"unstarted"},
	"labels":{"nodes":[]},"team":{"id":"team-eng","key":"ENG","name":"Engineering"},"project":` + projectJSON + `,
	"history":{"nodes":[` + historyNodes + `]}}`
}

func TestLinearCreationAddIsTheFirstTouchOfAnIssueCreatedInsideAProject(t *testing.T) {
	created := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		project    string
		history    string
		wantAdd    string // creation project; "" = no creation row expected
		wantRows   int
		wantAddAt  time.Time
		wantResult int
	}{
		{"no project history", "P", ``, "P", 1, created, 1},
		{"first row is a move P to Q: creation project is P not current Q", "Q",
			`{"id":"h1","createdAt":"2026-07-26T10:00:00Z","fromProjectId":"P","toProjectId":"Q"}`, "P", 2, created, 1},
		{"first row is a removal P to none: creation project is P", "",
			`{"id":"h1","createdAt":"2026-07-26T10:00:00Z","fromProjectId":"P"}`, "P", 2, created, 1},
		{"created with no project, added later: history already holds the add", "P",
			`{"id":"h1","createdAt":"2026-07-26T10:00:00Z","toProjectId":"P"}`, "", 1, time.Time{}, 0},
		{"no project ever", "", ``, "", 0, time.Time{}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, result := linearCreationRows(t, linearIssue("2026-07-25T09:00:00Z", tc.project, tc.history), true)
			if len(rows) != tc.wantRows {
				t.Fatalf("rows=%+v want %d", rows, tc.wantRows)
			}
			if bad := firstTouchRemovePairs(rows); len(bad) != 0 {
				t.Fatalf("pairs whose first touch is a REMOVE: %v (rows=%+v)", bad, rows)
			}
			adds := 0
			for _, row := range rows {
				if row.FromProjectID == "" && row.ToProjectID == tc.wantAdd && row.OccurredAt.Equal(tc.wantAddAt) && tc.wantAdd != "" {
					adds++
					if row.Actor != "" || len(row.EventID) != 32 {
						t.Fatalf("creation row=%+v", row)
					}
				}
			}
			if tc.wantAdd != "" && adds != 1 {
				t.Fatalf("want exactly one creation ADD of %q at %v, rows=%+v", tc.wantAdd, tc.wantAddAt, rows)
			}
			if result["membership_creation_adds"] != tc.wantResult {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestLinearCreationAddIsIdempotentAcrossSyncs(t *testing.T) {
	issue := linearIssue("2026-07-25T09:00:00Z", "P", ``)
	first, _ := linearCreationRows(t, issue, true)
	second, _ := linearCreationRows(t, issue, true)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	// Same sorting key (occurred_at, event_id) => ReplacingMergeTree collapses.
	if first[0].SortingKey() != second[0].SortingKey() {
		t.Fatalf("sorting key differs between syncs: %q vs %q", first[0].SortingKey(), second[0].SortingKey())
	}
}

func TestLinearCreationAddSkipsAreCountedNeverSilentNeverClockStamped(t *testing.T) {
	t.Run("history off", func(t *testing.T) {
		rows, result := linearCreationRows(t, linearIssue("2026-07-25T09:00:00Z", "P", ``), false)
		if len(rows) != 0 || result["membership_creation_skipped"] != 1 {
			t.Fatalf("rows=%+v result=%+v", rows, result)
		}
	})
	t.Run("history off, issue has no current project: still counted (an earlier project is unprovable)", func(t *testing.T) {
		rows, result := linearCreationRows(t, linearIssue("2026-07-25T09:00:00Z", "", ``), false)
		if len(rows) != 0 || result["membership_creation_skipped"] != 1 {
			t.Fatalf("rows=%+v result=%+v", rows, result)
		}
	})
	t.Run("unparseable createdAt: no row, never the sync clock", func(t *testing.T) {
		rows, result := linearCreationRows(t, linearIssue("not-a-time", "P", ``), true)
		if len(rows) != 0 || result["membership_creation_skipped"] != 1 {
			t.Fatalf("rows=%+v result=%+v", rows, result)
		}
	})
	t.Run("created not before first history row: an ADD after a move would re-open P", func(t *testing.T) {
		rows, result := linearCreationRows(t, linearIssue("2026-07-27T00:00:00Z", "Q",
			`{"id":"h1","createdAt":"2026-07-26T10:00:00Z","fromProjectId":"P","toProjectId":"Q"}`), true)
		if len(rows) != 1 || result["membership_creation_skipped"] != 1 {
			t.Fatalf("rows=%+v result=%+v", rows, result)
		}
	})
}

func TestLinearHistoryEndMismatchIsCounted(t *testing.T) {
	// History ends in Q, work_items.project_id says P.
	_, result := linearCreationRows(t, linearIssue("2026-07-25T09:00:00Z", "P",
		`{"id":"h1","createdAt":"2026-07-26T10:00:00Z","toProjectId":"Q"}`), true)
	if result["membership_history_end_mismatch"] != 1 {
		t.Fatalf("result=%+v", result)
	}
	_, result = linearCreationRows(t, linearIssue("2026-07-25T09:00:00Z", "Q",
		`{"id":"h1","createdAt":"2026-07-26T10:00:00Z","toProjectId":"Q"}`), true)
	if result["membership_history_end_mismatch"] != 0 {
		t.Fatalf("result=%+v", result)
	}
}

// jiraCreationDoer serves one issue and its changelog; every optional path
// answers empty.
type jiraCreationDoer struct {
	t         *testing.T
	issue     string
	changelog string
}

func (doer *jiraCreationDoer) Do(request *http.Request) (*http.Response, error) {
	body := `{}`
	switch {
	case request.URL.Path == "/rest/api/3/search/jql":
		body = `{"issues":[` + doer.issue + `],"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-9/changelog"):
		body = doer.changelog
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/project/"):
		return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func jiraCreationRows(t *testing.T, created, changelogValues string) ([]projectmembership.Row, map[string]any) {
	t.Helper()
	batch := jiraCreationBatch(t, created, changelogValues)
	return membershipRowsOf(t, batch.Effects), batch.Result
}

func jiraCreationBatch(t *testing.T, created, changelogValues string) CompleteRouteBatch {
	t.Helper()
	createdField := ""
	if created != "" {
		createdField = `"created":"` + created + `",`
	}
	issue := `{"id":"10060","key":"OPS-9","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-9","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"s","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Task"},"labels":[],` + createdField + `"updated":"2026-08-02T09:00:00Z"}}`
	doer := &jiraCreationDoer{t: t, issue: issue, changelog: `{"values":[` + changelogValues + `],"total":` + strconv.Itoa(strings.Count(changelogValues, `"items"`)) + `,"isLast":true}`}
	claim := jiraAtlassianClaim()
	claim.DatasetOptions = map[string]any{"fetch_worklogs": false, "fetch_board_sprints": false, "fetch_comments": false}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

const jiraMoveFromOtherToOps = `{"id":"500","created":"2026-08-01T09:00:00.000+0000","author":{"accountId":"a"},"items":[{"field":"project","from":"10002","fromString":"Billing","to":"10001","toString":"Operations"}]}`

func TestJiraCreationAddIsTheFirstTouchOfAnIssueCreatedInsideAProject(t *testing.T) {
	created := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	t.Run("no project history: creation project is the current one, with its key", func(t *testing.T) {
		rows, result := jiraCreationRows(t, "2026-07-30T08:00:00Z", ``)
		if len(rows) != 1 || bad(rows) {
			t.Fatalf("rows=%+v", rows)
		}
		row := rows[0]
		if row.FromProjectID != "" || row.ToProjectID != "10001" || row.ToProjectKey != "OPS" ||
			!row.OccurredAt.Equal(created) || row.Provider != "jira" || result["membership_creation_adds"] != 1 {
			t.Fatalf("row=%+v result=%+v", row, result)
		}
	})
	t.Run("first row is a move Billing to Operations: creation project is Billing", func(t *testing.T) {
		rows, _ := jiraCreationRows(t, "2026-07-30T08:00:00Z", jiraMoveFromOtherToOps)
		if len(rows) != 2 || bad(rows) {
			t.Fatalf("rows=%+v", rows)
		}
		found := false
		for _, row := range rows {
			if row.FromProjectID == "" && row.ToProjectID == "10002" && row.OccurredAt.Equal(created) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no creation ADD of 10002 at creation time: %+v", rows)
		}
	})
	t.Run("idempotent: second sync has the same sorting key", func(t *testing.T) {
		a, _ := jiraCreationRows(t, "2026-07-30T08:00:00Z", ``)
		b, _ := jiraCreationRows(t, "2026-07-30T08:00:00Z", ``)
		if len(a) != 1 || len(b) != 1 || a[0].SortingKey() != b[0].SortingKey() {
			t.Fatalf("a=%+v b=%+v", a, b)
		}
	})
	t.Run("missing created: no row, never the sync clock, counted", func(t *testing.T) {
		rows, result := jiraCreationRows(t, "", ``)
		if len(rows) != 0 || result["membership_creation_skipped"] != 1 {
			t.Fatalf("rows=%+v result=%+v", rows, result)
		}
	})
	t.Run("history ends elsewhere than work_items.project_id: counted", func(t *testing.T) {
		_, result := jiraCreationRows(t, "2026-07-30T08:00:00Z",
			`{"id":"501","created":"2026-08-01T09:00:00.000+0000","author":{"accountId":"a"},"items":[{"field":"project","from":"10001","fromString":"Operations","to":"10003","toString":"Other"}]}`)
		if result["membership_history_end_mismatch"] != 1 {
			t.Fatalf("result=%+v", result)
		}
	})
}

func bad(rows []projectmembership.Row) bool { return len(firstTouchRemovePairs(rows)) != 0 }

// CHAOS-7361 backfill fact: the creation time stored in work_items equals,
// at the stored precision (DateTime64(3) = milliseconds), the occurred_at the
// ADD row writes, so a derivation from ClickHouse alone reproduces the same
// (occurred_at, event_id) key the sync writes.
func TestCreationAddMatchesTheStoredWorkItemCreatedAt(t *testing.T) {
	msTime := func(value time.Time) time.Time { return value.UTC().Truncate(time.Millisecond) }
	rederive := func(t *testing.T, workItemCreated time.Time, add projectmembership.Row) {
		t.Helper()
		stored := msTime(workItemCreated)
		if !stored.Equal(add.OccurredAt) {
			t.Fatalf("stored created_at %v != ADD occurred_at %v", stored, add.OccurredAt)
		}
		again := add
		again.OccurredAt, again.EventID = stored, ""
		if got := projectmembership.EventID(again); got != add.EventID {
			t.Fatalf("event_id derived from the stored created_at %q != written %q", got, add.EventID)
		}
	}
	t.Run("linear, sub-millisecond provider time", func(t *testing.T) {
		batch := linearCreationBatch(t, linearIssue("2026-07-25T09:00:00.123456Z", "P", ``), true)
		var item linearWorkItemRow
		for _, effect := range batch.Effects {
			if effect.Destination == "work_items" {
				if err := json.Unmarshal(effect.Rows[0], &item); err != nil {
					t.Fatal(err)
				}
			}
		}
		adds := membershipRowsOf(t, batch.Effects)
		if len(adds) != 1 {
			t.Fatalf("rows=%+v", adds)
		}
		rederive(t, item.CreatedAt, adds[0])
	})
	t.Run("jira", func(t *testing.T) {
		batch := jiraCreationBatch(t, "2026-07-30T08:00:00.123456Z", ``)
		var item jiraWorkItemRow
		for _, effect := range batch.Effects {
			if effect.Destination == "work_items" {
				if err := json.Unmarshal(effect.Rows[0], &item); err != nil {
					t.Fatal(err)
				}
			}
		}
		adds := membershipRowsOf(t, batch.Effects)
		if len(adds) != 1 {
			t.Fatalf("rows=%+v", adds)
		}
		rederive(t, item.CreatedAt, adds[0])
	})
}
