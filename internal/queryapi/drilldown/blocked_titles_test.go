package drilldown

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

const (
	titleOrgA   = "org-a"
	titleOrgB   = "org-b"
	titleUUID   = "0b8f6a52-3c1d-4e7a-9d2e-5f4a1c7b8e90"
	titleSource = "FROM work_item_blocked_durations_daily"
)

// blockedTitleClient answers the blocked-items read with four rows and the
// work_items read from titlesByOrg, keyed by the org_id the query was bound
// to, so a read that is not bound to the caller's organisation serves
// another organisation's title.
func blockedTitleClient(t *testing.T, titlesByOrg map[string]map[string]string, titleErr error) fakeQueryClient {
	t.Helper()
	blocked := [][]any{
		{"github:acme/api#1", "github", "team-a", "Team A", uint64(4)},
		{"gitlab:group/api#2", "gitlab", "team-a", "Team A", uint64(4)},
		{"jira:OPS-3", "jira", "team-a", "Team A", uint64(4)},
		{"linear:ENG-4", "linear", "team-a", "Team A", uint64(4)},
	}
	return fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, titleSource) {
			return &fixtureRowScanner{rows: blocked}, nil
		}
		if !strings.Contains(query, "FROM work_items") {
			t.Fatalf("unexpected read:\n%s", query)
		}
		if titleErr != nil {
			return nil, titleErr
		}
		org, _ := bindingValue(bindings, "org_id")
		ids, _ := bindingValue(bindings, "work_item_ids")
		var rows [][]any
		for _, id := range ids.([]string) {
			if title, ok := titlesByOrg[org.(string)][id]; ok {
				rows = append(rows, []any{id, title})
			}
		}
		return &fixtureRowScanner{rows: rows}, nil
	}}
}

func blockedItemsJSON(t *testing.T, client fakeQueryClient) []map[string]any {
	t.Helper()
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	resp, err := BuildIssuesResponse(context.Background(), reader, titleOrgA, IssueParams{
		StartDay: day(2026, 10, 1, 0, 0, 0), EndDay: day(2026, 10, 8, 0, 0, 0), Limit: 50, BlockedOnly: true,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	value, err := pyjson.FromGoModel(resp)
	if err != nil {
		t.Fatalf("FromGoModel: %v", err)
	}
	raw, err := pyjson.MarshalModel(value)
	if err != nil {
		t.Fatalf("MarshalModel: %v", err)
	}
	var wire struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return wire.Items
}

func TestBlockedItemsServeTheStoredTitleAndNullWhenAbsent(t *testing.T) {
	items := blockedItemsJSON(t, blockedTitleClient(t, map[string]map[string]string{
		titleOrgA: {
			"github:acme/api#1":  "  Fix login redirect ",
			"gitlab:group/api#2": "   ",
			"jira:OPS-3":         titleUUID,
		},
	}, nil))
	if len(items) != 4 {
		t.Fatalf("items = %d, want 4", len(items))
	}
	want := map[string]any{
		"github:acme/api#1":  "Fix login redirect",
		"gitlab:group/api#2": nil,
		"jira:OPS-3":         nil,
		"linear:ENG-4":       nil,
	}
	for _, item := range items {
		title, present := item["title"]
		if !present {
			t.Fatalf("%v: title key missing, want the key with title or null", item["work_item_id"])
		}
		if title != want[item["work_item_id"].(string)] {
			t.Fatalf("%v: title = %#v, want %#v", item["work_item_id"], title, want[item["work_item_id"].(string)])
		}
		if title == item["work_item_id"] {
			t.Fatalf("%v: the id was served as the title", item["work_item_id"])
		}
	}
}

func TestBlockedItemsNeverServeAnotherOrganisationsTitle(t *testing.T) {
	items := blockedItemsJSON(t, blockedTitleClient(t, map[string]map[string]string{
		titleOrgB: {"github:acme/api#1": "Other org secret title"},
	}, nil))
	for _, item := range items {
		if item["title"] != nil {
			t.Fatalf("%v: title = %#v, want null (only org-b stores a title)", item["work_item_id"], item["title"])
		}
	}
}

func TestBlockedItemsTitleLookupIsBoundToTheCallersOrganisation(t *testing.T) {
	var query string
	var org any
	client := blockedTitleClient(t, nil, nil)
	inner := client.handler
	client.handler = func(t *testing.T, q string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(q, "FROM work_items") {
			query = q
			org, _ = bindingValue(b, "org_id")
		}
		return inner(t, q, b)
	}
	blockedItemsJSON(t, client)
	if org != titleOrgA {
		t.Fatalf("title lookup org_id binding = %v, want %s", org, titleOrgA)
	}
	if !strings.Contains(query, "org_id = {org_id:String}") {
		t.Fatalf("title lookup has no org predicate:\n%s", query)
	}
}

func TestBlockedItemsKeepRowsAndLogWhenTheTitleLookupFails(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	items := blockedItemsJSON(t, blockedTitleClient(t, nil, errors.New("title store down")))
	if len(items) != 4 {
		t.Fatalf("items = %d, want 4 kept rows", len(items))
	}
	for _, item := range items {
		if title, present := item["title"]; !present || title != nil {
			t.Fatalf("%v: title = %#v present=%v, want explicit null", item["work_item_id"], title, present)
		}
	}
	if !strings.Contains(logged.String(), "title store down") {
		t.Fatalf("lookup failure was not logged: %q", logged.String())
	}
}

func TestOrdinaryIssueItemsCarryNoTitleKey(t *testing.T) {
	value, err := pyjson.FromGoModel(IssueItem{WorkItemID: "w-1", Provider: "github", Status: "done"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pyjson.MarshalModel(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"title"`) {
		t.Fatalf("ordinary issue item carries a title key: %s", raw)
	}
}

func TestBlockedItemsDropATitleEqualToTheirOwnID(t *testing.T) {
	items := blockedItemsJSON(t, blockedTitleClient(t, map[string]map[string]string{
		titleOrgA: {
			"github:acme/api#1":  "github:acme/api#1",
			"gitlab:group/api#2": "Fix gitlab:group/api#2 crash",
			"jira:OPS-3":         "  JIRA:ops-3 ",
			"linear:ENG-4":       "Fix login redirect",
		},
	}, nil))
	want := map[string]any{
		"github:acme/api#1":  nil,
		"gitlab:group/api#2": "Fix gitlab:group/api#2 crash",
		"jira:OPS-3":         nil,
		"linear:ENG-4":       "Fix login redirect",
	}
	for _, item := range items {
		id := item["work_item_id"].(string)
		if item["title"] != want[id] {
			t.Fatalf("%s: title = %#v, want %#v", id, item["title"], want[id])
		}
	}
}
