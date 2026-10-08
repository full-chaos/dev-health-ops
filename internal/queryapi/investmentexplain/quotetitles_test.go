package investmentexplain

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

const (
	qtOrgA   = "org-a"
	qtOrgB   = "org-b"
	qtRepo   = "11111111-2222-4333-8444-555555555555"
	qtPRRef  = qtRepo + "#pr482"
	qtPRBlnk = qtRepo + "#pr483"
	qtPRUUID = qtRepo + "#pr484"
	qtUUID   = "0b8f6a52-3c1d-4e7a-9d2e-5f4a1c7b8e90"
	qtIssue1 = "github:acme/api#1"
	qtIssue2 = "jira:OPS-2"
	qtIssue3 = "linear:ENG-3"
)

// reflectScanner assigns each row value into the matching Scan destination:
// a nil value leaves the zero value, a value of the pointee type is stored,
// and a value of the type a pointer field points to allocates that pointer.
type reflectScanner struct {
	rows  [][]any
	index int
}

func (s *reflectScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *reflectScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		if row[i] == nil {
			continue
		}
		target := reflect.ValueOf(d).Elem()
		value := reflect.ValueOf(row[i])
		switch {
		case value.Type().AssignableTo(target.Type()):
			target.Set(value)
		case target.Kind() == reflect.Pointer && value.Type().AssignableTo(target.Type().Elem()):
			ptr := reflect.New(target.Type().Elem())
			ptr.Elem().Set(value)
			target.Set(ptr)
		default:
			return errors.New("reflectScanner: cannot assign " + value.Type().String() + " to " + target.Type().String())
		}
	}
	return nil
}
func (s *reflectScanner) Err() error   { return nil }
func (s *reflectScanner) Close() error { return nil }

type quoteTitleStore struct {
	issueTitles map[string]map[string]string // org -> work_item_id -> title
	prTitles    map[string]map[string]string // org -> "repo#number" -> title
	titleErr    error
	prErr       error
	queries     []string
	prBindings  []dhclickhouse.Binding
}

func (c *quoteTitleStore) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, query)
	org, _ := bindingValue(bindings, "org_id")
	switch {
	case strings.Contains(query, "FROM work_unit_investment_quotes"):
		return &reflectScanner{rows: [][]any{
			{"wu-1", "quote one", "issue", qtIssue1, "run-1"},
			{"wu-1", "quote two", "issue", qtIssue2, "run-1"},
			{"wu-1", "quote three", "issue", qtIssue3, "run-1"},
			{"wu-1", "quote pr", "pr", qtPRRef, "run-1"},
			{"wu-1", "quote commit", "commit", "abc123", "run-1"},
			{"wu-1", "quote pr blank", "pr", qtPRBlnk, "run-1"},
			{"wu-1", "quote pr uuid", "pr", qtPRUUID, "run-1"},
		}}, nil
	case strings.Contains(query, "FROM work_items"):
		if c.titleErr != nil {
			return nil, c.titleErr
		}
		ids, _ := bindingValue(bindings, "work_item_ids")
		var rows [][]any
		for _, id := range ids.([]string) {
			if title, ok := c.issueTitles[org.(string)][id]; ok {
				rows = append(rows, []any{id, title})
			}
		}
		return &reflectScanner{rows: rows}, nil
	case strings.Contains(query, "FROM git_pull_requests"):
		c.prBindings = bindings
		if c.prErr != nil {
			return nil, c.prErr
		}
		var rows [][]any
		for key, title := range c.prTitles[org.(string)] {
			parts := strings.SplitN(key, "#", 2)
			rows = append(rows, []any{parts[0], parts[1], title})
		}
		return &reflectScanner{rows: rows}, nil
	case strings.Contains(query, "FROM work_unit_investments"):
		return &reflectScanner{rows: [][]any{{
			"wu-1", "work_unit", "Unit", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
			nil, nil, "items_completed", 3.0,
			[]string{"feature_delivery"}, []float64{1}, []string{"feature_delivery.customer"}, []float64{1},
			"{}", 0.8, "high", "ok", "v1", "run-1", time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
		}}}, nil
	}
	return &reflectScanner{}, nil
}

func bindingValue(bindings []dhclickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func quoteEvidence(t *testing.T, store *quoteTitleStore, org string) map[string]map[string]any {
	t.Helper()
	reader, err := NewReader(store)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	units, err := reader.BuildWorkUnitInvestments(context.Background(), BuildWorkUnitInvestmentsOptions{
		OrgID: org, StartTS: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC),
		Limit: 10, IncludeText: true,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1", len(units))
	}
	out := map[string]map[string]any{}
	for _, entry := range units[0].Evidence.Textual {
		out[entry["id"].(string)] = entry
	}
	if len(out) != 7 {
		t.Fatalf("textual quotes = %d, want 7", len(out))
	}
	return out
}

func TestEvidenceQuotesServeTheSourceTitleAndNullWhenAbsent(t *testing.T) {
	store := &quoteTitleStore{
		issueTitles: map[string]map[string]string{qtOrgA: {
			qtIssue1: "  Fix login redirect ",
			qtIssue2: "   ",
			qtIssue3: qtUUID,
		}},
		prTitles: map[string]map[string]string{qtOrgA: {
			qtRepo + "#482": "Stream the CSV export",
			qtRepo + "#483": "  ",
			qtRepo + "#484": qtUUID,
		}},
	}
	quotes := quoteEvidence(t, store, qtOrgA)
	want := map[string]any{
		qtIssue1: "Fix login redirect",
		qtIssue2: nil,
		qtIssue3: nil,
		qtPRRef:  "Stream the CSV export",
		qtPRBlnk: nil,
		qtPRUUID: nil,
		"abc123": nil,
	}
	for id, entry := range quotes {
		title, present := entry["source_title"]
		if !present {
			t.Fatalf("%s: source_title key missing, want the key with a title or null", id)
		}
		if title != want[id] {
			t.Fatalf("%s: source_title = %#v, want %#v", id, title, want[id])
		}
		if title == id {
			t.Fatalf("%s: the id was served as the title", id)
		}
	}
}

func TestEvidenceQuotesNeverServeAnotherOrganisationsTitle(t *testing.T) {
	store := &quoteTitleStore{
		issueTitles: map[string]map[string]string{qtOrgB: {qtIssue1: "Other org issue"}},
		prTitles:    map[string]map[string]string{qtOrgB: {qtRepo + "#482": "Other org pull request"}},
	}
	for id, entry := range quoteEvidence(t, store, qtOrgA) {
		if entry["source_title"] != nil {
			t.Fatalf("%s: source_title = %#v, want null (only org-b stores titles)", id, entry["source_title"])
		}
	}
}

func TestQuoteTitleReadsAreBoundToTheCallersOrganisation(t *testing.T) {
	store := &quoteTitleStore{}
	quoteEvidence(t, store, qtOrgA)
	var sawIssue, sawPR bool
	for _, q := range store.queries {
		switch {
		case strings.Contains(q, "FROM work_items"):
			sawIssue = true
			if !strings.Contains(q, "org_id = {org_id:String}") {
				t.Fatalf("issue title read has no org predicate:\n%s", q)
			}
		case strings.Contains(q, "FROM git_pull_requests"):
			sawPR = true
			if !strings.Contains(q, "pr.org_id = {org_id:String}") ||
				!strings.Contains(q, "SELECT id FROM repos FINAL WHERE org_id = {org_id:String}") {
				t.Fatalf("pull request title read is not bound to the org on both the row and its repository:\n%s", q)
			}
		}
	}
	if !sawIssue || !sawPR {
		t.Fatalf("title reads issued: issue=%v pr=%v, want both", sawIssue, sawPR)
	}
	if org, _ := bindingValue(store.prBindings, "org_id"); org != qtOrgA {
		t.Fatalf("pull request title org_id binding = %v, want %s", org, qtOrgA)
	}
}

func TestEvidenceQuotesKeepRowsAndLogWhenTitleLookupsFail(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	store := &quoteTitleStore{titleErr: errors.New("issue title store down"), prErr: errors.New("pr title store down")}
	for id, entry := range quoteEvidence(t, store, qtOrgA) {
		if title, present := entry["source_title"]; !present || title != nil {
			t.Fatalf("%s: source_title = %#v present=%v, want explicit null", id, title, present)
		}
	}
	for _, want := range []string{"issue title store down", "pr title store down"} {
		if !strings.Contains(logged.String(), want) {
			t.Fatalf("lookup failure %q was not logged: %q", want, logged.String())
		}
	}
}

func TestEvidenceQuotesDropATitleEqualToTheirOwnID(t *testing.T) {
	store := &quoteTitleStore{
		issueTitles: map[string]map[string]string{qtOrgA: {
			qtIssue1: qtIssue1,
			qtIssue2: "Fix " + qtIssue2 + " crash",
			qtIssue3: "Fix login redirect",
		}},
		prTitles: map[string]map[string]string{qtOrgA: {
			qtRepo + "#482": " " + strings.ToUpper(qtPRRef) + " ",
			qtRepo + "#483": "Fix " + qtRepo + "#483",
		}},
	}
	quotes := quoteEvidence(t, store, qtOrgA)
	want := map[string]any{
		qtIssue1: nil,
		qtIssue2: "Fix " + qtIssue2 + " crash",
		qtIssue3: "Fix login redirect",
		qtPRRef:  nil,
		qtPRBlnk: "Fix " + qtRepo + "#483",
		qtPRUUID: nil,
		"abc123": nil,
	}
	for id, entry := range quotes {
		if entry["source_title"] != want[id] {
			t.Fatalf("%s: source_title = %#v, want %#v", id, entry["source_title"], want[id])
		}
	}
}
