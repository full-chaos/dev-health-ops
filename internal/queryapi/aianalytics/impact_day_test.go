package aianalytics

import (
	"context"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"encoding/json"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-7774: every aiImpactSummary daily row carries the calendar day it was
// rolled up for. The Python reference never exposed it, so the oracle strips
// exactly that field (goOnlyImpactDailyRowFields) and these tests pin it.

func impactCase(t *testing.T, name string) (oracleCase, dataset) {
	t.Helper()
	datasets, cases := loadOracle(t)
	for _, c := range cases {
		if c.Name == name {
			return c, datasets[c.Dataset]
		}
	}
	t.Fatalf("no oracle case %q", name)
	return oracleCase{}, dataset{}
}

func impactDaily(t *testing.T, name string) (*model.AIImpactSummary, dataset) {
	t.Helper()
	c, ds := impactCase(t, name)
	client := &fixtureClient{c: c, ds: ds}
	got, err := ImpactSummary(context.Background(), client, "org-1", oracleRange, scopeInput(c))
	if err != nil {
		t.Fatal(err)
	}
	return got, ds
}

// pairs is the sorted "bucket@day" list of a set of rows.
func impactPairs(t *testing.T, rows []model.AIImpactBucketRow) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, strings.ToLower(r.Bucket)+"@"+r.Day.String())
	}
	sort.Strings(out)
	return out
}

func TestImpactSummary_EveryDailyRowCarriesItsOwnDay(t *testing.T) {
	got, ds := impactDaily(t, "impact_summary/mixed")
	var want []string
	for _, r := range ds.Daily {
		want = append(want, strings.ToLower(r["attribution_bucket"].(string))+"@"+r["day"].(string))
	}
	sort.Strings(want)
	if len(want) != 8 {
		t.Fatalf("fixture has %d rows, want 8", len(want))
	}
	if g := impactPairs(t, got.Daily); !reflect.DeepEqual(g, want) {
		t.Fatalf("daily bucket@day = %v, want %v", g, want)
	}
}

// One date appears on several rows (one per repository, team and bucket); the
// rows are not collapsed to one per day.
func TestImpactSummary_SameDayRowsKeepTheSameDayAndStaySeparate(t *testing.T) {
	got, _ := impactDaily(t, "impact_summary/mixed")
	perDay := map[string]int{}
	for _, r := range got.Daily {
		perDay[r.Day.String()]++
	}
	want := map[string]int{"2026-08-01": 2, "2026-08-02": 3, "2026-08-03": 3}
	if !reflect.DeepEqual(perDay, want) {
		t.Fatalf("rows per day = %v, want %v", perDay, want)
	}
}

// The window is not the day: a row outside the first day of the window must not
// read as the window start.
func TestImpactSummary_DayIsNotTheWindowStartOrTheComputedDate(t *testing.T) {
	got, _ := impactDaily(t, "impact_summary/mixed")
	distinct := map[string]bool{}
	for _, r := range got.Daily {
		distinct[r.Day.String()] = true
	}
	if len(distinct) != 3 {
		t.Fatalf("distinct days = %v, want the three fixture days", distinct)
	}
}

// The oracle strips exactly the one Go-only daily field. A second stripped field
// would let a real divergence from the Python reference pass.
func TestOracleStripsOnlyDayFromImpactDaily(t *testing.T) {
	if !reflect.DeepEqual(goOnlyImpactDailyRowFields, []string{"day"}) {
		t.Fatalf("goOnlyImpactDailyRowFields = %v, want exactly [day]", goOnlyImpactDailyRowFields)
	}
	raw, err := os.ReadFile("testdata/oracle_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		rows, _ := c.Expected["daily"].([]any)
		for _, r := range rows {
			m, _ := r.(map[string]any)
			if _, ok := m["day"]; ok && c.Fn == "resolve_ai_impact_summary" {
				t.Fatalf("case %s: the recorded Python answer has daily[].day; it is not Go-only", c.Name)
			}
		}
	}
	m := map[string]any{"daily": []any{map[string]any{"day": "x", "bucket": "b", "prsTotal": 1}}}
	stripGoOnlyImpactDailyRowFields(m)
	row := m["daily"].([]any)[0].(map[string]any)
	if _, ok := row["day"]; ok {
		t.Fatal("day not stripped")
	}
	if row["bucket"] != "b" || row["prsTotal"] != 1 {
		t.Fatalf("another field was stripped: %v", row)
	}
}
