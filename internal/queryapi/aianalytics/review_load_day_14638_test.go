package aianalytics

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// CHAOS-14638: every aiReviewLoad daily row carries the calendar day it was rolled up for; the byBucket rows do not.
func TestReviewLoad_DailyRowsCarryTheirDayAndByBucketRowsDoNot(t *testing.T) {
	c, ds := impactCase(t, "review_load/mixed")
	got, err := ReviewLoad(context.Background(), &fixtureClient{c: c, ds: ds}, "org-1", oracleRange, scopeInput(c))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Daily    []map[string]any `json:"daily"`
		ByBucket []map[string]any `json:"byBucket"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var want, have []string
	for _, r := range ds.Daily {
		want = append(want, strings.ToLower(r["attribution_bucket"].(string))+"@"+r["day"].(string))
	}
	for _, r := range m.Daily {
		d, _ := r["day"].(string)
		have = append(have, strings.ToLower(r["bucket"].(string))+"@"+d)
	}
	sort.Strings(want)
	sort.Strings(have)
	if len(want) != 8 {
		t.Fatalf("fixture has %d daily rows, want 8", len(want))
	}
	if !reflect.DeepEqual(have, want) {
		t.Fatalf("daily bucket@day = %v, want %v", have, want)
	}
	if len(m.ByBucket) == 0 {
		t.Fatal("no byBucket rows to check")
	}
	for _, r := range m.ByBucket {
		if _, ok := r["day"]; ok {
			t.Fatalf("byBucket row %v carries a day", r["bucket"])
		}
	}
}
