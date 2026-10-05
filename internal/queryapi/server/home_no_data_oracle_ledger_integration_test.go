//go:build integration

package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// chaos8169HomeNoDataLedgerTicket documents GWC ruling D4834. It keeps the
// a484 frozen Python response measured while declaring the approved Home
// empty-window difference without changing, skipping, or masking that oracle.
const chaos8169HomeNoDataLedgerTicket = "CHAOS-8169"

type chaos8169HomeNoDataLedgerEntry struct {
	Path   string
	Python string
	Go     string
	Leaves int
}

var chaos8169HomeNoDataLedger = []chaos8169HomeNoDataLedgerEntry{
	{Path: "/summary", Python: `[{"id":"s1","text":"Cycle Time held steady 0%.","evidence_link":"/api/v1/explain?metric=cycle_time&scope_type=org&scope_id=&range_days=7&compare_days=14"}]`, Go: `[]`, Leaves: 3},
	{Path: "/constraint", Python: `{"title":"This week's constraint: CI Success Rate","claim":"CI Success Rate held steady 0% over the last 7 days.","evidence":[{"label":"Drill into CI Success Rate","link":"/api/v1/explain?metric=ci_success&scope_type=org&scope_id=&range_days=7&compare_days=14"}],"experiments":["Rebalance reviewer rotation to reduce queueing.","Set WIP limits per team and auto-alert at saturation."]}`, Go: `{"title":"","claim":"","evidence":[],"experiments":[]}`, Leaves: 6},
	{Path: "/health_state", Python: `{"status":"watch","headline":"Cycle Time appears flat across org","summary":"The strongest signal suggests Cycle Time appears flat; longer cycle time suggests delivery work may spend more time waiting than moving.","as_of":null}`, Go: `{"status":"no_data","headline":"","summary":"","as_of":null}`, Leaves: 3},
	{Path: "/signals", Python: `[{"id":"metric:cycle_time","title":"Cycle Time appears flat","metric":"cycle_time","current_value":"0 days","prior_value":"0 days","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Cycle Time appears flat; longer cycle time suggests delivery work may spend more time waiting than moving.","recommended_action":"Inspect the slowest stage and rebalance active work before adding scope.","evidence_ref":"/api/v1/explain?metric=cycle_time&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"delivery","scope_entity":null},{"id":"metric:review_latency","title":"Review Latency appears flat","metric":"review_latency","current_value":"0 hours","prior_value":"0 hours","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Review Latency appears flat; review queues shape how quickly teams can learn from completed work.","recommended_action":"Rebalance reviewer rotation and clear stale review queues.","evidence_ref":"/api/v1/explain?metric=review_latency&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"dynamics","scope_entity":null},{"id":"metric:throughput","title":"Throughput appears flat","metric":"throughput","current_value":"0 items","prior_value":"0 items","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Throughput appears flat; throughput movement changes the team's ability to keep commitments credible.","recommended_action":"Review WIP and dependency queues before changing delivery commitments.","evidence_ref":"/api/v1/explain?metric=throughput&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"delivery","scope_entity":null},{"id":"metric:deploy_freq","title":"Deploy Frequency appears flat","metric":"deploy_freq","current_value":"0 deploys","prior_value":"0 deploys","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Deploy Frequency appears flat; deployment cadence suggests whether finished work can reach users smoothly.","recommended_action":"Check release blockers and restore the smallest safe deployment path.","evidence_ref":"/api/v1/explain?metric=deploy_freq&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"delivery","scope_entity":null},{"id":"metric:churn","title":"Code Churn appears flat","metric":"churn","current_value":"0 loc","prior_value":"0 loc","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Code Churn appears flat; higher churn suggests effort may be cycling through rework rather than durable progress.","recommended_action":"Inspect hotspots and stabilize rework loops before expanding the change set.","evidence_ref":"/api/v1/explain?metric=churn&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"durability","scope_entity":null},{"id":"metric:wip_saturation","title":"WIP Saturation appears flat","metric":"wip_saturation","current_value":"0 %","prior_value":"0 %","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"WIP Saturation appears flat; saturation suggests active work may exceed the team's coordination capacity.","recommended_action":"Set a short-term WIP limit and finish active items before starting more.","evidence_ref":"/api/v1/explain?metric=wip_saturation&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"dynamics","scope_entity":null},{"id":"metric:blocked_work","title":"Blocked Work appears flat","metric":"blocked_work","current_value":"0 hours","prior_value":"0 hours","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Blocked Work appears flat; blocked time suggests dependencies may be consuming delivery capacity.","recommended_action":"Triage blocked items by owner and unblock the oldest high-impact queue first.","evidence_ref":"/api/v1/explain?metric=blocked_work&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"delivery","scope_entity":null},{"id":"metric:change_failure_rate","title":"Change Failure Rate appears flat","metric":"change_failure_rate","current_value":"0 %","prior_value":"0 %","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Change Failure Rate appears flat; failed changes suggest reliability work may be competing with delivery.","recommended_action":"Inspect recent failed changes and tighten pre-release checks around the common failure mode.","evidence_ref":"/api/v1/explain?metric=change_failure_rate&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"durability","scope_entity":null},{"id":"metric:rework_ratio","title":"Rework Ratio appears flat","metric":"rework_ratio","current_value":"0 %","prior_value":"0 %","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"Rework Ratio appears flat; rework suggests unclear requirements or fragile implementation paths may be taxing focus.","recommended_action":"Review reopened or rewritten work and pick one root-cause experiment.","evidence_ref":"/api/v1/explain?metric=rework_ratio&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"durability","scope_entity":null},{"id":"metric:pr_rework_ratio","title":"PR Rework Ratio appears flat","metric":"pr_rework_ratio","current_value":"0 %","prior_value":"0 %","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"PR Rework Ratio appears flat; PR Rework Ratio movement suggests an operating signal to inspect.","recommended_action":"Inspect supporting evidence and choose one reversible operating experiment.","evidence_ref":"/api/v1/explain?metric=pr_rework_ratio&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"durability","scope_entity":null},{"id":"metric:ci_success","title":"CI Success Rate appears flat","metric":"ci_success","current_value":"0 %","prior_value":"0 %","delta":"+0%","direction":"flat","severity":"low","confidence":"low","affected_scope":"org","evidence_count":0,"why_it_matters":"CI Success Rate appears flat; CI health suggests whether the delivery path is dependable.","recommended_action":"Inspect failing pipelines and restore the most common broken check first.","evidence_ref":"/api/v1/explain?metric=ci_success&scope_type=org&scope_id=&range_days=7&compare_days=14","category":"durability","scope_entity":null}]`, Go: `[]`, Leaves: 176},
	{Path: "/limiting_factor", Python: `{"claim":"Cycle Time appears flat appears to be the current limiting factor.","why_it_matters":"Cycle Time appears flat; longer cycle time suggests delivery work may spend more time waiting than moving.","recommended_action":"Inspect the slowest stage and rebalance active work before adding scope.","confidence":"low","evidence_ref":"/api/v1/explain?metric=cycle_time&scope_type=org&scope_id=&range_days=7&compare_days=14"}`, Go: `{"claim":"","why_it_matters":"","recommended_action":"","confidence":"low","evidence_ref":null}`, Leaves: 4},
}

func chaos8169HomeObject(t *testing.T, body string) *pyjson.Object {
	t.Helper()
	value, err := pyjson.Decode([]byte(body))
	if err != nil {
		t.Fatalf("%s ledger: decode response body: %v", chaos8169HomeNoDataLedgerTicket, err)
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		t.Fatalf("%s ledger: response body is %T, want JSON object", chaos8169HomeNoDataLedgerTicket, value)
	}
	return object
}

func chaos8169HomeValue(t *testing.T, object *pyjson.Object, path string) pyjson.Value {
	t.Helper()
	key := strings.TrimPrefix(path, "/")
	if key == "" || strings.Contains(key, "/") {
		t.Fatalf("%s ledger has unsupported path %q", chaos8169HomeNoDataLedgerTicket, path)
	}
	value, ok := object.Get(key)
	if !ok {
		t.Fatalf("%s ledger path %s is absent", chaos8169HomeNoDataLedgerTicket, path)
	}
	return value
}

func chaos8169JSONText(t *testing.T, value pyjson.Value) string {
	t.Helper()
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		t.Fatalf("%s ledger: marshal JSON value: %v", chaos8169HomeNoDataLedgerTicket, err)
	}
	return string(encoded)
}

func chaos8169HomeLeafPaths(value pyjson.Value, path string) []string {
	switch typed := value.(type) {
	case *pyjson.Object:
		keys := typed.Keys()
		if len(keys) == 0 {
			return []string{path}
		}
		var out []string
		for _, key := range keys {
			item, _ := typed.Get(key)
			out = append(out, chaos8169HomeLeafPaths(item, path+"/"+key)...)
		}
		return out
	case []pyjson.Value:
		if len(typed) == 0 {
			return []string{path}
		}
		var out []string
		for index, item := range typed {
			out = append(out, chaos8169HomeLeafPaths(item, fmt.Sprintf("%s/%d", path, index))...)
		}
		return out
	default:
		return []string{path}
	}
}

func chaos8169HomeObjectKeys(left, right *pyjson.Object) []string {
	seen := make(map[string]bool, left.Len()+right.Len())
	keys := make([]string, 0, left.Len()+right.Len())
	for _, object := range []*pyjson.Object{left, right} {
		for _, key := range object.Keys() {
			if seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}

// chaos8169HomeDifferenceLeaves reports every differing scalar or unmatched
// JSON leaf. It does not mutate either body or make their values equal.
func chaos8169HomeDifferenceLeaves(left pyjson.Value, leftOK bool, right pyjson.Value, rightOK bool, path string) []string {
	switch {
	case !leftOK && !rightOK:
		return nil
	case !leftOK:
		return chaos8169HomeLeafPaths(right, path)
	case !rightOK:
		return chaos8169HomeLeafPaths(left, path)
	}

	leftObject, leftIsObject := left.(*pyjson.Object)
	rightObject, rightIsObject := right.(*pyjson.Object)
	if leftIsObject || rightIsObject {
		if !leftIsObject || !rightIsObject {
			return []string{path}
		}
		var out []string
		for _, key := range chaos8169HomeObjectKeys(leftObject, rightObject) {
			leftValue, leftExists := leftObject.Get(key)
			rightValue, rightExists := rightObject.Get(key)
			out = append(out, chaos8169HomeDifferenceLeaves(leftValue, leftExists, rightValue, rightExists, path+"/"+key)...)
		}
		return out
	}

	leftList, leftIsList := left.([]pyjson.Value)
	rightList, rightIsList := right.([]pyjson.Value)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return []string{path}
		}
		var out []string
		maxLen := len(leftList)
		if len(rightList) > maxLen {
			maxLen = len(rightList)
		}
		for index := 0; index < maxLen; index++ {
			var leftValue, rightValue pyjson.Value
			leftExists, rightExists := index < len(leftList), index < len(rightList)
			if leftExists {
				leftValue = leftList[index]
			}
			if rightExists {
				rightValue = rightList[index]
			}
			out = append(out, chaos8169HomeDifferenceLeaves(leftValue, leftExists, rightValue, rightExists, fmt.Sprintf("%s/%d", path, index))...)
		}
		return out
	}

	if chaos8169JSONTextNoFail(left) != chaos8169JSONTextNoFail(right) {
		return []string{path}
	}
	return nil
}

func chaos8169JSONTextNoFail(value pyjson.Value) string {
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func chaos8169HomeArrayLength(t *testing.T, object *pyjson.Object, path string, want int) {
	t.Helper()
	value := chaos8169HomeValue(t, object, path)
	items, ok := value.([]pyjson.Value)
	if !ok {
		t.Fatalf("%s %s is %T, want JSON array", chaos8169HomeNoDataLedgerTicket, path, value)
	}
	if len(items) != want {
		t.Fatalf("%s %s length = %d, want %d", chaos8169HomeNoDataLedgerTicket, path, len(items), want)
	}
}

func chaos8169HomeObjectLength(t *testing.T, object *pyjson.Object, path string, want int) {
	t.Helper()
	value := chaos8169HomeValue(t, object, path)
	item, ok := value.(*pyjson.Object)
	if !ok {
		t.Fatalf("%s %s is %T, want JSON object", chaos8169HomeNoDataLedgerTicket, path, value)
	}
	if item.Len() != want {
		t.Fatalf("%s %s length = %d, want %d", chaos8169HomeNoDataLedgerTicket, path, item.Len(), want)
	}
}

// assertCHAOS8169HomeNoDataLedger verifies the actual a484 and Go responses.
// It accepts only D4834's five exact values, records 192 differing leaves, and
// makes all 11 REST deltas, four tiles, and every other JSON leaf strict.
func assertCHAOS8169HomeNoDataLedger(t *testing.T, pythonBody, goBody string) {
	t.Helper()
	python := chaos8169HomeObject(t, pythonBody)
	goResponse := chaos8169HomeObject(t, goBody)

	for _, entry := range chaos8169HomeNoDataLedger {
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, python, entry.Path)); got != entry.Python {
			t.Errorf("%s ledger Python %s = %s, want %s", chaos8169HomeNoDataLedgerTicket, entry.Path, got, entry.Python)
		}
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, goResponse, entry.Path)); got != entry.Go {
			t.Errorf("%s ledger Go %s = %s, want %s", chaos8169HomeNoDataLedgerTicket, entry.Path, got, entry.Go)
		}
	}

	for _, path := range []string{"/deltas", "/tiles"} {
		if got, want := chaos8169JSONText(t, chaos8169HomeValue(t, goResponse, path)), chaos8169JSONText(t, chaos8169HomeValue(t, python, path)); got != want {
			t.Errorf("%s %s changed outside the ledger: Go %s, Python %s", chaos8169HomeNoDataLedgerTicket, path, got, want)
		}
	}
	chaos8169HomeArrayLength(t, python, "/deltas", 11)
	chaos8169HomeArrayLength(t, goResponse, "/deltas", 11)
	chaos8169HomeObjectLength(t, python, "/tiles", 4)
	chaos8169HomeObjectLength(t, goResponse, "/tiles", 4)

	entries := make(map[string]chaos8169HomeNoDataLedgerEntry, len(chaos8169HomeNoDataLedger))
	wantLeaves := 0
	for _, entry := range chaos8169HomeNoDataLedger {
		entries[entry.Path] = entry
		wantLeaves += entry.Leaves
	}
	counts := make(map[string]int, len(entries))
	differences := chaos8169HomeDifferenceLeaves(python, true, goResponse, true, "")
	for _, path := range differences {
		trimmed := strings.TrimPrefix(path, "/")
		root := "/"
		if trimmed != "" {
			root = "/" + strings.Split(trimmed, "/")[0]
		}
		if _, ok := entries[root]; !ok {
			t.Errorf("%s unledgered difference at %s", chaos8169HomeNoDataLedgerTicket, path)
			continue
		}
		counts[root]++
	}
	if len(differences) != wantLeaves {
		t.Errorf("%s difference leaf count = %d, want %d", chaos8169HomeNoDataLedgerTicket, len(differences), wantLeaves)
	}
	for _, entry := range chaos8169HomeNoDataLedger {
		if got := counts[entry.Path]; got != entry.Leaves {
			t.Errorf("%s difference leaves at %s = %d, want %d", chaos8169HomeNoDataLedgerTicket, entry.Path, got, entry.Leaves)
		}
	}
}
