package shadowvenue

import (
	"strings"
	"testing"
)

const stateRow = "34\tPOST\t/api/v1/admin/integrations/github/install-callback\tA\tno\tgithub\t-\tmatch\t/api/v1/admin/integrations/github/install-callback\t{\"installation_id\":1,\"state\":\"{state}\"}\n"

func statesFor(t *testing.T, row string) States {
	t.Helper()
	text := statesHeader + "\n"
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			text += row + "\t" + caller + "\t" + itoa(k) + "\tst-" + caller + "-" + itoa(k) + "\"q\n"
		}
	}
	s, err := ParseStates([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Each slot of a state row gets its own minted state, JSON-escaped, and no state is used twice.
func TestPlanFillsOneStatePerSlot(t *testing.T) {
	venue, frozen, _, _ := testPoolVenue(t)
	plan, err := MakePlan(mustRows(t, testRowsHeader+stateRow), venue, PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, States: statesFor(t, "34"), AllowExternal: []string{"github"}, K: 3})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, s := range plan.Slots {
		want := `{"installation_id":1,"state":"st-` + s.Caller + "-" + itoa(s.K) + `\"q"}`
		if s.Body != want || seen[s.Body] {
			t.Fatalf("slot %d body %s, want %s", s.Seq, s.Body, want)
		}
		seen[s.Body] = true
	}
	if len(seen) != 9 {
		t.Fatalf("%d distinct bodies, want 9", len(seen))
	}
}

// A state row with no state for a slot, or {state} in the path or outside a JSON string, refuses the plan, and no
// error names a state value.
func TestPlanRefusesAStateRowWithoutItsState(t *testing.T) {
	venue, frozen, _, _ := testPoolVenue(t)
	o := PlanOptions{Throwaway: "T", Pool: testPool, Frozen: frozen, AllowExternal: []string{"github"}, K: 3}
	if _, err := MakePlan(mustRows(t, testRowsHeader+stateRow), venue, o); err == nil || !strings.Contains(err.Error(), "-states") {
		t.Fatalf("no states file: %v", err)
	}
	o.States = statesFor(t, "99")
	if _, err := MakePlan(mustRows(t, testRowsHeader+stateRow), venue, o); err == nil || strings.Contains(err.Error(), "st-") {
		t.Fatalf("states of another row: %v", err)
	}
	o.States = statesFor(t, "34")
	for name, row := range map[string]string{
		"in the path":  "34\tPOST\t/api/v1/admin/integrations/{x}/install-callback\tA\tno\tgithub\t-\tmatch\t/api/v1/admin/integrations/{state}/install-callback\t{}\n",
		"not a string": "34\tPOST\t/api/v1/admin/integrations/github/install-callback\tA\tno\tgithub\t-\tmatch\t/api/v1/admin/integrations/github/install-callback\t{\"s\":\"x{state}\"}\n",
	} {
		if _, err := MakePlan(mustRows(t, testRowsHeader+row), venue, o); err == nil || strings.Contains(err.Error(), "st-") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The states file refuses a reused state (one value for two cells), a cell twice and a malformed line.
func TestParseStatesRefusesAReusedState(t *testing.T) {
	good := statesHeader + "\n34\tadmin\t1\tst-a\n34\tadmin\t2\tst-b\n"
	if _, err := ParseStates([]byte(good)); err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"reused state": good + "34\tsuperuser\t1\tst-a\n",
		"cell twice":   good + "34\tadmin\t1\tst-c\n",
		"bad caller":   good + "34\towner\t1\tst-d\n",
		"bad k":        good + "34\tadmin\t0\tst-e\n",
		"no state":     good + "34\tadmin\t3\t\n",
		"header":       strings.Replace(good, "state", "token", 1),
	} {
		if _, err := ParseStates([]byte(text)); err == nil || strings.Contains(err.Error(), "st-") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
