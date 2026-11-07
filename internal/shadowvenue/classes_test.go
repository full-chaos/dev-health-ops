package shadowvenue

import (
	"strings"
	"testing"
	"time"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// reclassified is the embedded table with one row's ext or destructive column changed.
func reclassified(t *testing.T, id string, ext, destructive string) []Row {
	t.Helper()
	var out []string
	for _, line := range strings.Split(string(DefaultRows), "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 10 && f[0] == id {
			if ext != "" {
				f[5] = ext
			}
			if destructive != "" {
				f[4] = destructive
				if destructive == "no" {
					f[3] = "A"
				}
			}
			line = strings.Join(f, "\t")
		}
		out = append(out, line)
	}
	return mustRows(t, strings.Join(out, "\n"))
}

// A custom row table can change a row's path or body, never its class: an external row stays external with its own
// service and a destructive row stays destructive (the class is the embedded table's, by route key).
func TestCustomRowTableCannotReclassifyARow(t *testing.T) {
	var external, destructive []string
	for _, row := range mustRows(t, string(DefaultRows)) {
		switch {
		case row.Control():
		case row.Destructive:
			destructive = append(destructive, row.ID)
		case row.Ext != none:
			external = append(external, row.ID)
		}
	}
	if len(external) != 17 || len(destructive) != 5 {
		t.Fatalf("external %v destructive %v", external, destructive)
	}
	for _, id := range external {
		if err := CheckRows(reclassified(t, id, none, ""), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
			t.Errorf("row %s with ext - accepted: %v", id, err)
		}
		if err := CheckRows(reclassified(t, id, "mail", ""), authv1.GoAPIEndpointProfiles); id != "1" && (err == nil || !strings.Contains(err.Error(), "class")) {
			t.Errorf("row %s with another service accepted: %v", id, err)
		}
	}
	for _, id := range destructive {
		if err := CheckRows(reclassified(t, id, "", "no"), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
			t.Errorf("destructive row %s made plain accepted: %v", id, err)
		}
	}
	if err := CheckRows(reclassified(t, "58", "stripe", ""), authv1.GoAPIEndpointProfiles); err == nil || !strings.Contains(err.Error(), "class") {
		t.Errorf("a plain row made external accepted: %v", err)
	}
}

// The judge takes each sent slot's class from its route, not from the row table it is given: a table that names an
// external route as internal voids the run, even with no stub log.
func TestJudgeVoidsARowTableThatReclassifiesARoute(t *testing.T) {
	slots, lines := controlsOK()
	seq := len(slots) + 1
	for _, caller := range callerOrder {
		for k := 1; k <= 3; k++ {
			s := slotAt(seq, "22", caller, k)
			s.Method, s.Route, s.Path = "POST", "/api/v1/billing/plans/pull-stripe", "/api/v1/billing/plans/pull-stripe"
			slots = append(slots, s)
			lines = append(lines, result(s, 500*time.Millisecond, "match", "dec_"+strings.Repeat(string(rune('a'+seq)), 24)))
			seq++
		}
	}
	ev, err := ParseLog(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := Judge(slots, ev, nil, JudgeOptions{K: 3, Grace: 2200 * time.Millisecond, Outbound: Outbound{Ext: map[string]string{"C1": none, "C2": none, "22": none}}})
	if !r.Void || !strings.Contains(strings.Join(r.Reasons, "\n"), "class") {
		t.Fatalf("void %v pass %v: %v", r.Void, r.Pass, r.Reasons)
	}
}

// A key given twice is refused even when its first value is blank, and an empty value is refused as it is parsed, for
// every key.
func TestParseEgressRefusesABlankFirstDuplicateOfEveryKey(t *testing.T) {
	good := egressText("before")
	for _, key := range egressKeys {
		read := ParseEgress("before.txt", []byte(key+"=\n"+good))
		if read.Err == nil {
			t.Errorf("%s: a blank first value then a valid one was accepted", key)
		}
		if voids := checkEgress([]EgressRead{read, egressReads("between")[0]}, []string{"before", "between"}); len(voids) == 0 {
			t.Errorf("%s: the run is not void", key)
		}
	}
}
