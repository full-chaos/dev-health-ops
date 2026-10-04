package goapiproof

import "testing"

// CHAOS-8649: the census reports the accepted row that decides the switch's answer. The switch serves when
// ANY accepted row is in canary or primary, so that row wins wherever it sits; with none served, the row
// under the current text speaks for the operation.
func TestDecidingRowIsTheRowTheSwitchActsOn(t *testing.T) {
	accepted := []string{"current", "legacy1", "legacy2"}
	row := func(digest, mode string) routingStateRow {
		return routingStateRow{documentDigest: digest, mode: mode}
	}
	cases := []struct {
		name       string
		rows       []routingStateRow
		wantDigest string
	}{
		{"a served legacy row over an off current one", []routingStateRow{row("current", "disabled"), row("legacy1", "canary")}, "legacy1"},
		{"a served current row over a served legacy one", []routingStateRow{row("legacy1", "primary"), row("current", "canary")}, "current"},
		{"the first served row in accepted order", []routingStateRow{row("legacy2", "canary"), row("legacy1", "primary"), row("current", "shadow")}, "legacy1"},
		{"none served: the current row", []routingStateRow{row("legacy1", "shadow"), row("current", "disabled")}, "current"},
		{"none served, no current row: the first legacy row", []routingStateRow{row("legacy2", "python"), row("legacy1", "shadow")}, "legacy1"},
	}
	for _, c := range cases {
		if got := decidingRow(c.rows, accepted); got.documentDigest != c.wantDigest {
			t.Errorf("%s: decidingRow = %s (%s), want %s", c.name, got.documentDigest, got.mode, c.wantDigest)
		}
	}
}
