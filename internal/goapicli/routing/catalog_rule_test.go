package routing

// CHAOS-8517: what the verbs say now that query-api serves a catalog operation with no routing row.
// `status` must say such an operation is served without changing what `reachable` means (its JSON
// consumers read that key); `disable` and `seed` must say what they do and do not do to one.

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// TestStatusSaysAnOperationWithNoRowIsServed: served_without_row is true for exactly one state -- no row
// at any schema digest, the deployed plane registering the operation under the catalog's digest -- null
// when the deployed plane could not be asked, and false for every operation that has a row. `reachable`
// and its reason are what they were for each of them.
func TestStatusSaysAnOperationWithNoRowIsServed(t *testing.T) {
	agree := map[string]string{"hotspots": "catalog-digest"}
	status := func(state, mode string) goapiproof.OperationStatus {
		return goapiproof.OperationStatus{Operation: "hotspots", DocumentDigest: "catalog-digest", DigestState: state, Mode: mode}
	}
	for _, tc := range []struct {
		name          string
		status        goapiproof.OperationStatus
		deployed      map[string]string
		planeDown     bool
		want          *bool
		wantReachable *bool
	}{
		{"no row anywhere, deployed plane agrees", status(goapiproof.DigestMissing, ""), agree, false, boolPtr(true), boolPtr(false)},
		{"no row anywhere, deployed plane unreachable", status(goapiproof.DigestMissing, ""), nil, true, nil, boolPtr(false)},
		{"no row anywhere, deployed registry not read", status(goapiproof.DigestMissing, ""), nil, false, nil, boolPtr(false)},
		{"no row anywhere, deployed plane registers another document", status(goapiproof.DigestMissing, ""), map[string]string{"hotspots": "other"}, false, boolPtr(false), boolPtr(false)},
		{"no row anywhere, deployed plane does not register it", status(goapiproof.DigestMissing, ""), map[string]string{"home": "x"}, false, boolPtr(false), boolPtr(false)},
		{"rows only at another digest (STALE)", status(goapiproof.DigestStale, ""), agree, false, boolPtr(false), boolPtr(false)},
		{"rows only at this binary's digest (PENDING)", status(goapiproof.DigestPending, ""), agree, false, boolPtr(false), boolPtr(false)},
		{"live canary row", status(goapiproof.DigestMatch, "canary"), agree, false, boolPtr(false), boolPtr(true)},
		{"live primary row", status(goapiproof.DigestMatch, "primary"), agree, false, boolPtr(false), boolPtr(true)},
		{"live shadow row", status(goapiproof.DigestMatch, "shadow"), agree, false, boolPtr(false), boolPtr(false)},
		{"live python row", status(goapiproof.DigestMatch, "python"), agree, false, boolPtr(false), boolPtr(false)},
		{"live disabled row", status(goapiproof.DigestMatch, "disabled"), agree, false, boolPtr(false), boolPtr(false)},
		{"a live row of an operation the catalog does not register", status(goapiproof.DigestUnregistered, "canary"), agree, false, boolPtr(false), boolPtr(false)},
	} {
		got := toReportOperation(tc.status, tc.deployed, tc.planeDown, false)
		if !sameBool(got.ServedWithoutRow, tc.want) {
			t.Errorf("%s: served_without_row = %s, want %s", tc.name, showBool(got.ServedWithoutRow), showBool(tc.want))
		}
		if !sameBool(got.Reachable, tc.wantReachable) {
			t.Errorf("%s: reachable = %s, want %s: the key must keep its meaning (a routing ROW in a served mode)", tc.name, showBool(got.Reachable), showBool(tc.wantReachable))
		}
	}

	// The key is in the JSON, beside `reachable`, under the name consumers will read.
	encoded, err := json.Marshal(toReportOperation(status(goapiproof.DigestMissing, ""), agree, false, false))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["served_without_row"] != true || decoded["reachable"] != false || decoded["mode"] != nil {
		t.Errorf("JSON of an operation with no row: served_without_row=%v reachable=%v mode=%v, want true, false, null", decoded["served_without_row"], decoded["reachable"], decoded["mode"])
	}
}

func sameBool(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func showBool(value *bool) string {
	if value == nil {
		return "null"
	}
	if *value {
		return "true"
	}
	return "false"
}

// TestStatusTextSaysServedOnlyForAnOperationWithNoRow: the text prints "-" for the mode of every
// operation without a live row, so the line under it is what tells "served by the catalog rule" from
// "not served".
func TestStatusTextSaysServedOnlyForAnOperationWithNoRow(t *testing.T) {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	agree := map[string]string{"hotspots": "catalog-digest"}
	text := func(rows map[string]int, status goapiproof.OperationStatus, deployed map[string]string, planeDown bool) string {
		out, _, _ := captureStatusText(t, statusReport{
			LocalSchemaDigest: digest, GoPlaneSchemaDigest: stringPtr(digest), PlanesAgree: boolPtr(true), CatalogLoaded: true,
			RowsBySchemaDigest: rows,
			Operations:         []statusReportOperation{toReportOperation(status, deployed, planeDown, false)},
		}, digest)
		return out
	}
	missing := goapiproof.OperationStatus{Operation: "hotspots", DocumentDigest: "catalog-digest", DigestState: goapiproof.DigestMissing}

	out := text(map[string]int{}, missing, agree, false)
	if !strings.Contains(out, "no routing row at any schema digest: SERVED by the catalog rule") {
		t.Errorf("an operation with no row is not reported served:\n%s", out)
	}
	if !strings.Contains(out, "table is empty") || !strings.Contains(out, "serves every registered operation") || strings.Contains(out, "nothing is enabled") {
		t.Errorf("the empty-table line still reads as if nothing were served:\n%s", out)
	}

	if out := text(map[string]int{}, missing, nil, true); strings.Contains(out, "SERVED by the catalog rule") || !strings.Contains(out, "could not be asked whether it registers this one") {
		t.Errorf("with the deployed plane unreachable the text must not claim the operation is served:\n%s", out)
	}
	for name, status := range map[string]goapiproof.OperationStatus{
		"STALE":         {Operation: "hotspots", DocumentDigest: "catalog-digest", DigestState: goapiproof.DigestStale, StaleDigests: []string{"sha256:old"}},
		"live disabled": {Operation: "hotspots", DocumentDigest: "catalog-digest", DigestState: goapiproof.DigestMatch, Mode: "disabled"},
		"live canary":   {Operation: "hotspots", DocumentDigest: "catalog-digest", DigestState: goapiproof.DigestMatch, Mode: "canary"},
	} {
		if out := text(map[string]int{digest: 1}, status, agree, false); strings.Contains(out, "catalog rule") {
			t.Errorf("%s: an operation that has a row was reported under the catalog rule:\n%s", name, out)
		}
	}
	if out := text(map[string]int{}, missing, map[string]string{"hotspots": "other"}, false); strings.Contains(out, "SERVED by the catalog rule") {
		t.Errorf("an operation the deployed plane registers under another document was reported served:\n%s", out)
	}
	// An entry that is not MISSING never gets the line, whatever its served_without_row holds: a report
	// entry built without the field (null) must not read as "could not be asked".
	out, _, _ = captureStatusText(t, statusReport{
		LocalSchemaDigest: digest, GoPlaneSchemaDigest: stringPtr(digest), PlanesAgree: boolPtr(true), CatalogLoaded: true,
		RowsBySchemaDigest: map[string]int{digest: 1},
		Operations:         []statusReportOperation{{Operation: "hotspots", DigestState: goapiproof.DigestMatch, Mode: stringPtr("canary")}},
	}, digest)
	if strings.Contains(out, "catalog rule") {
		t.Errorf("a MATCH entry with no served_without_row answer was reported under the catalog rule:\n%s", out)
	}
}

// TestDisableSaysWhenItLeavesAnOperationServing: the plan line appears for a catalog operation with no
// row at any schema digest, and for nothing else. Each clause of the condition has its own row.
func TestDisableSaysWhenItLeavesAnOperationServing(t *testing.T) {
	noRow := goapiproof.DisableChange{Operation: "hotspots", NewMode: "disabled"}
	note := disableStillServedNote(noRow, false)
	for _, want := range []string{"SERVES this operation", "NOT held dark", "dho goapi routing seed -operations hotspots"} {
		if !strings.Contains(note, want) {
			t.Errorf("the note for an operation with no row lacks %q: %q", want, note)
		}
	}
	if !strings.HasPrefix(note, "    !! ") {
		t.Errorf("the note must be an indented `!!` plan line (the plan parsers read operation lines and NOTE lines only): %q", note)
	}

	withRow, deadOnly, staleOnly := noRow, noRow, noRow
	withRow.CurrentMode = "canary"
	deadOnly.DeadRowsOnly = true
	staleOnly.StaleSchemaDigests = []string{"sha256:old"}
	for name, got := range map[string]string{
		"an MCP class row with no row":                       disableStillServedNote(goapiproof.DisableChange{Operation: "mcp:hotspots", NewMode: "disabled"}, true),
		"an operation with a row at this digest":             disableStillServedNote(withRow, false),
		"an operation whose rows here serve other documents": disableStillServedNote(deadOnly, false),
		"an operation with a row at another schema digest":   disableStillServedNote(staleOnly, false),
	} {
		if got != "" {
			t.Errorf("%s: the note was printed (%q); that operation is not served by the catalog rule", name, got)
		}
	}
}

// TestSeedSaysWhenItHoldsACatalogOperationDark: the first row of a catalog operation takes it out of the
// catalog rule, and `seed` names every such operation -- written, or planned in a dry run or after a
// refusal. Not for MCP class rows, and not when no first row is involved.
func TestSeedSaysWhenItHoldsACatalogOperationDark(t *testing.T) {
	outcomes := func(actions ...string) []goapiproof.SeedOutcome {
		var out []goapiproof.SeedOutcome
		for index, action := range actions {
			out = append(out, goapiproof.SeedOutcome{Operation: []string{"home", "hotspots", "pr"}[index], Action: action})
		}
		return out
	}
	written := seedHoldsDarkNote(outcomes(goapiproof.SeedActionCreated, goapiproof.SeedActionAlreadyPresent, goapiproof.SeedActionCreated), false)
	if !strings.Contains(written, "2 catalog operation(s) had no routing row and were SERVED") || !strings.Contains(written, "NOT served until") || !strings.HasSuffix(written, ": home,pr") {
		t.Errorf("note after a write = %q", written)
	}
	planned := seedHoldsDarkNote(outcomes(goapiproof.SeedActionWouldCreate, goapiproof.SeedActionRefused), false)
	if !strings.Contains(planned, "1 catalog operation(s) have no routing row and are SERVED") || !strings.Contains(planned, "would hold each dark") || !strings.HasSuffix(planned, ": home") {
		t.Errorf("note for a planned write = %q", planned)
	}
	for name, got := range map[string]string{
		"MCP class rows":                    seedHoldsDarkNote(outcomes(goapiproof.SeedActionCreated), true),
		"nothing created":                   seedHoldsDarkNote(outcomes(goapiproof.SeedActionAlreadyPresent, goapiproof.SeedActionRefused), false),
		"no outcome":                        seedHoldsDarkNote(nil, false),
		"MCP class rows, planned (dry run)": seedHoldsDarkNote(outcomes(goapiproof.SeedActionWouldCreate), true),
	} {
		if got != "" {
			t.Errorf("%s: a note was printed: %q", name, got)
		}
	}
	for _, note := range []string{written, planned} {
		if strings.Contains(note, "go_api_routing.seeded") || strings.Contains(note, "recorded_by=") {
			t.Errorf("the note must not look like the seeded event line: %q", note)
		}
	}
}
