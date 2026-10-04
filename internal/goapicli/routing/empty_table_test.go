package routing

// CHAOS-8543: the machine-readable line and the operator line of the no-op `carry` and `repoint`
// answer an empty routing table with. The decision itself needs a database and is pinned by
// empty_table_integration_test.go; what is pinned here is the shape the callers parse.

import (
	"strings"
	"testing"
)

// The chart hooks and bigboy-cut.sh read `reason` as the FIRST key of the line (sed over
// `GOAPI_ROUTING_JSON {"reason":"...`; the tools image has no jq), and they treat exit 0 with the
// verb's own success reason as success. So the no-op keeps that reason, keeps it first, and marks
// itself with a field of its own that is absent from every other line.
func TestTheEmptyTableNoOpKeepsTheSuccessReasonFirstAndMarksItself(t *testing.T) {
	render := func(print func(*strings.Builder)) string {
		var out strings.Builder
		print(&out)
		return out.String()
	}
	for name, tc := range map[string]struct {
		noOp, plain, wantPrefix string
	}{
		"carry": {
			noOp: render(func(w *strings.Builder) {
				printCarryResult(w, carryResult{Reason: "carried", LiveDigest: "sha256:live", TargetDigest: "sha256:target", EmptyTable: true})
			}),
			plain: render(func(w *strings.Builder) {
				printCarryResult(w, carryResult{Reason: "carried", LiveDigest: "sha256:live", TargetDigest: "sha256:target", Carried: 2})
			}),
			wantPrefix: `GOAPI_ROUTING_JSON {"reason":"carried",`,
		},
		"repoint": {
			noOp:       render(func(w *strings.Builder) { printRepointResult(w, repointResult{Reason: "repointed", EmptyTable: true}) }),
			plain:      render(func(w *strings.Builder) { printRepointResult(w, repointResult{Reason: "repointed"}) }),
			wantPrefix: `GOAPI_ROUTING_JSON {"reason":"repointed"`,
		},
	} {
		if !strings.HasPrefix(tc.noOp, tc.wantPrefix) || !strings.HasSuffix(tc.noOp, `"empty_table":true}`+"\n") {
			t.Errorf("%s no-op line = %q, want it to start %q and end with the empty_table mark", name, tc.noOp, tc.wantPrefix)
		}
		if !strings.HasPrefix(tc.plain, tc.wantPrefix) || strings.Contains(tc.plain, "empty_table") {
			t.Errorf("%s line of a run over rows = %q, want the same reason first and no empty_table key", name, tc.plain)
		}
		if strings.Count(tc.noOp, "\n") != 1 {
			t.Errorf("%s no-op line is not one line: %q", name, tc.noOp)
		}
	}
}

// One line, naming what was not done, that nothing was written, and why the state is not an error.
func TestTheEmptyTableNoteSaysWhatWasNotDoneAndWhyThatIsNotAnError(t *testing.T) {
	for _, what := range []string{"carry", "re-point"} {
		note := routingTableEmptyNote(what)
		for _, want := range []string{
			"go-api-routing: NO-OP: go_api_routing_state has no row at any schema digest, so there is nothing to " + what + " and nothing was written.",
			"An empty table is a valid state",
			"serves every registered operation that has no routing row",
			"no MCP class root is enabled",
		} {
			if !strings.Contains(note, want) {
				t.Errorf("the %s note lacks %q: %q", what, want, note)
			}
		}
		if strings.Contains(note, "\n") || strings.Contains(strings.ToLower(note), "refus") {
			t.Errorf("the %s note must be one line and must not read as a refusal: %q", what, note)
		}
	}
}
