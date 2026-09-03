package pythonparity

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// payloadGuard names one shipped fixture and the payload fields its rot guard
// compares. It is the SINGLE SOURCE for both.
//
// Both facts used to be written twice: as string literals at the guard's
// comparePayload call site, and again in TestShippedFixturesExposeThePayload-
// FieldsTheGuardsCompare's table. That test asserts each fixture carries the
// fields its guard names -- but it read its own copy of the list, so a guard
// that changed which fields it compares would leave the test asserting the OLD
// ones: green, while describing a comparison that no longer happened. That is
// the failure mode the test was written to prevent, one level up.
//
// With one list the drift is not detected, it is impossible: the guard and the
// test cannot disagree about which fields matter because they read the same
// variable. TestEveryRotGuardUsesTheRegistry fails a guard that passes literals
// instead, so the registry cannot be bypassed by a future call site.
type payloadGuard struct {
	fixture string
	fields  []string
}

var (
	reprBandGuard = payloadGuard{
		fixture: "evidence_json_repr_band_python_golden.json",
		// distinct_input_values is payload, not provenance: it is derived from
		// the corpus, so a change means the corpus changed.
		fields: []string{"cases", "distinct_input_values"},
	}
	edgeShapesGuard = payloadGuard{
		fixture: "evidence_json_edge_shapes_python_golden.json",
		fields:  []string{"cases"},
	}
	insertionOrderGuard = payloadGuard{
		fixture: "python_json_insertion_order_python_golden.json",
		fields:  []string{"cases"},
	}

	// allPayloadGuards is what the shipped-fixture test iterates. A guard absent
	// from this slice is not covered by it -- which is why
	// TestEveryRotGuardUsesTheRegistry checks the call sites rather than trusting
	// this list to be complete.
	allPayloadGuards = []payloadGuard{reprBandGuard, edgeShapesGuard, insertionOrderGuard}
)

// comparePayload reports whether the frozen and freshly-rendered documents agree
// on the fields that carry DATA, ignoring everything else in the document.
//
// # WHY THE GUARDS NO LONGER COMPARE THE WHOLE FILE
//
// They used to, and it produced a false alarm with a confidently wrong
// diagnosis. A fixture froze `sys.version`, which embeds the build string, so a
// macOS-generated document could never match a Linux runner: the guard reported
// "has ROTTED", pointed the reader at recommendations/loader.py, and nothing had
// rotted. Recording the bare version instead narrowed the window without closing
// it, because the live interpreter is UNPINNED -- parityLivePython takes
// $PYTHON, else whatever `python3` resolves to -- so the next CPython patch
// reproduces the same incident on CPython's release schedule.
//
// Removing the metadata fixed the instance. Comparing the payload fixes the
// CLASS: a future field added to a document for provenance cannot re-create the
// tripwire, because nothing outside the named payload is looked at.
//
// The line worth keeping, from lane-ci-flakes: a frozen environment value is a
// DEFECT when the environment can drift without a decision, and a FEATURE when
// it cannot. This package holds one of each. `python_version` drifts on
// CPython's schedule with nobody deciding anything, so it lives outside the
// comparison. `clickhouse_connect_version_measured` in the ClickHouse decoder
// golden can only change when someone deliberately bumps a pinned requirement,
// and at that moment a decoder golden SHOULD demand re-verification -- so that
// one stays inside its whole-document comparison, deliberately.
//
// # WHY RawMessage RATHER THAN DECODE-AND-COMPARE
//
// The payload is compared as RAW BYTES, not as re-serialised values. Decoding
// into Go types and comparing those would silently forgive a formatting change,
// a duplicated key, or a number that round-trips differently -- and this is a
// BYTE-parity package, where those are exactly the differences that matter.
// json.RawMessage keeps the original bytes of each field, so the comparison
// stays as strict as the old whole-file one everywhere it still applies.

// interpreterOf renders the recorded interpreter for ATTRIBUTION.
//
// Adopted from floattext_rot_guard_test.go, which had this pattern before I
// wrote mine and which lane-ci-flakes pointed me at. Excluding provenance from
// the comparison is only half of it; the other half is USING it, so a failure
// can say which side moved.
//
// That half is exactly what the original incident lacked. The guard reported
// "has ROTTED" and sent the reader to recommendations/loader.py while the real
// difference was a macOS build string against a Linux one. It had the
// interpreter in hand and did not print it.
//
// An ABSENT interpreter block is fatal, not tolerated. A fixture that does not
// record what produced it cannot be attributed -- and making the field
// mandatory is what stops someone deleting it again, which is what I did in the
// commit before this one.
func interpreterOf(document []byte, label string) (string, error) {
	var parsed struct {
		Environment struct {
			PythonVersion  string `json:"python_version"`
			FloatReprStyle string `json:"float_repr_style"`
		} `json:"environment"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		return "", fmt.Errorf("decode %s document: %w", label, err)
	}
	if parsed.Environment.PythonVersion == "" {
		return "", fmt.Errorf(
			"%s document records no interpreter; a fixture that does not record "+
				"what produced it cannot be attributed, and the provenance must "+
				"not be deleted merely because it is not compared", label)
	}
	return fmt.Sprintf("CPython %s (float_repr_style %s)",
		parsed.Environment.PythonVersion, parsed.Environment.FloatReprStyle), nil
}

// comparePayload takes the registry ENTRY, not a name and a list of fields.
//
// It used to be `(frozen, rendered []byte, fields ...string)`. codex round 1 on
// CHAOS-4914 showed why that was not enough: the AST guard below only rejected
// a direct string LITERAL, so a call site could hold the same literal in a
// variable and bypass it. The round proved it was not theoretical -- it mutated
// a guard to compare an indirect "cases", DROPPING distinct_input_values, and
// every check stayed green.
//
// Taking the entry removes the argument that could carry a name at all. A guard
// cannot express "compare these fields" any more; it can only name which
// registry entry it is. That is a constraint on the SHAPE of the call rather
// than a search for one bad shape inside it.
// isRegisteredGuard reports whether *guard* IS one of the registry entries.
//
// codex round 2 defeated the previous defence, which was an AST rule requiring a
// bare identifier at the call site. payloadGuard is mutable inside this package,
// so a guard could write
//
//	g := reprBandGuard
//	g.fields = []string{"cases"}
//	comparePayload(frozen, rendered, g)
//
// -- a bare identifier, passing the syntactic check, silently dropping
// distinct_input_values. The round did exactly that and every test stayed green.
//
// That was my third syntactic proxy in a row: "not a string literal", then "is a
// bare identifier", each called a property check and each defeated by the next
// shape. This asks the actual question instead, at runtime, where syntax cannot
// help: is this value one of the registered entries. A copy with edited fields
// is not, however it was spelled at the call site.
func isRegisteredGuard(guard payloadGuard) bool {
	for _, registered := range allPayloadGuards {
		if registered.fixture != guard.fixture ||
			len(registered.fields) != len(guard.fields) {
			continue
		}
		same := true
		for index, field := range registered.fields {
			if guard.fields[index] != field {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}

// comparePayload compares the payload of a SHIPPED fixture, and refuses any
// guard value that is not in the registry.
//
// The synthetic cases in goldenpayload_behaviour_test.go exercise the comparison
// logic itself against hand-written documents, so they call comparePayloadFields
// directly. A rot guard may not: TestEveryRotGuardUsesTheRegistry fails a
// *_rot_guard_test.go that mentions comparePayloadFields at all, which keeps the
// unvalidated door open for the tests that need it and shut for the ones that
// must not have it.
func comparePayload(frozen, rendered []byte, guard payloadGuard) error {
	if !isRegisteredGuard(guard) {
		return fmt.Errorf(
			"comparePayload was given a guard for %q that is NOT in allPayloadGuards "+
				"(fields %v). A guard must compare exactly what the registry says, or "+
				"it and TestShippedFixturesExposeThePayloadFieldsTheGuardsCompare can "+
				"disagree -- which is the drift the registry exists to make impossible. "+
				"A copy with edited fields is not a registry entry, however it is spelled",
			guard.fixture, guard.fields)
	}
	return comparePayloadFields(frozen, rendered, guard.fields)
}

func comparePayloadFields(frozen, rendered []byte, fields []string) error {
	var frozenDoc, renderedDoc map[string]json.RawMessage
	if err := json.Unmarshal(frozen, &frozenDoc); err != nil {
		return fmt.Errorf("parse frozen document: %w", err)
	}
	if err := json.Unmarshal(rendered, &renderedDoc); err != nil {
		return fmt.Errorf("parse rendered document: %w", err)
	}

	for _, field := range fields {
		frozenValue, frozenPresent := frozenDoc[field]
		renderedValue, renderedPresent := renderedDoc[field]

		// A missing payload field is a failure, not a vacuous pass. Without
		// this, renaming `cases` would leave both sides absent, every named
		// field would compare equal by not existing, and the guard would go
		// green over a document it no longer understands.
		if !frozenPresent || !renderedPresent {
			return fmt.Errorf(
				"payload field %q missing (frozen present=%v, rendered present=%v) -- "+
					"the document shape changed; the guard cannot compare what it "+
					"cannot find, and must not pass by failing to look",
				field, frozenPresent, renderedPresent)
		}
		if !bytes.Equal(frozenValue, renderedValue) {
			// ATTRIBUTE the difference. The two interpreters are reported
			// alongside the field so the reader can tell "the data moved" from
			// "the interpreter moved" without going and looking -- which is the
			// step the original incident skipped.
			frozenInterpreter, frozenErr := interpreterOf(frozen, "frozen")
			renderedInterpreter, renderedErr := interpreterOf(rendered, "live")
			if frozenErr != nil {
				return frozenErr
			}
			if renderedErr != nil {
				return renderedErr
			}
			attribution := "the interpreter is IDENTICAL on both sides, so this " +
				"is a real data change"
			if frozenInterpreter != renderedInterpreter {
				attribution = "the interpreter ALSO differs, so check whether the " +
					"data change follows from it before porting anything"
			}
			return fmt.Errorf(
				"payload field %q differs between frozen and live\n"+
					"  frozen produced by: %s\n  live produced by:   %s\n  %s",
				field, frozenInterpreter, renderedInterpreter, attribution)
		}
	}
	return nil
}
