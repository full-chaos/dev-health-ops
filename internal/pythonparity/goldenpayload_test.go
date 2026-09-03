package pythonparity

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func comparePayload(frozen, rendered []byte, fields ...string) error {
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
