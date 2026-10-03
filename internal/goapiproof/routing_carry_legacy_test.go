package goapiproof

// The legacy-document-digest rule of `carry` (CHAOS-8000 dual accept).
//
// A registered document that is swapped keeps its old text accepted as a
// legacy one. Before this rule `carry` read that swap as "the registered
// document changed" and refused the whole run -- with a DO NOT ROLL line
// for a go-only operation -- although the image being rolled to reads the
// row under the old digest exactly as before.
//
// The enumeration in routing_carry_test.go covers the rule over the whole
// input domain. The tests here pin what the enumeration's axes cannot
// state: each clause of the "is this a target-legacy digest" predicate on
// its own, the unchanged refusal text, and the two loaders the verb reads
// the legacy digests with.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	legacyCarryOperation = "capacityForecast"
	// The row's digest: the text the operation registered BEFORE the swap.
	legacyCarryOldDigest = carryDocument
	// The target image's current digest: the text registered by the swap.
	legacyCarryNewDigest = carryOtherDoc
	// A third digest neither image knows.
	legacyCarryUnknownDigest = "2222222222222222222222222222222222222222222222222222222222222222"
)

func legacyCarryRow(mode string) CarryRow {
	return CarryRow{
		Operation: legacyCarryOperation, DocumentDigest: legacyCarryOldDigest, Mode: mode,
		Build: carryBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	}
}

// legacyCarryInputs is the image of a dual-accept swap, complete and
// agreeing: the operation's current text is the new one, the old one is
// listed as legacy by the registered-document dump AND by the catalog.
// live is what the deployed process registers for the operation.
func legacyCarryInputs(live string) CarryInputs {
	return CarryInputs{
		LiveDocumentDigest:           map[string]string{legacyCarryOperation: live},
		TargetDocumentDigest:         map[string]string{legacyCarryOperation: legacyCarryNewDigest},
		TargetLegacyDocumentDigests:  map[string][]string{legacyCarryOperation: {legacyCarryOldDigest}},
		CatalogDocumentDigest:        map[string]string{legacyCarryOperation: legacyCarryNewDigest},
		CatalogLegacyDocumentDigests: map[string][]string{legacyCarryOperation: {legacyCarryOldDigest}},
		TargetRows:                   map[string]CarryRow{},
	}
}

// THE RULE: a row keyed to a digest the target image accepts as a legacy
// text is carried VERBATIM -- same document digest, same mode, rollout,
// build and owner -- whether the deployed process still registers the old
// text (the roll that brings the swap) or already registers the new one
// (every later roll, while the legacy text stays accepted).
func TestDecideCarryKeepsARowKeyedToATargetLegacyDigestUnderItsOwnDigest(t *testing.T) {
	for _, mode := range []string{TargetModeCanary, TargetModePrimary, TargetModeShadow} {
		for name, live := range map[string]string{
			"the deployed process registers the old text": legacyCarryOldDigest,
			"the deployed process registers the new text": legacyCarryNewDigest,
		} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				row := legacyCarryRow(mode)
				got := DecideCarry(row, legacyCarryInputs(live))
				if got.Action != CarryActionCarry {
					t.Fatalf("action = %s (%s), want CARRY: the target image reads this row under its legacy digest", got.Action, got.Reason)
				}
				if got.DocumentDigest != legacyCarryOldDigest {
					t.Fatalf("carried under %s, want the row's own digest %s -- a re-keyed row is a new enablement, and it leaves the old text without a row", got.DocumentDigest, legacyCarryOldDigest)
				}
				if got.Mode != row.Mode || got.RolloutPercentage != row.RolloutPercentage || got.Build != row.Build || got.Owner != row.Owner {
					t.Fatalf("outcome %+v does not copy the row verbatim (%+v)", got, row)
				}
				if !got.LegacyDigest || got.TargetCurrentDigest != legacyCarryNewDigest {
					t.Fatalf("legacy digest = %t (current %q), want the report to name this row as a legacy-digest carry with current %s", got.LegacyDigest, got.TargetCurrentDigest, legacyCarryNewDigest)
				}
			})
		}
	}
}

// A second run finds the row it wrote and reports it, still named as a
// legacy-digest row.
func TestDecideCarryReportsAnAlreadyCarriedLegacyRowAsUnchanged(t *testing.T) {
	for _, mode := range []string{TargetModeCanary, TargetModeShadow} {
		row := legacyCarryRow(mode)
		inputs := legacyCarryInputs(legacyCarryOldDigest)
		inputs.TargetRows[legacyCarryOperation] = row
		got := DecideCarry(row, inputs)
		if got.Action != CarryActionUnchanged || !got.LegacyDigest {
			t.Fatalf("mode=%s: action = %s legacy = %t (%s), want UNCHANGED named as a legacy-digest row", mode, got.Action, got.LegacyDigest, got.Reason)
		}
	}
}

// STILL REFUSED, WITH THE SAME TEXT: a served row whose digest is neither
// the target image's current digest nor one of ITS OWN operation's legacy
// digests. Each case removes one thing that makes a digest a legacy one.
func TestDecideCarryStillRefusesADigestThatIsNeitherCurrentNorLegacy(t *testing.T) {
	for name, mutate := range map[string]func(*CarryRow, *CarryInputs){
		"the image lists no legacy text": func(_ *CarryRow, inputs *CarryInputs) {
			inputs.TargetLegacyDocumentDigests = nil
		},
		"the image lists another digest as legacy": func(_ *CarryRow, inputs *CarryInputs) {
			inputs.TargetLegacyDocumentDigests[legacyCarryOperation] = []string{legacyCarryUnknownDigest}
		},
		"the digest is a legacy text of ANOTHER operation": func(_ *CarryRow, inputs *CarryInputs) {
			inputs.TargetLegacyDocumentDigests = map[string][]string{"aiImpactSummary": {legacyCarryOldDigest}}
		},
		"the row is keyed to a digest no image knows": func(row *CarryRow, inputs *CarryInputs) {
			row.DocumentDigest = legacyCarryUnknownDigest
			inputs.LiveDocumentDigest[legacyCarryOperation] = legacyCarryUnknownDigest
		},
	} {
		t.Run(name, func(t *testing.T) {
			row := legacyCarryRow(TargetModeCanary)
			inputs := legacyCarryInputs(legacyCarryOldDigest)
			mutate(&row, &inputs)

			got := DecideCarry(row, inputs)
			if got.Action != CarryActionRefuse || !errors.Is(got.Refusal, ErrCarryDocumentMoved) {
				t.Fatalf("action = %s (%v: %s), want the refusal classified as a moved document", got.Action, got.Refusal, got.Reason)
			}
			want := fmt.Sprintf("the registered document changed: this row is keyed to %s, the target image registers %s", row.DocumentDigest, legacyCarryNewDigest)
			if got.Reason != want {
				t.Fatalf("reason = %q, want the unchanged text %q", got.Reason, want)
			}
			if got.LegacyDigest {
				t.Fatal("a refused row that is not a legacy-digest row is named as one")
			}

			// The shadow row of the same shape is dropped with the unchanged
			// reason, never refused and never carried.
			row.Mode = TargetModeShadow
			shadow := DecideCarry(row, inputs)
			if shadow.Action != CarryActionSkip || shadow.Refusal != nil ||
				!strings.Contains(shadow.Reason, "mode=shadow: the registered document changed") {
				t.Fatalf("shadow: action = %s (%v: %s), want a SKIP naming the changed document", shadow.Action, shadow.Refusal, shadow.Reason)
			}
		})
	}
}

// The catalog is the edge's map from a request text's digest to its
// operation. A legacy digest the registered-document dump accepts and the
// catalog does not list for that operation is an image that cannot
// dispatch the old text: a refusal naming the catalog for a served row, a
// SKIP for a shadow one.
func TestDecideCarryRefusesALegacyDigestTheCatalogDoesNotList(t *testing.T) {
	for name, catalogLegacy := range map[string]map[string][]string{
		"the catalog lists no legacy digest":                 nil,
		"the catalog lists another legacy digest":            {legacyCarryOperation: {legacyCarryUnknownDigest}},
		"the catalog lists the digest for ANOTHER operation": {"aiImpactSummary": {legacyCarryOldDigest}},
	} {
		t.Run(name, func(t *testing.T) {
			inputs := legacyCarryInputs(legacyCarryOldDigest)
			inputs.CatalogLegacyDocumentDigests = catalogLegacy

			got := DecideCarry(legacyCarryRow(TargetModeCanary), inputs)
			if got.Action != CarryActionRefuse || !errors.Is(got.Refusal, ErrCarryImageDisagrees) {
				t.Fatalf("action = %s (%v: %s), want a refusal classified as an image disagreement", got.Action, got.Refusal, got.Reason)
			}
			if !strings.Contains(got.Reason, "its edge catalog does not list it as one") || !strings.Contains(got.Reason, "regenerate the catalog") {
				t.Fatalf("reason %q does not name the catalog as the artifact to regenerate", got.Reason)
			}

			shadow := DecideCarry(legacyCarryRow(TargetModeShadow), inputs)
			if shadow.Action != CarryActionSkip || shadow.Refusal != nil || !strings.Contains(shadow.Reason, "edge catalog does not agree") {
				t.Fatalf("shadow: action = %s (%v: %s), want a SKIP naming the catalog", shadow.Action, shadow.Refusal, shadow.Reason)
			}
		})
	}
}

// The current-text half of the catalog check still applies to a legacy
// row: a catalog whose CURRENT digest for the operation is not the one the
// image registers cannot dispatch the new text.
func TestDecideCarryRefusesALegacyRowWhenTheCatalogCurrentDigestDisagrees(t *testing.T) {
	inputs := legacyCarryInputs(legacyCarryOldDigest)
	inputs.CatalogDocumentDigest[legacyCarryOperation] = legacyCarryUnknownDigest
	got := DecideCarry(legacyCarryRow(TargetModeCanary), inputs)
	if got.Action != CarryActionRefuse || !errors.Is(got.Refusal, ErrCarryImageDisagrees) {
		t.Fatalf("action = %s (%v: %s), want a refusal classified as an image disagreement", got.Action, got.Refusal, got.Reason)
	}
}

// A DEAD ROW STAYS A SKIP. The legacy rule relaxes "the deployed process
// registers another digest" only for a digest the target image lists as a
// legacy text of an operation it registers under ANOTHER current digest.
// Each case breaks one clause of that predicate on a row the deployed
// process does not read, and the row must still be skipped as unreachable
// -- never carried (an enablement nobody decided) and never refused (a
// stale row blocking a roll).
func TestDecideCarrySkipsADeadRowTheLegacyRuleDoesNotCover(t *testing.T) {
	for name, mutate := range map[string]func(*CarryInputs){
		// target != row.DocumentDigest: the image registers the row's
		// digest as its CURRENT text and (wrongly) lists it as legacy too.
		"the digest is the target image's current text": func(inputs *CarryInputs) {
			inputs.TargetDocumentDigest[legacyCarryOperation] = legacyCarryOldDigest
			inputs.CatalogDocumentDigest[legacyCarryOperation] = legacyCarryOldDigest
		},
		// serves: a legacy list for an operation the image does not register.
		"the image does not register the operation": func(inputs *CarryInputs) {
			delete(inputs.TargetDocumentDigest, legacyCarryOperation)
		},
		// listed: no legacy list at all.
		"the image lists no legacy text": func(inputs *CarryInputs) {
			inputs.TargetLegacyDocumentDigests = nil
		},
	} {
		for _, mode := range []string{TargetModeCanary, TargetModeShadow} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				// The deployed process registers a digest that is not the row's.
				inputs := legacyCarryInputs(legacyCarryUnknownDigest)
				mutate(&inputs)
				got := DecideCarry(legacyCarryRow(mode), inputs)
				if got.Action != CarryActionSkip || got.Refusal != nil {
					t.Fatalf("action = %s (%v: %s), want a SKIP: the deployed process does not read this row", got.Action, got.Refusal, got.Reason)
				}
				if !strings.Contains(got.Reason, "is not the one the deployed process registers") {
					t.Fatalf("reason %q does not say the row is already unreachable", got.Reason)
				}
			})
		}
	}
}

// A decision somebody took at the target digest is never overwritten, for
// a legacy-digest row as for any other.
func TestDecideCarryNeverOverwritesATargetRowWithALegacyRow(t *testing.T) {
	different := legacyCarryRow(TargetModeCanary)
	different.DocumentDigest = legacyCarryNewDigest

	inputs := legacyCarryInputs(legacyCarryOldDigest)
	inputs.TargetRows[legacyCarryOperation] = different
	got := DecideCarry(legacyCarryRow(TargetModeCanary), inputs)
	if got.Action != CarryActionRefuse || !errors.Is(got.Refusal, ErrCarryTargetRowExists) {
		t.Fatalf("action = %s (%v: %s), want the refusal that names the row already at the target digest", got.Action, got.Refusal, got.Reason)
	}

	shadow := DecideCarry(legacyCarryRow(TargetModeShadow), inputs)
	if shadow.Action != CarryActionSkip || !strings.Contains(shadow.Reason, "already holds a different row") {
		t.Fatalf("shadow: action = %s (%s), want a SKIP naming the different target row", shadow.Action, shadow.Reason)
	}
}

// ---- the two loaders the verb reads the legacy digests with ---------------

func TestLoadDocumentsWithLegacyReturnsEachOperationsLegacyTextsInFileOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.json")
	body := `[
  {"operation": "bar", "document": "query Bar { bar }", "digest": "d-bar"},
  {"operation": "foo", "document": "query Foo { new }", "digest": "d-new"},
  {"operation": "foo", "document": "query Foo { older }", "digest": "d-older", "legacy": true},
  {"operation": "foo", "document": "query Foo { old }", "digest": "d-old", "legacy": true}
]`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	current, legacy, err := LoadDocumentsWithLegacy(path)
	if err != nil {
		t.Fatal(err)
	}
	wantCurrent := map[string]string{"foo": "query Foo { new }", "bar": "query Bar { bar }"}
	if !reflect.DeepEqual(current, wantCurrent) {
		t.Fatalf("current = %v, want %v", current, wantCurrent)
	}
	wantLegacy := map[string][]string{"foo": {"query Foo { older }", "query Foo { old }"}}
	if !reflect.DeepEqual(legacy, wantLegacy) {
		t.Fatalf("legacy = %v, want %v", legacy, wantLegacy)
	}
	// LoadDocuments reads the same file to the same current texts.
	plain, err := LoadDocuments(path)
	if err != nil || !reflect.DeepEqual(plain, wantCurrent) {
		t.Fatalf("LoadDocuments = %v (%v), want %v", plain, err, wantCurrent)
	}
}

func TestLoadDocumentsWithLegacyRefusesALegacyTextWithNoCurrentOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.json")
	body := `[
  {"operation": "bar", "document": "query Bar { bar }"},
  {"operation": "foo", "document": "query Foo { old }", "legacy": true}
]`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadDocumentsWithLegacy(path); err == nil || !strings.Contains(err.Error(), `"foo"`) || !strings.Contains(err.Error(), "no current text") {
		t.Fatalf("err = %v, want a refusal naming foo and the missing current text", err)
	}
}

func TestLoadOperationCatalogWithLegacyReturnsEachOperationsLegacyDigestsInFileOrder(t *testing.T) {
	path := writeCatalogFile(t, `[
  {"operation": "foo", "digest": "d-older", "legacy": true},
  {"operation": "bar", "digest": "d-bar"},
  {"operation": "foo", "digest": "d-new"},
  {"operation": "foo", "digest": "d-old", "legacy": true}
]`)
	current, legacy, err := LoadOperationCatalogWithLegacy(path)
	if err != nil {
		t.Fatal(err)
	}
	wantCurrent := map[string]string{"foo": "d-new", "bar": "d-bar"}
	if !reflect.DeepEqual(current, wantCurrent) {
		t.Fatalf("current = %v, want %v", current, wantCurrent)
	}
	wantLegacy := map[string][]string{"foo": {"d-older", "d-old"}}
	if !reflect.DeepEqual(legacy, wantLegacy) {
		t.Fatalf("legacy = %v, want %v", legacy, wantLegacy)
	}
	// The guards are the shared reader's: a catalog the plain loader
	// refuses is refused here too.
	broken := writeCatalogFile(t, `[{"operation":"bar","digest":"d2"},{"operation":"foo","digest":"d1","legacy":true}]`)
	if _, _, err := LoadOperationCatalogWithLegacy(broken); !errors.Is(err, ErrCatalogUnusable) {
		t.Fatalf("err = %v, want the catalog refused as unusable", err)
	}
}
