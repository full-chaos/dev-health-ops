package scopeparity

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func corpus(t *testing.T) Corpus {
	t.Helper()
	loaded, err := LoadCorpus(filepath.Join("testdata", "corpus_seed1.json"))
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	return loaded
}

// faithfulAdapter answers exactly as the corpus says the reference does. It is
// the control: Compare must find NOTHING against it, or every result below is
// meaningless.
func faithfulAdapter(t *testing.T, loaded Corpus) Adapter {
	t.Helper()
	byScope := map[string]Case{}
	for _, testCase := range loaded.Cases {
		byScope[compact(testCase.Scope)] = testCase
	}
	return func(scope json.RawMessage) (Resolved, error) {
		testCase, ok := byScope[compact(scope)]
		if !ok || testCase.Verdict == "RAISES" {
			return Resolved{}, errors.New("refused")
		}
		from, _ := parseInstant(testCase.Window.From)
		to, _ := parseInstant(testCase.Window.To)
		repo := ""
		if testCase.Window.RepoID != nil {
			repo = *testCase.Window.RepoID
		}
		return Resolved{From: from, To: to, RepoID: repo}, nil
	}
}

func TestCompareFindsNothingAgainstAFaithfulAdapter(t *testing.T) {
	loaded := corpus(t)
	if found := Compare(loaded, faithfulAdapter(t, loaded), nil); len(found) != 0 {
		t.Fatalf("comparator reported %d disagreements against a faithful adapter: %v", len(found), found[0])
	}
}

// TestCompareIsFalsifiable plants a defect of each shape the comparator exists
// to catch. A comparator that has never been shown to fail produces the same
// green as a correct adapter, so this runs before any parity claim is made.
func TestCompareIsFalsifiable(t *testing.T) {
	loaded := corpus(t)
	faithful := faithfulAdapter(t, loaded)

	t.Run("accepts what the reference rejects", func(t *testing.T) {
		// The direction that writes rows for a doomed build.
		lax := func(scope json.RawMessage) (Resolved, error) {
			resolved, err := faithful(scope)
			if err != nil {
				return Resolved{From: time.Now(), To: time.Now()}, nil
			}
			return resolved, nil
		}
		found := Compare(loaded, lax, nil)
		if len(found) == 0 {
			t.Fatal("an adapter accepting everything went undetected")
		}
		for _, disagreement := range found {
			if disagreement.Kind == KindAcceptedRejected {
				return
			}
		}
		t.Fatalf("reported %d disagreements but none of the accepted-rejected kind", len(found))
	})

	t.Run("refuses what the reference accepts", func(t *testing.T) {
		strict := func(json.RawMessage) (Resolved, error) { return Resolved{}, errors.New("refuse all") }
		found := Compare(loaded, strict, nil)
		if len(found) == 0 {
			t.Fatal("an adapter refusing everything went undetected")
		}
	})

	t.Run("derives a different window", func(t *testing.T) {
		skewed := func(scope json.RawMessage) (Resolved, error) {
			resolved, err := faithful(scope)
			if err != nil {
				return resolved, err
			}
			resolved.To = resolved.To.Add(time.Second)
			return resolved, nil
		}
		found := Compare(loaded, skewed, nil)
		if len(found) == 0 {
			t.Fatal("a one-second window skew went undetected")
		}
		for _, disagreement := range found {
			if disagreement.Kind == KindWindowDiffers {
				return
			}
		}
		t.Fatalf("reported %d disagreements but none of the window kind", len(found))
	})

	t.Run("drops a repo_id", func(t *testing.T) {
		blank := func(scope json.RawMessage) (Resolved, error) {
			resolved, err := faithful(scope)
			resolved.RepoID = ""
			return resolved, err
		}
		if found := Compare(loaded, blank, nil); len(found) == 0 {
			t.Fatal("a dropped repo_id went undetected")
		}
	})
}

// TestEnumerationCannotSilenceTheDangerousDirection is the rule that makes this
// package worth having: a fail-closed divergence may be enumerated with a
// reason; accepting a scope the reference rejects may NOT, at any price.
func TestEnumerationCannotSilenceTheDangerousDirection(t *testing.T) {
	loaded := corpus(t)
	faithful := faithfulAdapter(t, loaded)
	lax := func(scope json.RawMessage) (Resolved, error) {
		if resolved, err := faithful(scope); err == nil {
			return resolved, nil
		}
		return Resolved{}, nil
	}

	// Try to silence EVERY case by enumerating all of them.
	everything := map[string]string{}
	for _, testCase := range loaded.Cases {
		everything[compact(testCase.Scope)] = "attempting to silence this"
	}

	found := Compare(loaded, lax, everything)
	dangerous := 0
	for _, disagreement := range found {
		if disagreement.Kind == KindAcceptedRejected {
			dangerous++
		}
	}
	if dangerous == 0 {
		t.Fatal("enumerating every case silenced the accepted-rejected direction; it must be unsilenceable")
	}
}

// TestStaleEnumerationIsReported: an entry whose case now agrees is itself a
// finding, because it hides the next real divergence on that input.
func TestStaleEnumerationIsReported(t *testing.T) {
	loaded := corpus(t)
	faithful := faithfulAdapter(t, loaded)

	var agreeing string
	for _, testCase := range loaded.Cases {
		if testCase.Verdict == "RUNS" {
			agreeing = compact(testCase.Scope)
			break
		}
	}
	if agreeing == "" {
		t.Skip("corpus has no RUNS case")
	}

	found := Compare(loaded, faithful, map[string]string{agreeing: "no longer true"})
	for _, disagreement := range found {
		if disagreement.Kind == "stale_enumerated_divergence" && disagreement.Scope == agreeing {
			return
		}
	}
	t.Fatalf("a stale divergence entry was not reported; got %d disagreements", len(found))
}

// TestEnumerationCannotHideASuccessfulButWrongWindow pins the hole the CHAOS-4815
// review found: the allowlist is documented to excuse only the adapter REFUSING a
// scope the reference runs. It must not excuse the adapter SUCCEEDING with the
// wrong window.
//
// Before the fix, an enumerated RUNS case short-circuited to staleEntry and
// `continue`d before windowDiffers ever ran -- so a one-second window skew was
// reported as "the adapter now agrees with the reference", which is not merely a
// missed finding but an actively FALSE message pointing the reader away from a
// live divergence.
//
// TestStaleEnumerationIsReported cannot catch this: it uses the faithful adapter,
// for which the case genuinely does agree.
func TestEnumerationCannotHideASuccessfulButWrongWindow(t *testing.T) {
	loaded := corpus(t)
	faithful := faithfulAdapter(t, loaded)

	var target string
	for _, testCase := range loaded.Cases {
		if testCase.Verdict == "RUNS" {
			target = compact(testCase.Scope)
			break
		}
	}
	if target == "" {
		t.Fatal("corpus has no RUNS case, so this test would assert nothing")
	}

	// Faithful everywhere EXCEPT the target, whose window is skewed by one
	// second -- a successful return carrying the wrong answer.
	skewed := Adapter(func(scope json.RawMessage) (Resolved, error) {
		resolved, err := faithful(scope)
		if err != nil || compact(scope) != target {
			return resolved, err
		}
		resolved.To = resolved.To.Add(time.Second)
		return resolved, nil
	})

	found := Compare(loaded, skewed, map[string]string{target: "excused refusal"})

	var sawWindowDiffers bool
	for _, disagreement := range found {
		if disagreement.Scope == target && disagreement.Kind == KindWindowDiffers {
			sawWindowDiffers = true
		}
	}
	if !sawWindowDiffers {
		t.Fatalf("an enumerated entry hid a successful-but-wrong window: "+
			"KindWindowDiffers was not reported for %s; got %d disagreements",
			target, len(found))
	}
}
