package pythonparity

import (
	"sync"
	"testing"
)

// TestLowerHandlesContextSensitiveFinalSigma pins the one divergence that is
// NOT a length change, so the enumeration above cannot see it: a sigma's
// mapping depends on its POSITION, and strings.ToLower gets it wrong while
// producing a same-length, plausible-looking result.
func TestLowerHandlesContextSensitiveFinalSigma(t *testing.T) {
	for _, testCase := range []struct{ input, want string }{
		{"ΟΔΟΣ", "οδος"},   // final position -> final sigma
		{"ΣΟΦΟΣ", "σοφος"}, // both positions in one word
		{"Σ", "σ"},         // lone sigma is NOT final
	} {
		if got := Lower(testCase.input); got != testCase.want {
			t.Errorf("Lower(%q) = %q, want %q", testCase.input, got, testCase.want)
		}
	}
}

// TestLowerAndUpperAreSafeUnderConcurrency is a REGRESSION guard on the
// pooling, not a demonstration that sharing is unsafe.
//
// x/text documents that a Caser may be stateful and must not be shared
// between goroutines (cases.go:35-36; only cases.Fold is exempt, :87), and
// Caser.String calls transform.String, which begins with t.Reset() -- a
// mutation. A package-level shared Caser is therefore a latent race even
// though a 64-goroutine probe over final-sigma inputs did not flag one. Run
// under -race, this asserts the pooled implementation stays correct when
// hammered concurrently; if someone later "simplifies" the pool back into a
// package-level var, this is the test that has a chance of catching it.
func TestLowerAndUpperAreSafeUnderConcurrency(t *testing.T) {
	inputs := []string{"ΟΔΟΣ", "ΣΟΦΟΣ", "İstanbul", "straße", "AI-Assisted", "CHANGES_REQUESTED"}
	wantLower := make([]string, len(inputs))
	wantUpper := make([]string, len(inputs))
	for index, input := range inputs {
		wantLower[index] = Lower(input)
		wantUpper[index] = Upper(input)
	}

	var waitGroup sync.WaitGroup
	failures := make(chan string, 64)
	for goroutine := 0; goroutine < 64; goroutine++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 200; iteration++ {
				for index, input := range inputs {
					if got := Lower(input); got != wantLower[index] {
						failures <- "Lower(" + input + ") = " + got
						return
					}
					if got := Upper(input); got != wantUpper[index] {
						failures <- "Upper(" + input + ") = " + got
						return
					}
				}
			}
		}()
	}
	waitGroup.Wait()
	close(failures)
	for failure := range failures {
		t.Fatalf("concurrent casing produced a wrong result: %s", failure)
	}
}

func TestLowerAndUpperPassThroughTheEmptyString(t *testing.T) {
	if got := Lower(""); got != "" {
		t.Fatalf("Lower(\"\") = %q", got)
	}
	if got := Upper(""); got != "" {
		t.Fatalf("Upper(\"\") = %q", got)
	}
}
