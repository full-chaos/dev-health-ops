package providerunit

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// failureReturn matches the two transport constructors that mark a provider
// unit's attempt as FAILED. Deliberately not RetryableAfter/RateLimited/
// BudgetContention: those are attempt-neutral snoozes on the HEALTHY path (a
// chunk continuation granted, a provider rate limit honoured, a shared budget
// collision) where the work has not failed and there is no cause to record.
// DomainMismatch is excluded for the same reason its own line is: it is the
// guard clause refusing a malformed envelope before any claim exists, and its
// error is a package sentinel the adapter already names.
var failureReturn = regexp.MustCompile(`return jobruntime\.(Retryable|Permanent)\(`)

var safeCauseWrapper = regexp.MustCompile(`jobruntime\.WithSafeCause(Text)?\(`)

// TestEveryProviderUnitFailureReturnCarriesASafeCause is the class-level
// guard, added after codex round 2 found four failure returns the original
// change missed.
//
// The four were the DEFERRAL persistence paths -- RateLimitEpisode,
// DeferForRateLimit, DeferChunkContinuation and DeferForBudgetContention --
// and they were the worst case of all: the work itself had not failed, only
// the write that RECORDS the deferral had, so the durable trace said
// "dev-health job failed [retryable]" while the actual fault was a database
// write on a healthy unit's happy path. An operator reading that looks at the
// provider instead of the store.
//
// Enumerating the call sites once and fixing them is not enough -- that is
// exactly what the first pass did. This asserts the property instead, so a
// failure return added later cannot ship without a cause.
//
// RED CONTROL: unwrapping any one of the returns in providerunit.go fails this
// test naming its line.
func TestEveryProviderUnitFailureReturnCarriesASafeCause(t *testing.T) {
	t.Parallel()
	const source = "providerunit.go"
	body, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("cannot read %s: %v", source, err)
	}
	lines := strings.Split(string(body), "\n")
	found := 0
	for index, line := range lines {
		if !failureReturn.MatchString(line) {
			continue
		}
		found++
		// The wrapper may sit on the same line or on the continuation lines of
		// a wrapped call, so the window is the return statement itself: from
		// this line until the first line whose trimmed text ends the call.
		window := line
		for offset := 1; offset <= 3 && index+offset < len(lines); offset++ {
			window += "\n" + lines[index+offset]
			if strings.HasSuffix(strings.TrimSpace(lines[index+offset]), "))") {
				break
			}
		}
		if !safeCauseWrapper.MatchString(window) {
			t.Errorf(
				"%s:%d returns a FAILED attempt with no jobruntime.WithSafeCause/WithSafeCauseText.\n"+
					"River's durable error row is the fixed string \"dev-health job failed [<category>]\" "+
					"with an empty trace, so an unwrapped failure here is permanently undiagnosable -- "+
					"which is how the cause of sync run 115e6246's 77-second collapse was lost.\n\t%s",
				source, index+1, strings.TrimSpace(line),
			)
		}
	}
	// The scan must have found something. A renamed constructor, a moved file
	// or a broken regex would otherwise turn this into a guard that cannot
	// fail -- the vacuous class this package has already been bitten by.
	if found < 8 {
		t.Fatalf("found only %d failure returns in %s; this guard is not looking at anything", found, source)
	}
}
