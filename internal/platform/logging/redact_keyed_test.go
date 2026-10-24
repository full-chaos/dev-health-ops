package logging

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestRedactKeyedTextHidesProtectedKeysAndScrubsTheRest(t *testing.T) {
	core := strings.Repeat("A1b2C3d4", 5)
	if got := RedactKeyedText("password", "anything"); got != redacted {
		t.Errorf("protected key: %q", got)
	}
	if got := RedactKeyedText("note", "see gh"+"p_"+core+" now"); strings.Contains(got, core) {
		t.Errorf("credential survived: %q", got)
	}
	if got := RedactKeyedText("note", "plain text"); got != "plain text" {
		t.Errorf("plain text changed: %q", got)
	}
	if got := RedactKeyedText("note", strings.Repeat("x", maxLoggedValueBytes+10)); len(got) > maxLoggedValueBytes+len(truncatedSuffix) {
		t.Errorf("value not bounded: %d bytes", len(got))
	}
	if !KeyProtected("api_key") || KeyProtected("outcome") {
		t.Error("KeyProtected disagrees with ProtectedKey")
	}
}

func TestKeyCleanCacheStaysWithinItsBoundUnderConcurrentMisses(t *testing.T) {
	clear := func() {
		cleanKeys.Range(func(k, _ any) bool { cleanKeys.Delete(k); return true })
		cleanKeyCount.Store(0)
	}
	clear()
	t.Cleanup(clear)
	var wg sync.WaitGroup
	for worker := 0; worker < 64; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				KeyClean(fmt.Sprintf("bound_probe_%d_%d", worker, i))
			}
		}()
	}
	wg.Wait()
	stored := 0
	cleanKeys.Range(func(_, _ any) bool { stored++; return true })
	if stored > maxCleanKeys {
		t.Errorf("clean-key cache holds %d entries, bound is %d", stored, maxCleanKeys)
	}
	if got := int(cleanKeyCount.Load()); got != stored {
		t.Errorf("count %d disagrees with stored entries %d", got, stored)
	}
}
