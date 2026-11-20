package providersync

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
	"testing/iotest"
	"time"
)

// The in-place retry waits 25-100 ms before attempt 2 and 100-400 ms before
// attempt 3, drawn from crypto/rand; a failing entropy source waits the lower
// bound and never panics.
func TestStoreRetryJitterStaysInItsWindow(t *testing.T) {
	for _, testCase := range []struct {
		next      int
		low, high time.Duration
	}{
		{2, 25 * time.Millisecond, 100 * time.Millisecond},
		{3, 100 * time.Millisecond, 400 * time.Millisecond},
	} {
		low, high := storeRetryWindow(testCase.next)
		if low != testCase.low || high != testCase.high {
			t.Fatalf("window before attempt %d = [%s, %s), want [%s, %s)", testCase.next, low, high, testCase.low, testCase.high)
		}
		for draw := 0; draw < 200; draw++ {
			if got := storeRetryJitter(rand.Reader, low, high); got < low || got >= high {
				t.Fatalf("jitter before attempt %d = %s, outside [%s, %s)", testCase.next, got, low, high)
			}
		}
		if got := storeRetryJitter(bytes.NewReader(bytes.Repeat([]byte{0xff}, 16)), low, high); got < low || got >= high {
			t.Fatalf("jitter from the top of the entropy range = %s, outside [%s, %s)", got, low, high)
		}
		if got := storeRetryJitter(iotest.ErrReader(errors.New("no entropy")), low, high); got != low {
			t.Fatalf("jitter with failing entropy = %s, want the lower bound %s", got, low)
		}
	}
}
