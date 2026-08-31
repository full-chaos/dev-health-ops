package main

import (
	"fmt"
	"strings"
	"testing"
)

// TestTruncateForLog is the CHAOS-4647 P3 fix's direct proof (codex
// review, merge-gate round, ARGUED: the unwrap-chain log line had no
// size bound, and the deepest cause is frequently server-authored
// ClickHouse exception text this process does not control).
func TestTruncateForLog(t *testing.T) {
	t.Run("short string passes through unchanged", func(t *testing.T) {
		got := truncateForLog("short", 4096)
		if got != "short" {
			t.Fatalf("got %q, want unchanged %q", got, "short")
		}
	})

	t.Run("string at exactly the limit passes through unchanged", func(t *testing.T) {
		s := strings.Repeat("x", maxUnwrapChainLogBytes)
		got := truncateForLog(s, maxUnwrapChainLogBytes)
		if got != s {
			t.Fatalf("got length %d, want unchanged length %d", len(got), len(s))
		}
	})

	t.Run("oversized string is truncated and says so", func(t *testing.T) {
		s := strings.Repeat("x", maxUnwrapChainLogBytes+1000)
		got := truncateForLog(s, maxUnwrapChainLogBytes)
		if !strings.HasPrefix(got, strings.Repeat("x", maxUnwrapChainLogBytes)) {
			t.Fatalf("truncated output does not start with the first %d bytes of input", maxUnwrapChainLogBytes)
		}
		if !strings.Contains(got, "truncated") {
			t.Fatalf("truncated output %q does not visibly say it was truncated", got)
		}
		wantTotal := fmt.Sprintf("%d", len(s))
		if !strings.Contains(got, wantTotal) {
			// the original TOTAL length should be recoverable from the
			// message, not just "it was cut somewhere"
			t.Fatalf("truncated output does not name the original total length (%s); got suffix %q", wantTotal, got[len(got)-60:])
		}
		// A truncated line must still be BOUNDED -- the whole point of this
		// fix -- not merely shorter than the input.
		const maxReasonableSuffixOverhead = 64
		if len(got) > maxUnwrapChainLogBytes+maxReasonableSuffixOverhead {
			t.Fatalf("truncated output length %d exceeds the bound (%d) by more than the suffix should ever add",
				len(got), maxUnwrapChainLogBytes)
		}
	})

	t.Run("real CHAOS-4647 unwrap chain shape stays under the bound", func(t *testing.T) {
		// The actual chain shape this fix protects: gqlgen's own
		// "<op>: <op>: ..." prefixing plus a ClickHouse code/message tail
		// that, in the worst real case this ticket found, named a real
		// query error inline -- simulate a pathological one far larger
		// than anything seen live.
		pathological := "hotspots: hotspots: rows: ClickHouse row iteration failed <- " +
			"code: 307, message: " + strings.Repeat("query fragment echoed by the server ", 500)
		got := truncateForLog(pathological, maxUnwrapChainLogBytes)
		if len(got) >= len(pathological) {
			t.Fatalf("a pathologically long real-shaped chain (%d bytes) was not shortened", len(pathological))
		}
		if len(got) > maxUnwrapChainLogBytes+128 {
			t.Fatalf("truncated real-shaped chain is still %d bytes, want at most ~%d", len(got), maxUnwrapChainLogBytes)
		}
	})
}
