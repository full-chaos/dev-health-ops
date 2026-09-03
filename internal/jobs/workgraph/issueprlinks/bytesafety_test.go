package issueprlinks

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// Byte-index arithmetic on a value the reference treats as CHARACTERS is a
// named defect class (lane-4752-go round 3: an ISO normaliser corrupted a
// multi-byte separator via candidate[10], and a digit limit counted bytes where
// the reference counts characters).
//
// `ParsePRSource` byte-slices: it finds the separator with strings.LastIndex
// and then slices `body[:index]` / `body[index+len(separator):]`.
//
// # The invariant, and the CORRECTED reasoning for it
//
// I first justified this as "a multi-byte UTF-8 sequence contains no byte below
// 0x80, so an index found on an ASCII separator cannot land inside a rune."
// That is true for WELL-FORMED UTF-8 only, and lane-4441 was right to reject it
// as the whole story: `\xff` is >= 0x80 and belongs to no valid sequence, so a
// value carrying invalid bytes is not "runes with ASCII separators" at all.
//
// The invariant that actually holds has two halves, and both are load-bearing:
//
//  1. the separator is ASCII, so on well-formed UTF-8 the split point is a rune
//     boundary — asserted below over multi-byte slugs; and
//  2. the value IS well-formed UTF-8 by the time this sees it, BECAUSE THE
//     READER GUARANTEES IT. In Python that holds because clickhouse-connect
//     replaces an undecodable value with the hex of its bytes (pure ASCII); in
//     Go it holds only once the reader applies the same substitution
//     (pythonparity.DecodeClickHouseString, ledgered for PR3).
//
// Half 2 is not this package's to enforce, so this file asserts half 1 and
// pins the failure mode of half 2 rather than pretending it is covered.

func TestParsePRSourceSlicesOnRuneBoundariesWithMultiByteSlugs(t *testing.T) {
	// Slugs whose bytes include sequences at every UTF-8 width, so a split at a
	// byte index would corrupt one of them if the reasoning were wrong.
	slugs := []string{
		"öwner/repo",      // 2-byte
		"owner/日本語",       // 3-byte
		"owner/repo𐀀",     // 4-byte (astral)
		"Ωμέγα/répô-🧪",    // mixed widths incl. emoji
		"owner/re#po",     // the separator INSIDE the slug (last-wins)
		"öwner#日本語𐀀/repo", // multi-byte on BOTH sides of an inner separator
	}
	for _, slug := range slugs {
		t.Run(slug, func(t *testing.T) {
			source, ok := ParsePRSource("ghpr:" + slug + "#12")
			if !ok {
				t.Fatalf("ParsePRSource refused a valid multi-byte slug %q", slug)
			}
			if source.RepoSlug != slug {
				t.Fatalf("slug = %q, want %q — a byte index landed inside a rune", source.RepoSlug, slug)
			}
			if source.PRNumber != 12 {
				t.Fatalf("pr_number = %d, want 12", source.PRNumber)
			}
			if !utf8.ValidString(source.RepoSlug) {
				t.Fatalf("slug %q is no longer valid UTF-8 after slicing", source.RepoSlug)
			}
		})
	}
}

// TestParsePRSourceOnInvalidUTF8IsTheReadersProblem pins half 2's failure mode
// rather than claiming this package handles it.
//
// With raw invalid bytes the slice is still byte-safe — the separator is ASCII
// and slicing at it cannot split a *valid* sequence — but the two planes hold
// DIFFERENT VALUES at this point: Python received the hex of the whole value
// from clickhouse-connect, so it never sees a `ghpr:` prefix at all and rejects,
// while Go sees the raw bytes and parses. That is a reader divergence, not a
// slicing bug, and it is why DecodeClickHouseString is ledgered for PR3.
func TestParsePRSourceOnInvalidUTF8IsTheReadersProblem(t *testing.T) {
	invalid := "ghpr:ow\xffner/repo#12"
	if utf8.ValidString(invalid) {
		t.Fatal("test input is supposed to be invalid UTF-8")
	}

	source, ok := ParsePRSource(invalid)
	if !ok {
		t.Skip("parser rejects it outright; nothing to pin")
	}
	// The slug round-trips byte-for-byte: slicing did not corrupt anything.
	if want := "ow\xffner/repo"; source.RepoSlug != want {
		t.Fatalf("slug = %q, want %q — slicing corrupted the value", source.RepoSlug, want)
	}
	// But Python would never reach here: it sees the hex of the whole value.
	hexed := "676870723a6f77ff6e65722f7265706f233132"
	if _, hexOK := ParsePRSource(hexed); hexOK {
		t.Fatalf("the hexed form parsed as a PR source; the planes were assumed to diverge here")
	}
	if strings.HasPrefix(hexed, "ghpr:") {
		t.Fatal("the hexed form still carries the prefix; the divergence argument is wrong")
	}
}
