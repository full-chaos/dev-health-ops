package migrationmatrix

import (
	"strings"
	"testing"
)

// F8 (CHAOS-5581, opus-r10): shortHex's own comment claims "anything that
// is not plain hex is printed whole, so a malformed value is never
// disguised as a real one by truncation" -- but nothing exercised that
// claim, so a mutant that drops the hex charset test (truncating any long
// string) left every existing test green. A non-hex document digest is
// exactly the producer class R88's trust boundary admits: an operator
// inserting a row by hand.
func TestShortHexPrintsNonHexWhole(t *testing.T) {
	for _, c := range []struct {
		name  string
		input string
		want  string
	}{
		{"a real hex digest, truncated", "0123456789abcdef0123456789abcdef", "0123456789ab…"},
		{"exactly 12 hex characters, not truncated (len must be > 12)", "0123456789ab", "0123456789ab"},
		{"a long non-hex operation-style name, printed whole", "x' OR ''='' -- an injection-shaped operation name over 12 chars", "x' OR ''='' -- an injection-shaped operation name over 12 chars"},
		{"hex characters with one uppercase letter, printed whole", "0123456789AB0123456789ab", "0123456789AB0123456789ab"},
		{"a hyphenated UUID-shaped value, printed whole", "11111111-1111-4111-8111-111111111111", "11111111-1111-4111-8111-111111111111"},
		{"hex at both ends, non-hex in the middle, printed whole", "aaaaaaaa-XXXX-aaaaaaaa", "aaaaaaaa-XXXX-aaaaaaaa"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := shortHex(c.input); got != c.want {
				t.Fatalf("shortHex(%q) = %q, want %q", c.input, got, c.want)
			}
		})
	}
}

// F10 (CHAOS-5581, opus-r10): the sort's live-first key survives losing it
// in the mutation sense -- deleting `rows[i].Live != rows[j].Live` /
// `return rows[i].Live` and falling through to the schema-digest
// comparison leaves every existing test green, because in every existing
// fixture live-first and schema-digest order already coincide. Here they
// deliberately do not: the DEAD row's schema digest sorts BEFORE the live
// row's, so only the live-first key -- not the digest tie-break below it,
// which is already pinned by TestTheOpsBlockDoesNotDependOnTheOrderRowsArriveIn
// -- can put the live row first.
func TestLiveRowsSortBeforeDeadOnesEvenWhenTheirSchemaDigestSortsAfter(t *testing.T) {
	live := liveRow("zzLastAlphabetically", "canary")
	dead := liveRow("aaFirstAlphabetically", "primary")
	dead.Live = false
	dead.SchemaDigest = "sha256:" + strings.Repeat("0", 64)
	if dead.SchemaDigest >= live.SchemaDigest {
		t.Fatalf("test setup: the dead row's schema digest must sort BEFORE the live row's for this to distinguish the live-first key from the digest tie-break (dead=%q live=%q)", dead.SchemaDigest, live.SchemaDigest)
	}
	catalog := catalogFor(t, live.Operation, dead.Operation)
	block := RenderOpsBlock(snapshot(dead, live), catalog)
	liveAt := strings.Index(block, live.Operation)
	deadAt := strings.Index(block, dead.Operation)
	if liveAt < 0 || deadAt < 0 {
		t.Fatalf("both operations must appear in the rendered block:\n%s", block)
	}
	if liveAt > deadAt {
		t.Fatalf("the live row must render before the dead one despite sorting after it by schema digest:\n%s", block)
	}
}
