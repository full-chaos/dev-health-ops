package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// A credential constant a test sends on purpose (a push token, a password the CLI puts in a header) is the same
// value in every run, so a Scrub that blanks it is refused (the value would stop being compared), and a golden
// that stores it holds a token-shaped value. GoldenSpec.CredentialConstants declares such values by exact text
// (CHAOS-7898): the harness, not the test, replaces each by `<credential sha256:<12 hex>>` in every request key,
// compared answer, header and row (Golden.DigestDeclared for an answer a test compares itself), so the VALUE stays
// compared (as its digest) and is never stored. The golden's header holds the full sha256 of each declared
// constant under a name no secret scanner reads as a key (declared_constants_sha256): a frozen run refuses a test
// that declares other constants. Exact values only, no pattern.

// declaredPlaceholderHex is how many hex digits of the digest the placeholder carries: 48 bits, which keeps two of
// at most a few dozen test constants apart (collision odds about n*n/2^49) and keeps the golden free of a
// 64-hex value in a body. The header holds the full digest.
const declaredPlaceholderHex = 12

func constantDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// declaredDigests is the sorted full digests of the declared constants.
func declaredDigests(constants []string) []string {
	out := make([]string, 0, len(constants))
	for _, value := range constants {
		out = append(out, constantDigest(value))
	}
	sort.Strings(out)
	return out
}

// declaredConstantsErr is an error unless the declaration is usable: no very short constant (it would replace
// text that is not a credential) and none twice. One constant may be a prefix of another ("fcpush_f" of
// "fcpush_flag"): the longer is replaced first, so each stays pinned by its own digest.
func declaredConstantsErr(constants []string) error {
	return declaredConstantsErrWidth(constants, declaredPlaceholderHex)
}

// declaredConstantsErrWidth is declaredConstantsErr for a placeholder of width hex digits (a test narrows it to find
// a collision without 2^24 candidates).
func declaredConstantsErrWidth(constants []string, width int) error {
	for index, value := range constants {
		if len(value) < 8 {
			return fmt.Errorf("GoldenSpec.CredentialConstants entry #%d is shorter than 8 characters: such a text is not a credential and would be replaced inside unrelated text", index)
		}
		for other := 0; other < index; other++ {
			if constants[other] == value {
				return fmt.Errorf("GoldenSpec.CredentialConstants entry #%d repeats entry #%d: declare each constant once", index, other)
			}
			if constantDigest(constants[other])[:width] == constantDigest(value)[:width] {
				return fmt.Errorf("GoldenSpec.CredentialConstants entries #%d and #%d have the same %d-hex placeholder: their answers could not be told apart; change one constant", other, index, width)
			}
		}
	}
	return nil
}

// DigestDeclared replaces every declared credential constant in text by its digest placeholder, wherever it
// stands (a whole value, inside a header value, a path or free text), the longest constant first. It is idempotent (the placeholder holds
// no constant) and is what the harness applies to every recorded and compared text; a test that compares an
// answer itself (a Produce answer's program output) applies it to the answer of the Go plane with this method,
// so both planes go through one function.
func (g *Golden) DigestDeclared(text string) string {
	if len(g.spec.CredentialConstants) == 0 {
		return text
	}
	order := make([]int, len(g.spec.CredentialConstants))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return len(g.spec.CredentialConstants[order[a]]) > len(g.spec.CredentialConstants[order[b]])
	})
	for _, i := range order {
		value := g.spec.CredentialConstants[i]
		if strings.Contains(text, value) {
			text = strings.ReplaceAll(text, value, "<credential sha256:"+constantDigest(value)[:declaredPlaceholderHex]+">")
		}
	}
	return text
}

// declaredHeaderErr is an error when the frozen golden's declared-constant digests are not those the test declares.
func declaredHeaderErr(path string, header []string, constants []string) error {
	want := declaredDigests(constants)
	if len(header) == len(want) {
		same := true
		for i := range want {
			if header[i] != want[i] {
				same = false
			}
		}
		if same {
			return nil
		}
	}
	return fmt.Errorf("golden %s was recorded with %d declared credential constant(s), the test declares %d, and their digests differ: a changed, added or removed constant changes what the golden pins (GoldenSpec.CredentialConstants)", path, len(header), len(want))
}
