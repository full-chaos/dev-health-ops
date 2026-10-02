package venueoracle

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// A credential constant a test sends on purpose (a push token, a password the CLI puts in a header) is the same
// value in every run, so a Scrub that blanks it is refused (the value would stop being compared), and a golden
// that stores it holds a token-shaped value. GoldenSpec.CredentialConstants declares such values by exact text
// (CHAOS-7898): the harness, not the test, replaces each by `<credential sha256:<12 hex>>` in every recorded
// request key, answer, header and row, on the Python plane while recording and on the compared plane alike
// (Golden.DigestDeclared for an answer a test compares itself), so the VALUE stays compared (as its digest) and
// is never stored. The header holds the full sha256 of each declared constant under a name no secret scanner
// reads as a key (declared_constants_sha256): a frozen run refuses a test that declares other constants, and the
// record verb refuses a candidate that still holds a declared constant raw, and a declaration that occurs nowhere.
//
// A declared constant is refused raw AND in these decoded forms (D4196): inside a base64 token (standard and URL
// alphabets, padded or not: a Basic header value, an SMTP AUTH PLAIN payload, base64url), URL-escaped (query and
// path escaping) and JSON-escaped (one and two string levels). The record verb refuses such a candidate and names the
// form: the test then declares the encoded text it sends as its own constant, which is replaced and pinned like any
// other. Exact values only, no pattern. Any OTHER transform of a constant (hex, a hash, a cipher, a split across two
// JSON strings) is NOT covered: the scan of record is the net.

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
	g.declaredMu.Lock()
	defer g.declaredMu.Unlock()
	if g.declaredSeen == nil {
		g.declaredSeen = make([]int, len(g.spec.CredentialConstants))
	}
	for _, i := range order {
		value := g.spec.CredentialConstants[i]
		if n := strings.Count(text, value); n > 0 {
			g.declaredSeen[i] += n
			text = strings.ReplaceAll(text, value, "<credential sha256:"+constantDigest(value)[:declaredPlaceholderHex]+">")
		}
	}
	return text
}

// declaredStaleErr is an error when a declared constant was never replaced in this recording: a declaration
// nothing needs (the test stopped sending it) pins nothing.
func (g *Golden) declaredStaleErr() error {
	g.declaredMu.Lock()
	defer g.declaredMu.Unlock()
	for index := range g.spec.CredentialConstants {
		if g.declaredSeen == nil || g.declaredSeen[index] == 0 {
			return fmt.Errorf("recording %s: declared credential constant #%d never occurs in the recording (GoldenSpec.CredentialConstants): a stale declaration pins nothing; remove it", g.spec.Path, index)
		}
	}
	return nil
}

// declaredLeakErr is an error when the candidate, with every packed body unpacked, still holds a declared
// constant raw. It names the constant by position and the count, never the value.
func declaredLeakErr(path string, raw []byte, constants []string) error {
	if len(constants) == 0 {
		return nil
	}
	var doc any
	text := string(raw)
	var leafTexts []string
	if err := json.Unmarshal(raw, &doc); err == nil {
		var all strings.Builder
		var walk func(any)
		walk = func(node any) {
			switch value := node.(type) {
			case string:
				all.WriteString(value)
				all.WriteByte('\n')
				leafTexts = append(leafTexts, value)
				if strings.HasPrefix(value, packedPrefix) {
					if inner, err := unpackBody(value); err == nil {
						all.WriteString(inner)
						all.WriteByte('\n')
						leafTexts = append(leafTexts, inner)
					}
				}
			case []any:
				for _, item := range value {
					walk(item)
				}
			case map[string]any:
				for _, item := range value {
					walk(item)
				}
			}
		}
		walk(doc)
		text += "\n" + all.String()
	}
	var leaked []string
	for index, value := range constants {
		if n := strings.Count(text, value); n > 0 {
			leaked = append(leaked, fmt.Sprintf("#%d (%d times)", index, n))
		}
	}
	if len(leaked) > 0 {
		return fmt.Errorf("recording %s: the candidate still holds declared credential constant(s) %s raw: the digest scrub missed them (a text the projection does not reach); no token-shaped value is stored", path, strings.Join(leaked, ", "))
	}
	decoded := map[string]bool{}
	for _, leaf := range leafTexts {
		for _, hit := range declaredDecodedLeaks(leaf, constants) {
			decoded[hit] = true
		}
	}
	if len(decoded) > 0 {
		var hits []string
		for hit := range decoded {
			hits = append(hits, hit)
		}
		sort.Strings(hits)
		return fmt.Errorf("recording %s: the candidate holds declared credential constant(s) %s in a decoded form: declare the encoded text the test sends as its own constant (it is then replaced and pinned like any other); no token-shaped value is stored", path, strings.Join(hits, ", "))
	}
	return nil
}

// declaredHeaderErr is an error when the frozen golden's declared-constant digests are not those the test declares.
func declaredHeaderErr(path string, header []string, constants []string, recipe string) error {
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
	return fmt.Errorf("golden %s was recorded with %d declared credential constant(s), the test declares %d, and their digests differ: a changed, added or removed constant changes what the golden pins (GoldenSpec.CredentialConstants); regenerate: %s", path, len(header), len(want), recipe)
}

// base64 tokens are taken per alphabet: the standard one holds '/' and '+', the URL one '-' and '_', and a token of
// one alphabet is cut by the characters of the other (a path "/callback/" + base64url + "/done" is cut at its slashes).
var (
	base64StdToken = regexp.MustCompile(`[A-Za-z0-9+/]{8,}={0,2}`)
	base64URLToken = regexp.MustCompile(`[A-Za-z0-9_-]{8,}={0,2}`)
)

// base64Alignments is how many leading characters of a token are tried as the start of the encoded text: base64 reads
// four characters at a time, so a token that follows characters of its own alphabet (a path, a word) starts at one of
// four offsets, and only one of them decodes the constant.
const base64Alignments = 4

// declaredDecodedLeaks names the declared constants that text holds in a decoded form: inside a base64 token (either
// alphabet, any padding, any alignment), URL-escaped (any hex case, any set of escaped characters: the text is
// unescaped, not searched for one encoder's spelling) and JSON-escaped (one and two levels). Other transforms (double
// base64, hex) are NOT covered.
func declaredDecodedLeaks(text string, constants []string) []string {
	var found []string
	note := func(index int, form string) { found = append(found, fmt.Sprintf("#%d (%s)", index, form)) }
	// The text is compared as DECODED values, not searched for one spelling of the constant: the JSON escapes in it
	// (\uXXXX, \/, \", and the rest) are resolved, once and twice (a JSON text inside a JSON string), and each
	// resulting text goes through the URL decoder and the base64 alphabets.
	variants := []string{text}
	for level := 0; level < 2; level++ {
		next := jsonUnescapedText(variants[len(variants)-1])
		if next == variants[len(variants)-1] {
			break
		}
		variants = append(variants, next)
	}
	for level, variant := range variants {
		if level > 0 {
			for index, value := range constants {
				if strings.Contains(variant, value) {
					note(index, "JSON-escaped")
				}
			}
		}
		for _, tokens := range []struct {
			tokens   []string
			encoding *base64.Encoding
		}{
			{base64StdToken.FindAllString(variant, -1), base64.RawStdEncoding},
			{base64URLToken.FindAllString(variant, -1), base64.RawURLEncoding},
		} {
			for _, token := range tokens.tokens {
				// Padding is dropped first: the raw decoders read a padded token too once its '=' are gone.
				trimmed := strings.TrimRight(token, "=")
				for drop := 0; drop < base64Alignments && drop < len(trimmed); drop++ {
					part := trimmed[drop:]
					if len(part)%4 == 1 {
						part = part[:len(part)-1] // a lone last character carries no byte
					}
					decoded := decodeBase64(tokens.encoding, part)
					if decoded == nil {
						continue
					}
					for index, value := range constants {
						if strings.Contains(string(decoded), value) {
							note(index, "base64")
						}
					}
				}
			}
		}
		for _, candidate := range []string{percentDecoded(variant, false), percentDecoded(variant, true)} {
			if candidate == variant {
				continue
			}
			for index, value := range constants {
				if strings.Contains(candidate, value) {
					note(index, "URL-escaped")
				}
			}
		}
	}
	return found
}

// jsonUnescapedText is text with every valid JSON escape resolved (\" \\ \/ \b \f \n \r \t and \uXXXX, a surrogate
// pair as one rune), wherever it stands: the text is not parsed, so it may be a fragment of JSON. An invalid escape
// stays as it is.
func jsonUnescapedText(text string) string {
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c != '\\' || i+1 >= len(text) {
			out.WriteByte(c)
			continue
		}
		switch next := text[i+1]; next {
		case '"', '\\', '/':
			out.WriteByte(next)
			i++
		case 'b':
			out.WriteByte('\b')
			i++
		case 'f':
			out.WriteByte('\f')
			i++
		case 'n':
			out.WriteByte('\n')
			i++
		case 'r':
			out.WriteByte('\r')
			i++
		case 't':
			out.WriteByte('\t')
			i++
		case 'u':
			r, width := jsonRuneEscape(text[i:])
			if width == 0 {
				out.WriteByte(c)
				continue
			}
			out.WriteRune(r)
			i += width - 1
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

// jsonRuneEscape reads \uXXXX (and a following \uXXXX low surrogate) at the start of text: the rune and the bytes used,
// or width 0 when text does not start with a valid escape.
func jsonRuneEscape(text string) (rune, int) {
	hex4 := func(at int) (rune, bool) {
		if len(text) < at+6 || text[at] != '\\' || text[at+1] != 'u' {
			return 0, false
		}
		var r rune
		for _, c := range []byte(text[at+2 : at+6]) {
			if !isHex(c) {
				return 0, false
			}
			r = r<<4 | rune(unhex(c))
		}
		return r, true
	}
	first, ok := hex4(0)
	if !ok {
		return 0, 0
	}
	if first >= 0xD800 && first < 0xDC00 {
		if second, ok := hex4(6); ok && second >= 0xDC00 && second < 0xE000 {
			return 0x10000 + (first-0xD800)<<10 + (second - 0xDC00), 12
		}
		return utf8.RuneError, 6
	}
	return first, 6
}

// percentDecoded is text with every valid %XX (either hex case) replaced by its byte, whatever encoder wrote it; an
// invalid or truncated escape stays as it is. plusIsSpace also reads '+' as a space (the form encoding of a query).
func percentDecoded(text string, plusIsSpace bool) string {
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '%' && i+2 < len(text) && isHex(text[i+1]) && isHex(text[i+2]):
			out.WriteByte(unhex(text[i+1])<<4 | unhex(text[i+2]))
			i += 2
		case c == '+' && plusIsSpace:
			out.WriteByte(' ')
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func unhex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	}
	return c - 'A' + 10
}

func decodeBase64(encoding *base64.Encoding, text string) []byte {
	decoded, err := encoding.DecodeString(text)
	if err != nil {
		return nil
	}
	return decoded
}

// jsonEscaped is value as it stands inside a JSON string (without the quotes).
func jsonEscaped(value string) string {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) < 2 {
		return value
	}
	return string(raw[1 : len(raw)-1])
}
