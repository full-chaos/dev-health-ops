// Package goldenscan is the unpacked secret scan of the recorded goldens (CHAOS-7890): the CI secret scanner cannot read inside a
// packed ("gzip+base64:") body or a JSON text held in a string leaf, so the same walk the unpacked-secret-scan tool of record makes is
// done here, in Go, and wired into the record verb and `manifest -check`.
//
// What it finds is what the generic-api-key rule of gitleaks v8.24.3 finds and the token-shape scan of the venueoracle package does not
// (a credential word, a separator, then ten to 150 characters of a key, a token or a base64 text). The rule is a PORT of that scanner's
// own (rule expression and allowlists in rule_gen.go, copied from the release config; stopwords in stopwords.txt; the filters in goldenscan.go), and the parity claim is exactly this: on every vector of
// testdata/scanner_vectors.tsv, whose verdicts were recorded from the REAL binary, the port reports a hit wherever the scanner does
// (Go-hit is a superset of gitleaks-hit; the port may be stricter on a vector, never looser). Beyond the vectors parity is not proved. What
// the port does not do: the scanner's own base64 and percent decoding of the text it reads (the walk unpacks packed bodies and JSON
// texts instead).
//
// A hit passes only through a row of allowlist.tsv: ONE row per golden file and key, for a digest or id OUTPUT field the triage record
// accepts, with the exact value shape, the exact count and the triage line it cites. There is no wildcard path and no rule-wide row,
// and a row that matches fewer or more hits than it pins is itself a failure, so the list only shrinks.
package goldenscan

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
)

const (
	packedPrefix = "gzip+base64:"
	maxDepth     = 8
	maxUnpacked  = 256 << 20
)

// Leaf is one string leaf of a golden with the key it sits under (a list element takes its list's key).
type Leaf struct{ Key, Value string }

// Leaves is every string leaf of the golden, each as its own unit with its key. A packed body is unpacked first; a leaf (packed or not)
// that is itself JSON text of an object or a list is walked the same way, leaf by leaf; a packed body that is not JSON is one leaf.
func Leaves(golden []byte) ([]Leaf, error) {
	doc, err := decodeJSON(golden)
	if err != nil {
		return nil, fmt.Errorf("golden is not JSON: %w", err)
	}
	var leaves []Leaf
	if err := walk(doc, "", 0, &leaves); err != nil {
		return nil, err
	}
	return leaves, nil
}

// decodeJSON reads one JSON document with numbers kept as text (a number too large for a float is still a number).
func decodeJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var doc any
	if err := decoder.Decode(&doc); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, fmt.Errorf("text after the JSON value")
	}
	return doc, nil
}

func walk(node any, key string, depth int, out *[]Leaf) error {
	if depth > maxDepth {
		return fmt.Errorf("a golden nests JSON texts more than %d deep, so it cannot be scanned", maxDepth)
	}
	switch value := node.(type) {
	case string:
		text := value
		if strings.HasPrefix(text, packedPrefix) {
			unpacked, err := unpack(text)
			if err != nil {
				return fmt.Errorf("a packed body under key %q does not unpack, so it cannot be scanned: %w", key, err)
			}
			text = unpacked
		}
		if trimmed := strings.TrimSpace(text); strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			if inner, err := decodeJSON([]byte(trimmed)); err == nil {
				return walk(inner, key, depth+1, out)
			}
		}
		*out = append(*out, Leaf{Key: key, Value: text})
	case []any:
		for _, item := range value {
			if err := walk(item, key, depth, out); err != nil {
				return err
			}
		}
	case map[string]any:
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := walk(value[k], k, depth, out); err != nil {
				return err
			}
		}
	}
	return nil
}

func unpack(body string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(body, packedPrefix))
	if err != nil {
		return "", err
	}
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	defer reader.Close()
	text, err := io.ReadAll(io.LimitReader(reader, maxUnpacked+1))
	if err != nil {
		return "", err
	}
	if len(text) > maxUnpacked {
		return "", fmt.Errorf("a packed body is larger than %d bytes", maxUnpacked)
	}
	return string(text), nil
}

//go:embed stopwords.txt
var stopwordsText string

var ruleStopwords = func() []string {
	var words []string
	for _, line := range strings.Split(stopwordsText, "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			words = append(words, strings.ToLower(line))
		}
	}
	return words
}()

// entropyMax is the rule's entropy: a secret whose entropy is at or below it is dropped.
const entropyMax = 3.5

// Hit is one leaf the rule finds: the key and a shape of the value, never the value.
type Hit struct {
	Key   string
	Shape string
	Value string // kept for the allowlist match only; never printed
}

var (
	uuidShape = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	hex64     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	hex32     = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// Shape names the shape of a value for the allowlist: uuid, hex64, hex32, or other.
func Shape(v string) string {
	switch {
	case uuidShape.MatchString(v):
		return "uuid"
	case hex64.MatchString(v):
		return "hex64"
	case hex32.MatchString(v):
		return "hex32"
	}
	return "other"
}

// entropy is the Shannon entropy as the scanner computes it (per rune, over the byte length).
func entropy(data string) float64 {
	if data == "" {
		return 0
	}
	counts := map[rune]int{}
	for _, char := range data {
		counts[char]++
	}
	invLength := 1.0 / float64(len(data))
	var bits float64
	for _, count := range counts {
		frequency := float64(count) * invLength
		bits -= frequency * math.Log2(frequency)
	}
	return bits
}

func hasStopword(words []string, secret string) bool {
	lower := strings.ToLower(secret)
	for _, word := range words {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

func anyMatch(res []*regexp.Regexp, target string) bool {
	for _, re := range res {
		if re.MatchString(target) {
			return true
		}
	}
	return false
}

// secretOf is the secret of one match of generic, as the scanner finds it: the match without its newlines, the expression run again on
// it, and the first non-empty group.
func secretOf(match string) (full, secret string) {
	full = strings.Trim(match, "\n")
	secret = full
	if groups := generic.FindStringSubmatch(full); len(groups) >= 2 {
		for _, group := range groups[1:] {
			if len(group) > 0 {
				secret = group
				break
			}
		}
	}
	return full, secret
}

// accepted is every filter the scanner runs on a match, in its order: entropy above the bound, the global allowlist (regexes and
// stopwords on the secret), then the rule's allowlists: a regex on the secret; a regex on the whole match or a stopword in the secret;
// a regex on the line of the match. (The fourth rule allowlist needs a Yocto recipe path and never applies to a golden.) line is the
// line of text that holds the match.
func accepted(full, secret, line string) bool {
	if entropy(secret) <= entropyMax {
		return false
	}
	if anyMatch(globalAllowRegexes, secret) || hasStopword(globalStopwords, secret) {
		return false
	}
	if anyMatch(ruleAllowSecret, secret) {
		return false
	}
	if anyMatch(ruleAllowMatch, full) || hasStopword(ruleStopwords, secret) {
		return false
	}
	return !anyMatch(ruleAllowLine, line)
}

// Hits is the secrets the rule finds in the leaves, in leaf order. A hit carries the key of its leaf.
func Hits(leaves []Leaf) []Hit {
	var hits []Hit
	// a golden repeats the same leaf thousands of times (one fixture row per case): the secrets of a leaf are found once
	found := map[Leaf][]string{}
	for _, leaf := range leaves {
		secrets, ok := found[leaf]
		if !ok {
			secrets = SecretsIn(`"` + leaf.Key + `": "` + leaf.Value + `"`)
			found[leaf] = secrets
		}
		for _, secret := range secrets {
			hits = append(hits, Hit{Key: leaf.Key, Shape: Shape(secret), Value: secret})
		}
	}
	return hits
}

// SecretsIn is the secrets the scanner would report in text (one unit, as the tool of record's per-leaf wrapper writes it).
func SecretsIn(text string) []string {
	var secrets []string
	for _, span := range matchSpans(text) {
		full, secret := secretOf(text[span[0]:span[1]])
		if accepted(full, secret, lineOf(text, span[0], span[1])) {
			secrets = append(secrets, secret)
		}
	}
	return secrets
}

// lineOf is the lines of text the match [start, end) touches, as the scanner takes them (from the start of the first line to the end of
// the last).
func lineOf(text string, start, end int) string {
	from := strings.LastIndexByte(text[:start], '\n') + 1
	to := len(text)
	if at := strings.IndexByte(text[end:], '\n'); at >= 0 {
		to = end + at
	}
	return text[from:to]
}

// keywords are the credential words of the rule, lower case: a match holds one of them, whatever its case.
var keywords = []string{"access", "api", "auth", "key", "credential", "creds", "passwd", "password", "secret", "token"}

const (
	windowBefore = 50 + 10                       // the lazy prefix of the rule, and room
	windowAfter  = 20 + 3 + 3 + 5 + 150 + 1 + 60 // gap, quotes, separator, secret prefix, the longest bounded secret, its terminator, and room
)

// matchSpans is every match of generic over text (start and end offsets). The expression is slow over a long text, and most of a golden
// holds no credential word, so for an ASCII text it is run only over a window around each keyword (every match holds one; a window runs
// on to the end of the run of secret characters, because the second secret form has no upper length); a text with a non-ASCII byte is
// run whole, because the expression's case folding reaches beyond ASCII. A differential test holds this equal to the expression over
// the whole text.
func matchSpans(text string) [][2]int {
	if !hasSegmentRun(text) {
		return nil
	}
	// the scanner runs a rule only on text whose lower-cased form holds one of the rule's keywords (a long s is not folded there)
	lowered := strings.ToLower(text)
	found := false
	for _, word := range keywords {
		if strings.Contains(lowered, word) {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	ascii := true
	for i := 0; i < len(text); i++ {
		if text[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if !ascii {
		var spans [][2]int
		for _, loc := range generic.FindAllStringIndex(text, -1) {
			spans = append(spans, [2]int{loc[0], loc[1]})
		}
		return spans
	}
	var windows [][2]int
	for _, word := range keywords {
		for from := 0; ; {
			at := strings.Index(lowered[from:], word)
			if at < 0 {
				break
			}
			at += from
			from = at + 1
			start, end := at-windowBefore, at+len(word)+windowAfter
			if start < 0 {
				start = 0
			}
			if end > len(text) {
				end = len(text)
			}
			for end < len(text) && isSecretByte(text[end]) {
				end++
			}
			if end < len(text) {
				end++ // its terminator
			}
			windows = append(windows, [2]int{start, end})
		}
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i][0] < windows[j][0] })
	var merged [][2]int
	for _, window := range windows {
		if n := len(merged); n > 0 && window[0] <= merged[n-1][1] {
			if window[1] > merged[n-1][1] {
				merged[n-1][1] = window[1]
			}
			continue
		}
		merged = append(merged, window)
	}
	var spans [][2]int
	for _, window := range merged {
		for _, loc := range generic.FindAllStringIndex(text[window[0]:window[1]], -1) {
			spans = append(spans, [2]int{window[0] + loc[0], window[0] + loc[1]})
		}
	}
	return spans
}

// isSecretByte is a character of either secret form: [\w.=-] or the base64 alphabet.
func isSecretByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '=' || c == '-' || c == '+' || c == '/'
}

// hasSegmentRun reports whether text holds twelve secret characters in a row. It is necessary for a hit (a secret above the entropy
// bound has at least twelve different characters, so at least twelve), so a text without it is not run through the expression at all.
func hasSegmentRun(text string) bool {
	run := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if isSecretByte(c) || c >= 0x80 {
			if run++; run >= 12 {
				return true
			}
			continue
		}
		run = 0
	}
	return false
}
