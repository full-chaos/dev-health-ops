// Package goldenscan is the unpacked secret scan of the recorded goldens (CHAOS-7890): the CI secret scanner cannot read inside a
// packed ("gzip+base64:") body or a JSON text held in a string leaf, so the same walk the unpacked-secret-scan tool of record makes is
// done here, in Go, and wired into the record verb and `manifest -check`.
//
// What it finds is the shape the keyword rule of that scanner finds and the token-shape scan of the venueoracle package does not: a
// leaf whose KEY names a credential (access, auth, api, credential, creds, key, passw, secret, token) and whose value is a long
// string of high entropy. Parity with the scanner's own rule is by test, not by identity.
//
// A hit passes only through a row of allowlist.tsv: ONE row per golden file and key, for a digest or id OUTPUT field the triage record
// accepts, with the exact value shape, the exact count and the triage line it cites. There is no wildcard path and no rule-wide row,
// and a row that matches fewer or more hits than it pins is itself a failure, so the list only shrinks.
package goldenscan

import (
	"bytes"
	"compress/gzip"
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
	var doc any
	if err := json.Unmarshal(golden, &doc); err != nil {
		return nil, fmt.Errorf("golden is not JSON: %w", err)
	}
	var leaves []Leaf
	if err := walk(doc, "", 0, &leaves); err != nil {
		return nil, err
	}
	return leaves, nil
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
			var inner any
			if json.Unmarshal([]byte(trimmed), &inner) == nil {
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

// generic is the keyword rule of the scanner (its generic-api-key rule) over the text of a leaf rendered as `"<key>": "<value>"`:
// a credential word, a separator, then a value of ten or more characters of a key, a token or a base64 text. The key of a leaf, or a
// `name=value` inside a text leaf, or a typed leaf whose own text starts with `api:` all match, as they do in the scanner.
var generic = regexp.MustCompile(`(?i)[\w.-]{0,50}?(?:access|auth|(?-i:[Aa]pi|API)|credential|creds|key|passw(?:or)?d|secret|token)(?:[ \t\w.-]{0,20})[\s'"]{0,3}(?:=|>|:{1,3}=|\|\||:|=>|\?=|,)[\x60'"\s=]{0,5}([\w.=-]{10,150}|[a-z0-9][a-z0-9+/]{11,}={0,3})(?:[\x60'"\s;]|\\[nr]|$)`)

// entropyMin is the Shannon entropy per character, in bits, from which a value is a hit (the scanner's own bound).
const entropyMin = 3.5

// Hit is one leaf the rule finds: the key and a shape of the value, never the value.
type Hit struct {
	Key   string
	Shape string
	Value string // kept for the allowlist match only; never printed
}

var (
	uuidShape = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	hex64     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Shape names the shape of a value for the allowlist: uuid, hex64, or other.
func Shape(v string) string {
	switch {
	case uuidShape.MatchString(v):
		return "uuid"
	case hex64.MatchString(v):
		return "hex64"
	}
	return "other"
}

func entropy(v string) float64 {
	counts := map[rune]int{}
	total := 0
	for _, r := range v {
		counts[r]++
		total++
	}
	var bits float64
	for _, n := range counts {
		p := float64(n) / float64(total)
		bits -= p * math.Log2(p)
	}
	return bits
}

// stopwords drops a value that names itself a placeholder (the scanner drops values that hold a word of its stopword list; this
// is the short part of that list that matters for test data). A value that holds one is not a credential somebody left behind.
var stopwords = []string{"test", "example", "sample", "fake", "dummy", "mock", "placeholder", "synthetic", "changeme", "password", "secret", "token", "demo"}

// separators split a value into the segments an identifier is made of.
var separators = regexp.MustCompile(`[_.:/ -]+`)

// randomLooking is false for an identifier made of short words and numbers (snake_case, kebab-case, a name with a counter): no
// segment of it is long enough, or mixed enough, to be a generated secret.
func randomLooking(v string) bool {
	for _, segment := range separators.Split(v, -1) {
		if len(segment) >= 20 {
			return true
		}
		if len(segment) >= 12 && strings.ContainsAny(segment, "0123456789") && strings.ContainsAny(segment, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			return true
		}
	}
	return false
}

func hasStopword(v string) bool {
	lower := strings.ToLower(v)
	for _, word := range stopwords {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

// accepted is the filter on a capture: enough entropy, random-looking, no placeholder word.
func accepted(captured string) bool {
	return entropy(captured) >= entropyMin && randomLooking(captured) && !hasStopword(captured)
}

// Hits is the values the rule finds in the leaves, in leaf order. A hit carries the key of its leaf.
func Hits(leaves []Leaf) []Hit {
	var hits []Hit
	// a golden repeats the same leaf thousands of times (one fixture row per case): the captures of a leaf are found once
	found := map[Leaf][]string{}
	for _, leaf := range leaves {
		captures, ok := found[leaf]
		if !ok {
			for _, captured := range genericCaptures(`"` + leaf.Key + `": "` + leaf.Value + `"`) {
				if accepted(captured) {
					captures = append(captures, captured)
				}
			}
			found[leaf] = captures
		}
		for _, captured := range captures {
			hits = append(hits, Hit{Key: leaf.Key, Shape: Shape(captured), Value: captured})
		}
	}
	return hits
}

// keywords are the credential words of the rule, lower case: a match holds one of them, whatever its case (the case-sensitive api
// form is a subset of the case-insensitive one).
var keywords = []string{"access", "auth", "api", "credential", "creds", "key", "passw", "secret", "token"}

const (
	windowBefore = 50                    // the lazy prefix of the rule
	windowAfter  = 20 + 3 + 5 + 150 + 60 // gap, quotes, separator run, the longest capture, and room for its terminator
)

// genericCaptures is every capture of generic over text. The expression is slow over a long text, and most of a golden holds no
// credential word, so for an ASCII text it is run only over a window around each keyword (every match holds one); a text with a
// non-ASCII byte is run whole, because the expression's case folding reaches beyond ASCII. A differential test holds this equal to
// the expression over the whole text.
func genericCaptures(text string) []string {
	var captures []string
	if !hasSegmentRun(text) {
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
		for _, match := range generic.FindAllStringSubmatch(text, -1) {
			captures = append(captures, match[1])
		}
		return captures
	}
	lowered := strings.ToLower(text)
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
			// the second value form has no upper length: the window runs to the end of the run of value characters
			for end < len(text) && (text[end] == '+' || text[end] == '/' || text[end] == '=' || text[end] == '.' || text[end] == '-' || text[end] == '_' || text[end] >= '0' && text[end] <= '9' || text[end] >= 'a' && text[end] <= 'z' || text[end] >= 'A' && text[end] <= 'Z') {
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
	for _, window := range merged {
		for _, match := range generic.FindAllStringSubmatch(text[window[0]:window[1]], -1) {
			captures = append(captures, match[1])
		}
	}
	return captures
}

// hasSegmentRun reports whether text holds twelve letters, digits, plus or equals signs in a row. It is necessary for a hit (the
// shortest random-looking value holds such a segment), so a text without it is not run through the expression at all; the capture of
// a hit is filtered afterwards by hits, and a differential test holds genericCaptures equal to the expression for every text that
// has one.
func hasSegmentRun(text string) bool {
	run := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '=' || c >= 0x80 {
			if run++; run >= 12 {
				return true
			}
			continue
		}
		run = 0
	}
	return false
}
