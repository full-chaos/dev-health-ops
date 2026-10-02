// Package goldenscan is the unpacked secret scan of the recorded goldens (CHAOS-7890): the CI secret scanner cannot read inside a
// packed ("gzip+base64:") body or a JSON text held in a string leaf, so the same walk the unpacked-secret-scan tool of record makes is
// done here, in Go, and wired into the record verb and `manifest -check`.
//
// What it finds is what the generic-api-key rule of gitleaks v8.21.2 finds and the token-shape scan of the venueoracle package does not
// (a credential word, a separator, then ten to 150 characters of a key, a token or a base64 text). The rule is a PORT of that scanner's
// own: the same expression (rule.go, copied from config/gitleaks.toml), the same filters in the same order (global allowlist regexes,
// the rule allowlist regex on the match, the rule's 1476 stopwords from stopwords.txt, entropy above 3.5, one digit from 1 to 9), and
// scanner_vectors.tsv holds the verdict of the real binary on a set of vectors, which the port must repeat. What it does not do: the
// scanner's own base64 and percent decoding of the text it reads (the walk here unpacks packed bodies and JSON texts instead).
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

// generic is the generic-api-key expression of gitleaks v8.21.2, exactly as config/gitleaks.toml has it.
var generic = regexp.MustCompile(`(?i)[\w.-]{0,10}?(?:access|auth|(?-i:[Aa]pi|API)|credential|creds|key|passwd|password|secret|token)(?:[ \t\w.-]{0,20})(?:[\s|']|[\s|"]){0,3}(?:=|>|:{1,3}=|\|\|:|<=|=>|:|\?=)(?:'|\"|\s|=|\x60){0,5}([\w.=-]{10,150})(?:['|\"|\n|\r|\s|\x60|;]|$)`)

// globalAllowRegexes are the regexes of the scanner's global allowlist (applied to the secret).
var globalAllowRegexes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^true|false|null$`),
	regexp.MustCompile(`^(?i:a+|b+|c+|d+|e+|f+|g+|h+|i+|j+|k+|l+|m+|n+|o+|p+|q+|r+|s+|t+|u+|v+|w+|x+|y+|z+|\*+|\.+)$`),
	regexp.MustCompile(`^\$(\d+|{\d+})$`),
	regexp.MustCompile(`^\$([A-Z_]+|[a-z_]+)$`),
	regexp.MustCompile(`^\${([A-Z_]+|[a-z_]+)}$`),
	regexp.MustCompile(`^\{\{[ \t]*[\w ().|]+[ \t]*}}$`),
	regexp.MustCompile(`^\$\{\{[ \t]*((env|github|secrets|vars)(\.[A-Za-z]\w+)+[\w "'&./=|]*)[ \t]*}}$`),
	regexp.MustCompile(`^%([A-Z_]+|[a-z_]+)%$`),
	regexp.MustCompile(`^%[+\-# 0]?[bcdeEfFgGoOpqstTUvxX]$`),
	regexp.MustCompile(`^\{\d{0,2}}$`),
	regexp.MustCompile(`^@([A-Z_]+|[a-z_]+)@$`),
}

// globalStopword is the one stopword of the scanner's global allowlist.
const globalStopword = "014df517-39d1-4453-b7b3-9930c563627c"

// ruleAllowRegex is the rule allowlist regex of generic-api-key, applied to the whole match.
var ruleAllowRegex = regexp.MustCompile(`(?i)(accessor|api[_.-]?(version|id)|rapid|capital|author|(?-i:(?:c|jobC)redentials?Id|withCredentials)|key[_.-]?(alias|board|code|ring|stone|storetype|word|up|down|left|right)|issuerkeyhash|(bucket|primary|foreign|natural|hot)[_.-]?key|(?-i:[DdMm]onkey|[DM]ONKEY)|keying|(secret)[_.-]?name|public[_.-]?(key|token)|(key|token)[_.-]?file)`)

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

func hasStopword(rules []string, secret string) bool {
	lower := strings.ToLower(secret)
	for _, word := range rules {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

func containsDigit(secret string) bool { return strings.ContainsAny(secret, "123456789") }

// secretOf is the secret of one match of generic, as the scanner finds it: the match without its newlines, the expression run again on
// it, and the first group.
func secretOf(match string) (full, secret string) {
	full = strings.Trim(match, "\n")
	secret = full
	if groups := generic.FindStringSubmatch(full); len(groups) >= 2 {
		secret = groups[1]
	}
	return full, secret
}

// accepted is every filter the scanner runs on a match, in its order: the global allowlist, the rule allowlist (regex on the match,
// stopwords on the secret), entropy above the bound, and a digit from 1 to 9.
func accepted(full, secret string) bool {
	for _, re := range globalAllowRegexes {
		if re.MatchString(secret) {
			return false
		}
	}
	if strings.Contains(strings.ToLower(secret), globalStopword) {
		return false
	}
	if ruleAllowRegex.MatchString(full) || hasStopword(ruleStopwords, secret) {
		return false
	}
	return entropy(secret) > entropyMax && containsDigit(secret)
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
	for _, match := range matchesIn(text) {
		if full, secret := secretOf(match); accepted(full, secret) {
			secrets = append(secrets, secret)
		}
	}
	return secrets
}

// keywords are the credential words of the rule, lower case: a match holds one of them, whatever its case (the case-sensitive api
// form is a subset of the case-insensitive one).
var keywords = []string{"access", "auth", "api", "credential", "creds", "key", "passwd", "password", "secret", "token"}

const (
	windowBefore = 10 + 5                        // the lazy prefix of the rule, and room
	windowAfter  = 20 + 3 + 3 + 5 + 150 + 1 + 60 // gap, quotes, separator, secret prefix, the longest secret, its terminator, and room
)

// matchesIn is every match of generic over text. The expression is slow over a long text, and most of a golden holds no credential
// word, so for an ASCII text it is run only over a window around each keyword (every match holds one); a text with a non-ASCII byte is
// run whole, because the expression's case folding reaches beyond ASCII. A differential test holds this equal to the expression over
// the whole text.
func matchesIn(text string) []string {
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
		return generic.FindAllString(text, -1)
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
	var matches []string
	for _, window := range merged {
		matches = append(matches, generic.FindAllString(text[window[0]:window[1]], -1)...)
	}
	return matches
}

// hasSegmentRun reports whether text holds twelve characters of [A-Za-z0-9_.=-] in a row. It is necessary for a hit (a secret above
// the entropy bound has at least twelve different characters, so at least twelve), so a text without it is not run through the
// expression at all; a differential test holds the result equal to the whole-text expression for every text.
func hasSegmentRun(text string) bool {
	run := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '=' || c == '-' || c >= 0x80 {
			if run++; run >= 12 {
				return true
			}
			continue
		}
		run = 0
	}
	return false
}
