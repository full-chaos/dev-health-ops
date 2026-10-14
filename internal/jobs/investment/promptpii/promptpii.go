// Package promptpii is the one deterministic filter between synced work-item,
// pull-request and commit text and the investment categorization prompt. It
// replaces credential-shaped tokens, URLs with userinfo, e-mail addresses,
// phone numbers and @-mentions before the text bundle is built, so the prompt,
// the bundle hash and every quote the model returns are all made from the same
// filtered text.
//
// Named limits: a credential written as a plain word with no digit ("the
// password is hunterpass") reads like "the password is invalid" and stays; a
// package pin whose tag is not a known release channel or a "v" version
// (name@somebranch) is read as an address; and a real address whose dotless host
// is a release channel or version (alice@latest) cannot be told from a pin and
// stays.
package promptpii

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// credentialMarker is what platform/logging puts where it hides a credential.
const credentialMarker = "[REDACTED]"

// Counts is how many values of each class a filter replaced. It never holds a
// value.
type Counts struct {
	Credentials int
	Emails      int
	Phones      int
	Mentions    int
}

// Total is the number of replaced values.
func (c Counts) Total() int { return c.Credentials + c.Emails + c.Phones + c.Mentions }

// Add returns the sum of two counts.
func (c Counts) Add(o Counts) Counts {
	return Counts{
		Credentials: c.Credentials + o.Credentials,
		Emails:      c.Emails + o.Emails,
		Phones:      c.Phones + o.Phones,
		Mentions:    c.Mentions + o.Mentions,
	}
}

var (
	// A dotless host ("alice@localhost") is an address too. The host starts with a
	// letter so "react@18.3.1" stays a package pin.
	emailPattern = regexp.MustCompile(`[\p{L}\p{N}._%+'-]+@\p{L}[\p{L}\p{N}-]*(?:\.[\p{L}\p{N}-]+)*`)

	// An explicit assignment of a credential label: "password: x", "token=x".
	labeledSecretPattern = regexp.MustCompile(`(?i)\b(passwords?|passwd|passphrase|pwd|secret|api[_ -]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|token)(\s*[:=]\s*)("[^"\n]+"|'[^'\n]+'|[^\s"',;]+)`)

	// An Authorization header value, with or without a scheme word, and a
	// bearer token written in prose (which must hold a digit, so "bearer of
	// bad news" stays).
	authorizationPattern = regexp.MustCompile(`(?i)\b(authorization\s*[:=]\s*(?:(?:bearer|basic|token)\s+)?)([^\s"',;]+)`)
	bearerPattern        = regexp.MustCompile(`(?i)\b(bearer\s+)([^\s"',;]*\d[^\s"',;]*)`)

	// A credential written in prose behind filler words, with no separator:
	// "the api key is abcd1234", "password was set to Tr0ub4dor3". The value is
	// checked by proseSecretShaped; without a digit it is a word ("the token is
	// invalid") and stays.
	proseSecretPattern = regexp.MustCompile(`(?i)\b(passwords?|passwd|pwd|passphrase|secret|api[ _-]?keys?|access[ _-]?token|auth[ _-]?token|client[ _-]?secret|credentials?|token)((?:[ \t]+(?:is|was|are|set|to|equals?))+[ \t:]+["'` + "`" + `]?)([^\s"'` + "`" + `<>,;()\[\]{}]{8,})`)
	issueKeyPattern    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*-\d+$`)
	versionPattern     = regexp.MustCompile(`^[vV]?\d+(?:\.\d+)+(?:[-+][A-Za-z0-9.+-]+)?$`)
	datePattern        = regexp.MustCompile(`^(?:\d{4}[-/]\d{2}[-/]\d{2}(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}(?::?\d{2})?)?)?|\d{2}[-/.]\d{2}[-/.]\d{4}|\d{1,2}:\d{2}(?::\d{2})?)$`)

	// The host part of a package@tag pin: a release channel or a "v"-version,
	// optionally followed by digit-only parts ("beta.3", "next-1042", "v18.3.1").
	// A host with a name part ("beta.example.com") is still an address.
	packageNamePattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)
	packageTagPattern  = regexp.MustCompile(`(?i)^(?:(?:latest|next|beta|alpha|canary|nightly|rc|lts|insiders|experimental|preview|snapshot)(?:[.-]\d+)*|v\d+(?:[.-]\d+)*(?:-(?:alpha|beta|rc)(?:[.-]\d+)*)?|sha(?:256|512))$`)

	// Jira wiki mentions: [~username] and [~accountid:...].
	jiraMentionPattern = regexp.MustCompile(`\[~[^\]\s]{1,128}\]`)

	// GitHub, GitLab, Linear and Slack style mentions. RE2 has no lookbehind, so
	// the character before the "@" is captured and written back.
	mentionPattern = regexp.MustCompile(`(^|[^A-Za-z0-9_@.\]])@[A-Za-z0-9](?:[A-Za-z0-9_-]|\.[A-Za-z0-9])*(?:/[A-Za-z0-9_-]+)?`)

	// A phone number needs a leading "+" or the separators of a national format.
	// Other digit runs (an issue number, a timestamp) are not matched, except a
	// run of exactly ten digits (see digitRunPattern).
	phonePatterns = []*regexp.Regexp{
		regexp.MustCompile(`\+\d{1,3}(?:[\s.-]?\(?\d{1,4}\)?){2,5}`),
		regexp.MustCompile(`\(?\b\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}\b`),
		regexp.MustCompile(`\b0\d{2,4}[\s.-]\d{3,4}[\s.-]\d{3,4}\b`),
		regexp.MustCompile(`\(\d{1,4}\)[\s.-]?\d{3,4}[\s.-]\d{3,4}\b`),
		regexp.MustCompile(`\b0\d(?:[\s.-]\d{2}){4}\b`),
	}

	// A whole run of digits is a plain phone number when it is exactly ten
	// digits long. The run is maximal, so it is bounded by non-digits.
	digitRunPattern = regexp.MustCompile(`\d+`)
)

const plainPhoneDigits = 10

// Filter replaces PII with placeholders that are stable for one filter: the
// same value gets the same placeholder, in the order the values first appear.
// A Filter is for one work unit; it is not safe for concurrent use.
type Filter struct {
	emails   map[string]int
	phones   map[string]int
	mentions map[string]int
	counts   Counts
}

// NewFilter returns an empty filter.
func NewFilter() *Filter {
	return &Filter{emails: map[string]int{}, phones: map[string]int{}, mentions: map[string]int{}}
}

// Counts returns what the filter replaced so far.
func (f *Filter) Counts() Counts { return f.counts }

func ordinal(seen map[string]int, value string) int {
	key := strings.ToLower(value)
	n, ok := seen[key]
	if !ok {
		n = len(seen) + 1
		seen[key] = n
	}
	return n
}

// Text returns text with every supported PII class replaced. Placeholders do
// not match any pattern, so Text(Text(x)) == Text(x).
func (f *Filter) Text(text string) string {
	if text == "" {
		return text
	}
	// Credentials first: a "user:secret@host" URL must be gone before the
	// e-mail pattern reads "secret@host.example" as an address.
	filtered := logging.RedactCredentialShapes(text)
	f.counts.Credentials += strings.Count(filtered, credentialMarker) - strings.Count(text, credentialMarker)

	filtered = labeledSecretPattern.ReplaceAllStringFunc(filtered, func(m string) string {
		sub := labeledSecretPattern.FindStringSubmatch(m)
		if strings.Trim(sub[3], `"'`) == credentialMarker {
			return m
		}
		f.counts.Credentials++
		return sub[1] + sub[2] + credentialMarker
	})

	for _, pattern := range []*regexp.Regexp{authorizationPattern, bearerPattern} {
		pattern := pattern
		filtered = pattern.ReplaceAllStringFunc(filtered, func(m string) string {
			sub := pattern.FindStringSubmatch(m)
			if sub[2] == credentialMarker {
				return m
			}
			f.counts.Credentials++
			return sub[1] + credentialMarker
		})
	}

	filtered = proseSecretPattern.ReplaceAllStringFunc(filtered, func(m string) string {
		sub := proseSecretPattern.FindStringSubmatch(m)
		if !proseSecretShaped(sub[3]) {
			return m
		}
		f.counts.Credentials++
		return sub[1] + sub[2] + credentialMarker + sub[3][len(strings.TrimRight(sub[3], proseSecretTrim)):]
	})

	filtered = emailPattern.ReplaceAllStringFunc(filtered, func(m string) string {
		if isPackagePin(m) {
			return m
		}
		f.counts.Emails++
		return "[email-" + strconv.Itoa(ordinal(f.emails, m)) + "]"
	})
	filtered = jiraMentionPattern.ReplaceAllStringFunc(filtered, func(m string) string {
		f.counts.Mentions++
		return "[user-" + strconv.Itoa(ordinal(f.mentions, m)) + "]"
	})
	filtered = mentionPattern.ReplaceAllStringFunc(filtered, func(m string) string {
		at := strings.IndexByte(m, '@')
		f.counts.Mentions++
		return m[:at] + "[user-" + strconv.Itoa(ordinal(f.mentions, m[at:])) + "]"
	})
	for _, pattern := range phonePatterns {
		filtered = pattern.ReplaceAllStringFunc(filtered, func(m string) string {
			if digits := countDigits(m); digits < 8 || digits > 15 {
				return m
			}
			f.counts.Phones++
			return "[phone-" + strconv.Itoa(ordinal(f.phones, m)) + "]"
		})
	}
	filtered = digitRunPattern.ReplaceAllStringFunc(filtered, func(m string) string {
		if len(m) != plainPhoneDigits {
			return m
		}
		f.counts.Phones++
		return "[phone-" + strconv.Itoa(ordinal(f.phones, m)) + "]"
	})
	return filtered
}

// proseSecretTrim is the sentence punctuation that may end a prose value.
const proseSecretTrim = ".-,:"

// proseSecretShaped: at least 8 bytes holding a digit, and not a path, a URL, an
// issue key, a version, a date or a time, which follow the same words in
// ordinary text. The date, version and time forms are whole-value matches.
func proseSecretShaped(value string) bool {
	value = strings.TrimRight(value, proseSecretTrim)
	if len(value) < 8 || strings.ContainsAny(value, "[]") || countDigits(value) == 0 {
		return false
	}
	if value[0] == '/' || value[0] == '~' || strings.Contains(value, "://") {
		return false
	}
	return !issueKeyPattern.MatchString(value) && !versionPattern.MatchString(value) && !datePattern.MatchString(value)
}

// isPackagePin: the local part uses only characters of a package or action name
// (no "+", "%" or quote, which only an address carries) and the host part is a
// release channel or a version. "alice@latest" cannot be told from "lodash@latest".
func isPackagePin(m string) bool {
	at := strings.LastIndexByte(m, '@')
	return packageNamePattern.MatchString(m[:at]) && packageTagPattern.MatchString(m[at+1:])
}

func countDigits(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			n++
		}
	}
	return n
}
