// Package emailvalidator is email-validator's syntax check (the version
// pinned in uv.lock, email_validator/syntax.py and validate_email.py) as
// pydantic's EmailStr calls it: check_deliverability=False and every other
// option at the module default. Strings are code points ([]rune), since a
// Python str may hold a lone surrogate.
//
// A rejection is a *SyntaxError whose Reason is str(EmailSyntaxError), the
// text pydantic puts into its error message and ctx.
package emailvalidator

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// SyntaxError is EmailSyntaxError; Reason is str(exc).
type SyntaxError struct{ Reason string }

func (e *SyntaxError) Error() string { return e.Reason }

func syntaxError(format string, args ...any) *SyntaxError {
	return &SyntaxError{Reason: fmt.Sprintf(format, args...)}
}

// Constants from email_validator/rfc_constants.py.
const (
	emailMaxLength      = 254
	dnsLabelLengthLimit = 63
	domainMaxLength     = 253
	fullWidthAt         = '＠'
	smallCommercialAt   = '﹫'
)

var caseInsensitiveMailboxNames = map[string]bool{
	"info": true, "marketing": true, "sales": true, "support": true,
	"abuse": true, "noc": true, "security": true,
	"postmaster": true, "hostmaster": true, "usenet": true, "news": true, "webmaster": true, "www": true, "uucp": true, "ftp": true,
}

var specialUseDomainNames = []string{"arpa", "invalid", "local", "localhost", "onion", "test"}

// isATEXT is one character of ATEXT: a-zA-Z0-9_!#$%&'*+-/=?^`{|}~.
func isATEXT(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune("_!#$%&'*+-/=?^`{|}~", r)
}

func isATEXTIntl(r rune) bool { return isATEXT(r) || r >= 0x80 }

// ATEXT_INTL_DOT_RE.
func atextIntlOrDot(r rune) bool { return r == '.' || isATEXTIntl(r) }

// ATEXT_HOSTNAME_INTL.
func hostnameIntl(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r >= 0x80:
		return true
	}
	return false
}

// QTEXT_INTL.
func qtextIntl(r rune) bool { return (r >= 0x20 && r <= 0x7e) || r >= 0x80 }

// dotAtom is the shape [atext]+(\.[atext]+)* matched against the whole
// text (the rfc_constants patterns end in \Z).
func dotAtom(text []rune, atext func(rune) bool) bool {
	if len(text) == 0 {
		return false
	}
	previousDot := true
	for _, r := range text {
		if r == '.' {
			if previousDot {
				return false
			}
			previousDot = true
			continue
		}
		if !atext(r) {
			return false
		}
		previousDot = false
	}
	return !previousDot
}

// hostnameLabel and dotAtomTextHostname are rfc_constants.HOSTNAME_LABEL
// and DOT_ATOM_TEXT_HOSTNAME, matched against the whole text (.match with
// a trailing \Z).
const hostnameLabel = `(?:(?:[a-zA-Z0-9][a-zA-Z0-9\-]*)?[a-zA-Z0-9])`

var dotAtomTextHostname = regexp.MustCompile(`\A` + hostnameLabel + `(?:\.` + hostnameLabel + `)*\z`)

// dotAtomHostname is DOT_ATOM_TEXT_HOSTNAME.match(text).
func dotAtomHostname(text []rune) bool {
	return dotAtomTextHostname.MatchString(string(text))
}

func splitRunes(text []rune, sep rune) [][]rune {
	var out [][]rune
	start := 0
	for i, r := range text {
		if r == sep {
			out = append(out, text[start:i])
			start = i + 1
		}
	}
	return append(out, text[start:])
}

// safeCharacterDisplay is safe_character_display.
func safeCharacterDisplay(r rune) string {
	if r == '\\' {
		return `"\"`
	}
	switch pyunicodedata.Category(r)[0] {
	case 'L', 'N', 'P', 'S':
		return pythonparity.StrReprRunes([]rune{r})
	}
	var fallback string
	if r < 0xffff {
		fallback = fmt.Sprintf("U+%04X", r)
	} else {
		fallback = fmt.Sprintf("U+%08X", r)
	}
	if name, ok := pyunicodedata.Name(r); ok {
		return name
	}
	return fallback
}

// displaySet is ", ".join(sorted({safe_character_display(c) for c in
// text if bad(c)})): the distinct DISPLAY strings, sorted as Python sorts
// str (code point order, which is UTF-8 byte order).
func displaySet(text []rune, bad func(rune) bool) string {
	seen := map[string]bool{}
	var items []string
	for _, r := range text {
		if !bad(r) {
			continue
		}
		display := safeCharacterDisplay(r)
		if !seen[display] {
			seen[display] = true
			items = append(items, display)
		}
	}
	sort.Strings(items)
	return strings.Join(items, ", ")
}

// checkUnsafeChars is check_unsafe_chars: it sorts the distinct CHARACTERS
// and then displays each one.
func checkUnsafeChars(text []rune, allowSpace bool) *SyntaxError {
	bad := map[rune]bool{}
	for i, r := range text {
		category := pyunicodedata.Category(r)
		switch {
		case strings.ContainsRune("LNPS", rune(category[0])):
		case category[0] == 'M':
			if i == 0 {
				bad[r] = true
			}
		case category == "Zs":
			if !allowSpace {
				bad[r] = true
			}
		default:
			bad[r] = true
		}
	}
	if len(bad) == 0 {
		return nil
	}
	chars := make([]rune, 0, len(bad))
	for r := range bad {
		chars = append(chars, r)
	}
	sort.Slice(chars, func(i, j int) bool { return chars[i] < chars[j] })
	displays := make([]string, len(chars))
	for i, r := range chars {
		displays[i] = safeCharacterDisplay(r)
	}
	return syntaxError("The email address contains unsafe characters: %s.", strings.Join(displays, ", "))
}

func hasPrefix(text []rune, prefix string) bool {
	p := []rune(prefix)
	return len(text) >= len(p) && string(text[:len(p)]) == prefix
}

func hasSuffix(text []rune, suffix string) bool {
	s := []rune(suffix)
	return len(text) >= len(s) && string(text[len(text)-len(s):]) == suffix
}

func containsSeq(text []rune, seq string) bool {
	s := []rune(seq)
	for i := 0; i+len(s) <= len(text); i++ {
		if string(text[i:i+len(s)]) == seq {
			return true
		}
	}
	return false
}

// checkDotAtom is check_dot_atom.
func checkDotAtom(label []rune, startDescr, endDescr string, isHostname bool) *SyntaxError {
	if hasSuffix(label, ".") {
		return syntaxError(endDescr, "period")
	}
	if hasPrefix(label, ".") {
		return syntaxError(startDescr, "period")
	}
	if containsSeq(label, "..") {
		return syntaxError("An email address cannot have two periods in a row.")
	}
	if isHostname {
		if hasSuffix(label, "-") {
			return syntaxError(endDescr, "hyphen")
		}
		if hasPrefix(label, "-") {
			return syntaxError(startDescr, "hyphen")
		}
		if containsSeq(label, ".-") || containsSeq(label, "-.") {
			return syntaxError("An email address cannot have a period and a hyphen next to each other.")
		}
	}
	return nil
}

func lengthReason(length, limit int) string {
	diff := length - limit
	suffix := ""
	if diff > 1 {
		suffix = "s"
	}
	return fmt.Sprintf("(%d character%s too many)", diff, suffix)
}
