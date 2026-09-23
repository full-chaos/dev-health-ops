package emailvalidator

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// MaxEmailLength is pydantic.networks.MAX_EMAIL_LENGTH.
const MaxEmailLength = 2048

// prettyEmail is pydantic's pretty_email_regex, used with fullmatch:
//
//	\s*(?:((?:N+\s+)*N+)|"((?:[^"]|\")+)")?\s*<(.+)>\s*
//
// with N = [\w!#$%&'*+\-/=?^_`{|}~]. \w and \s are Python's (str.isalnum
// plus "_", and str.isspace), built from the pinned interpreter's tables.
// Go's regexp picks the submatches a backtracking engine picks
// (leftmost-first), so the groups equal Python's.
var prettyEmail = func() *regexp.Regexp {
	word := classFromPredicate(pyunicodedata.IsWord)
	space := classFromPredicate(pythonparity.IsSpace)
	name := `[` + word + `!#$%&'*+\-/=?^_` + "`" + `{|}~]`
	pattern := `\A[` + space + `]*(?:((?:` + name + `+[` + space + `]+)*` + name + `+)|"((?:[^"]|")+)")?[` + space + `]*<(.+)>[` + space + `]*\z`
	return regexp.MustCompile(pattern)
}()

// classFromPredicate renders every code point pred accepts as regexp class
// ranges (without the brackets). Surrogates are left out: the matcher
// never sees one (see ValidateEmail).
func classFromPredicate(pred func(rune) bool) string {
	var out strings.Builder
	start := rune(-1)
	flush := func(end rune) {
		if start < 0 {
			return
		}
		fmt.Fprintf(&out, `\x{%x}`, start)
		if end > start {
			fmt.Fprintf(&out, `-\x{%x}`, end)
		}
		start = -1
	}
	for r := rune(0); r <= 0x10ffff; r++ {
		if utf16.IsSurrogate(r) || !pred(r) {
			flush(r - 1)
			continue
		}
		if start < 0 {
			start = r
		}
	}
	flush(0x10ffff)
	return out.String()
}

// ValidateEmail is pydantic.networks.validate_email(value): it returns the
// normalized address pydantic stores in an EmailStr field, or the reason
// pydantic reports ("value is not a valid email address: <reason>").
func ValidateEmail(value []rune) ([]rune, string, bool) {
	if len(value) > MaxEmailLength {
		return nil, fmt.Sprintf("Length must not exceed %d characters", MaxEmailLength), false
	}
	if group, ok := prettyEmailAddress(value); ok {
		value = group
	}
	email := stripSpace(value)
	validated, err := Validate(email)
	if err != nil {
		return nil, err.Reason, false
	}
	return validated.Normalized, "", true
}

// prettyEmailAddress fullmatches prettyEmail against value and returns its
// third group. A lone surrogate is matched as U+FFFD: neither is a word
// character, whitespace, a quote, an angle bracket or a newline, so every
// part of the pattern treats the two alike, and the group is cut from
// value itself by code point offset.
func prettyEmailAddress(value []rune) ([]rune, bool) {
	var text strings.Builder
	offsets := make([]int, 0, len(value)+1)
	for _, r := range value {
		offsets = append(offsets, text.Len())
		if utf16.IsSurrogate(r) || !utf8.ValidRune(r) {
			r = utf8.RuneError
		}
		text.WriteRune(r)
	}
	offsets = append(offsets, text.Len())
	match := prettyEmail.FindStringSubmatchIndex(text.String())
	if match == nil {
		return nil, false
	}
	start, end := runeIndex(offsets, match[6]), runeIndex(offsets, match[7])
	return value[start:end], true
}

func runeIndex(offsets []int, byteOffset int) int {
	for index, offset := range offsets {
		if offset == byteOffset {
			return index
		}
	}
	panic("emailvalidator: submatch offset is not on a code point boundary")
}

// stripSpace is str.strip().
func stripSpace(text []rune) []rune {
	start := 0
	for start < len(text) && pythonparity.IsSpace(text[start]) {
		start++
	}
	return rstripSpace(text[start:])
}
