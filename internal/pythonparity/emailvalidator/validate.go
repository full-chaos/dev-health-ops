package emailvalidator

import (
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyidna"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// Validated is the part of ValidatedEmail pydantic reads.
type Validated struct {
	// Normalized is ValidatedEmail.normalized.
	Normalized []rune
	// LocalPart is ValidatedEmail.local_part.
	LocalPart []rune
}

// nfcFirstDiffers reports unicodedata.normalize("NFC", text[i:])[0] != text[i]
// (the NFC of a non-empty text is never empty).
func nfcFirstDiffers(text []rune, i int) bool {
	return pyunicodedata.NFC(text[i:])[0] != text[i]
}

// splitAtUnquotedSpecial is split_email's split_string_at_unquoted_special.
func splitAtUnquotedSpecial(text []rune, specials string) ([]rune, []rune, *SyntaxError) {
	insideQuote, escaped := false, false
	var left []rune
	for i, c := range text {
		switch {
		case nfcFirstDiffers(text, i):
			left = append(left, c)
		case insideQuote:
			left = append(left, c)
			switch {
			case c == '\\' && !escaped:
				escaped = true
			case c == '"' && !escaped:
				insideQuote = false
				escaped = false
			default:
				escaped = false
			}
		case c == '"':
			left = append(left, c)
			insideQuote = true
		case strings.ContainsRune(specials, c):
			return left, text[len(left):], nil
		default:
			left = append(left, c)
		}
	}
	for _, c := range text {
		if c == fullWidthAt {
			return nil, nil, syntaxError(`The email address has the "full-width" at-sign (@) character instead of a regular at-sign.`)
		}
	}
	for _, c := range text {
		if c == smallCommercialAt {
			return nil, nil, syntaxError(`The email address has the "small commercial at" character instead of a regular at-sign.`)
		}
	}
	return nil, nil, syntaxError("An email address must have an @-sign.")
}

// unquoteQuotedString is split_email's unquote_quoted_string.
func unquoteQuotedString(text []rune) ([]rune, bool, *SyntaxError) {
	quoted, escaped := false, false
	var value []rune
	for i, c := range text {
		switch {
		case quoted:
			switch {
			case escaped:
				value = append(value, c)
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				if i != len(text)-1 {
					displays := make([]string, 0, len(text)-i-1)
					for _, extra := range text[i+1:] {
						displays = append(displays, safeCharacterDisplay(extra))
					}
					return nil, false, syntaxError("Extra character(s) found after close quote: %s", strings.Join(displays, ", "))
				}
				return value, quoted, nil
			default:
				value = append(value, c)
			}
		case i == 0 && c == '"':
			quoted = true
		default:
			value = append(value, c)
		}
	}
	return value, quoted, nil
}

func rstripChar(text []rune, char rune) []rune {
	end := len(text)
	for end > 0 && text[end-1] == char {
		end--
	}
	return text[:end]
}

// splitEmail is split_email: display name (nil when absent), local part,
// domain part, whether the local part was quoted.
func splitEmail(email []rune) (displayName []rune, hasDisplayName bool, local, domain []rune, quotedLocal bool, err *SyntaxError) {
	left, right, err := splitAtUnquotedSpecial(email, "@<")
	if err != nil {
		return nil, false, nil, nil, false, err
	}
	// right is never empty: it starts at the special that ended left.
	if right[0] == '<' {
		left = rstripSpace(left)
		name, nameQuoted, err := unquoteQuotedString(left)
		if err != nil {
			return nil, false, nil, nil, false, err
		}
		if !nameQuoted {
			// (not ATEXT_RE.match(c) and c != ' ') or c == '.': ATEXT_RE
			// is ATEXT plus the period, which the second clause takes back
			// out, so the allowed set is ATEXT and the space.
			if bad := displaySet(name, func(c rune) bool { return !isATEXT(c) && c != ' ' }); bad != "" {
				return nil, false, nil, nil, false, syntaxError("The display name contains invalid characters when not quoted: %s.", bad)
			}
		}
		if err := checkUnsafeChars(name, true); err != nil {
			return nil, false, nil, nil, false, err
		}
		if !containsRune(right, '>') {
			return nil, false, nil, nil, false, syntaxError("An open angle bracket at the start of the email address has to be followed by a close angle bracket at the end.")
		}
		right = rstripChar(right, ' ')
		if right[len(right)-1] != '>' {
			return nil, false, nil, nil, false, syntaxError("There can't be anything after the email address.")
		}
		addrSpec := rstripChar(right[1:], '>')
		local, domain, err = splitAtUnquotedSpecial(addrSpec, "@")
		if err != nil {
			return nil, false, nil, nil, false, err
		}
		displayName, hasDisplayName = name, true
	} else {
		local, domain = left, right
	}
	// Python strips a leading "@" when there is one; both splits above
	// always hand back the domain part starting at its "@".
	domain = domain[1:]
	local, quotedLocal, err = unquoteQuotedString(local)
	if err != nil {
		return nil, false, nil, nil, false, err
	}
	return displayName, hasDisplayName, local, domain, quotedLocal, nil
}

func containsRune(text []rune, want rune) bool {
	for _, r := range text {
		if r == want {
			return true
		}
	}
	return false
}

// localPartResult is LocalPartValidationResult. Python's ascii_local_part
// is the local part itself exactly when smtputf8 is false, so the flag
// carries it.
type localPartResult struct {
	localPart []rune
	smtputf8  bool
}

// validateLocalPart is validate_email_local_part with allow_smtputf8=True,
// allow_empty_local=False, strict=False.
func validateLocalPart(local []rune, quoted bool) (localPartResult, *SyntaxError) {
	if len(local) == 0 {
		return localPartResult{}, syntaxError("There must be something before the @-sign.")
	}
	if dotAtom(local, isATEXT) {
		return localPartResult{localPart: local}, nil
	}
	valid := ""
	if dotAtom(local, isATEXTIntl) {
		valid = "dot-atom"
	} else if quoted {
		if bad := displaySet(local, func(c rune) bool { return !qtextIntl(c) }); bad != "" {
			return localPartResult{}, syntaxError("The email address contains invalid characters in quotes before the @-sign: %s.", bad)
		}
		valid = "quoted"
	}
	if valid != "" {
		if err := checkUnsafeChars(local, valid == "quoted"); err != nil {
			return localPartResult{}, err
		}
		for _, c := range local {
			if !utf8.ValidRune(c) {
				return localPartResult{}, syntaxError("The email address contains an invalid character.")
			}
		}
		// Python also re-quotes a quoted local part and works out whether
		// it needs SMTPUTF8; a quoted local part is always refused before
		// either is read (allow_quoted_local is false), so neither is
		// computed here.
		return localPartResult{localPart: local, smtputf8: true}, nil
	}
	if bad := displaySet(local, func(c rune) bool { return !atextIntlOrDot(c) }); bad != "" {
		return localPartResult{}, syntaxError("The email address contains invalid characters before the @-sign: %s.", bad)
	}
	if err := checkDotAtom(local, "An email address cannot start with a %s.", "An email address cannot have a %s immediately before the @-sign.", false); err != nil {
		return localPartResult{}, err
	}
	return localPartResult{}, syntaxError("The email address contains invalid characters before the @-sign.")
}

// isUTS46ValidChar is uts46_valid_char for the code points that reach it:
// validateDomainName calls it only after checkUnsafeChars has refused
// every code point of category C* or Z*. Over the rest (all of Unicode
// 16.0.0, checked against the live function), uts46_valid_char is true
// exactly for U+FF0E and for a code point whose decomposition does not hold
// U+002E; its C0/C1, Cf/Cn/Co/Cs and space-separator branches decide only
// code points that cannot get here.
func isUTS46ValidChar(r rune) bool {
	return r == 0xff0e || !pyunicodedata.DecompositionHasFullStop(r)
}

// twoLettersTwoDashes is re.match(r"(?!xn)..--", label, re.I) for a label
// of hostname characters (no newline, which "." would not match, can be in
// one here).
func twoLettersTwoDashes(label []rune) bool {
	if len(label) < 4 || label[2] != '-' || label[3] != '-' {
		return false
	}
	return !((label[0] == 'x' || label[0] == 'X') && (label[1] == 'n' || label[1] == 'N'))
}

// domainNameRegex is DOMAIN_NAME_REGEX (used with search): the domain ends
// in an ASCII letter.
var domainNameRegex = regexp.MustCompile(`[A-Za-z]\z`)

type domainResult struct{ asciiDomain, domain []rune }

// validateDomainName is validate_email_domain_name with
// test_environment=False, globally_deliverable=True.
func validateDomainName(domain []rune) (domainResult, *SyntaxError) {
	if bad := displaySet(domain, func(c rune) bool { return !hostnameIntl(c) }); bad != "" {
		return domainResult{}, syntaxError("The part after the @-sign contains invalid characters: %s.", bad)
	}
	if err := checkUnsafeChars(domain, false); err != nil {
		return domainResult{}, err
	}
	if bad := displaySet(domain, func(c rune) bool { return !isUTS46ValidChar(c) }); bad != "" {
		return domainResult{}, syntaxError("The part after the @-sign contains invalid characters: %s.", bad)
	}
	original := domain
	remapped, idnaErr := pyidna.UTS46Remap(domain)
	if idnaErr != nil {
		return domainResult{}, syntaxError("The part after the @-sign contains invalid characters (%s).", idnaErr.Message)
	}
	domain = remapped
	if bad := displaySet(domain, func(c rune) bool { return !hostnameIntl(c) }); bad != "" {
		return domainResult{}, syntaxError("The part after the @-sign contains invalid characters after Unicode normalization: %s.", bad)
	}
	if err := checkDotAtom(domain, "An email address cannot have a %s immediately after the @-sign.", "An email address cannot end with a %s.", true); err != nil {
		return domainResult{}, err
	}
	for _, label := range splitRunes(domain, '.') {
		if twoLettersTwoDashes(label) {
			return domainResult{}, syntaxError("An email address cannot have two letters followed by two dashes immediately after the @-sign or after a period, except Punycode.")
		}
	}
	var asciiDomain []rune
	if dotAtomHostname(domain) {
		asciiDomain = domain
	} else {
		for i, label := range splitRunes(domain, '.') {
			encoded, idnaErr := pyidna.Alabel(label)
			if idnaErr != nil {
				return domainResult{}, syntaxError("The part after the @-sign is invalid (%s).", idnaErr.Message)
			}
			if i > 0 {
				asciiDomain = append(asciiDomain, '.')
			}
			for _, b := range encoded {
				asciiDomain = append(asciiDomain, rune(b))
			}
		}
		if !dotAtomHostname(asciiDomain) {
			return domainResult{}, syntaxError("The email address contains invalid characters after the @-sign after IDNA encoding.")
		}
	}
	if len(asciiDomain) > domainMaxLength {
		if equalRunes(asciiDomain, original) {
			return domainResult{}, syntaxError("The email address is too long after the @-sign %s.", lengthReason(len(asciiDomain), domainMaxLength))
		}
		diff := len(asciiDomain) - domainMaxLength
		s := "s"
		if diff == 1 {
			s = ""
		}
		return domainResult{}, syntaxError("The email address is too long after the @-sign (%d byte%s too many after IDNA encoding).", diff, s)
	}
	for _, label := range splitRunes(asciiDomain, '.') {
		if len(label) > dnsLabelLengthLimit {
			return domainResult{}, syntaxError("After the @-sign, periods cannot be separated by so many characters %s.", lengthReason(len(label), dnsLabelLengthLimit))
		}
	}
	if !containsRune(asciiDomain, '.') {
		return domainResult{}, syntaxError("The part after the @-sign is not valid. It should have a period.")
	}
	if !domainNameRegex.MatchString(string(asciiDomain)) {
		return domainResult{}, syntaxError("The part after the @-sign is not valid. It is not within a valid top-level domain.")
	}
	for _, name := range specialUseDomainNames {
		// Python also refuses the bare name; every special-use name has no
		// period, and a domain without one was refused just above.
		if hasSuffix(asciiDomain, "."+name) {
			return domainResult{}, syntaxError("The part after the @-sign is a special-use or reserved name that cannot be used with email.")
		}
	}
	domainI18n, idnaErr := pyidna.Decode(asciiDomain)
	if idnaErr != nil {
		return domainResult{}, syntaxError("The part after the @-sign is not valid IDNA (%s).", idnaErr.Message)
	}
	if bad := displaySet(domainI18n, func(c rune) bool { return !hostnameIntl(c) }); bad != "" {
		return domainResult{}, syntaxError("The part after the @-sign contains invalid characters: %s.", bad)
	}
	if err := checkUnsafeChars(domainI18n, false); err != nil {
		return domainResult{}, err
	}
	if _, idnaErr := pyidna.Encode(domainI18n); idnaErr != nil {
		return domainResult{}, syntaxError("The part after the @-sign became invalid after normalizing to international characters (%s).", idnaErr.Message)
	}
	return domainResult{asciiDomain: asciiDomain, domain: domainI18n}, nil
}

func equalRunes(a, b []rune) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func utf8Len(text []rune) int {
	n := 0
	for _, r := range text {
		n += utf8.RuneLen(r)
	}
	return n
}

// validateLength is validate_email_length. The three addresses hold no
// lone surrogate by the time it runs (every path to it has rejected one),
// so their UTF-8 length is well defined.
func validateLength(original, normalized, asciiForm []rune) *SyntaxError {
	checks := []struct {
		addr   []rune
		reason string
		named  bool
	}{
		{original, "", false},
		{normalized, "after normalization", true},
		{asciiForm, "when the part after the @-sign is converted to IDNA ASCII", true},
	}
	for _, check := range checks {
		addrLen, addrUTF8Len := len(check.addr), utf8Len(check.addr)
		diff := addrUTF8Len - emailMaxLength
		if diff <= 0 {
			continue
		}
		suffix := ""
		if diff > 1 {
			suffix = "s"
		}
		var reason string
		switch {
		case !check.named && addrLen == addrUTF8Len:
			reason = lengthReason(addrLen, emailMaxLength)
		case !check.named:
			mbpc := 0
			for _, r := range check.addr {
				if n := utf8.RuneLen(r); n > mbpc {
					mbpc = n
				}
			}
			mchars := diff / mbpc
			if mchars < 1 {
				mchars = 1
			}
			if mchars == diff {
				reason = "(" + itoa(diff) + " character" + suffix + " too many)"
			} else {
				reason = "(" + itoa(mchars) + "-" + itoa(diff) + " character" + suffix + " too many)"
			}
		default:
			reason = check.reason + " (" + itoa(diff) + " byte" + suffix + " too many)"
		}
		return syntaxError("The email address is too long %s.", reason)
	}
	return nil
}

// Validate is email_validator.validate_email(email,
// check_deliverability=False) with every other option at its default.
func Validate(email []rune) (Validated, *SyntaxError) {
	displayName, hasDisplayName, local, domainPart, quotedLocal, err := splitEmail(email)
	if err != nil {
		return Validated{}, err
	}
	_ = displayName
	var original []rune
	if quotedLocal {
		original = append(append([]rune{'"'}, local...), '"')
	} else {
		original = append([]rune{}, local...)
	}
	original = append(append(original, '@'), domainPart...)

	info, err := validateLocalPart(local, quotedLocal)
	if err != nil {
		return Validated{}, err
	}
	localPart := info.localPart
	if normalizedLocal := pyunicodedata.NFC(localPart); !equalRunes(normalizedLocal, localPart) {
		if _, err := validateLocalPart(normalizedLocal, quotedLocal); err != nil {
			return Validated{}, syntaxError("After Unicode normalization: %s", err.Reason)
		}
		localPart = normalizedLocal
	}
	if quotedLocal {
		return Validated{}, syntaxError("Quoting the part before the @-sign is not allowed here.")
	}
	// ascii_local_part is set (and equal to the local part, which NFC
	// leaves unchanged when it is ASCII) exactly when smtputf8 is false.
	if !info.smtputf8 && caseInsensitiveMailboxNames[strings.ToLower(string(localPart))] {
		localPart = []rune(strings.ToLower(string(localPart)))
	}

	var domain, asciiDomain []rune
	switch {
	case len(domainPart) == 0:
		return Validated{}, syntaxError("There must be something after the @-sign.")
	case domainPart[0] == '[' && domainPart[len(domainPart)-1] == ']':
		if err := validateDomainLiteral(domainPart[1 : len(domainPart)-1]); err != nil {
			return Validated{}, err
		}
		return Validated{}, syntaxError("A bracketed IP address after the @-sign is not allowed here.")
	default:
		result, err := validateDomainName(domainPart)
		if err != nil {
			return Validated{}, err
		}
		domain, asciiDomain = result.domain, result.asciiDomain
	}

	normalized := append(append(append([]rune{}, localPart...), '@'), domain...)
	// (ascii_local_part or local_part): the two are equal whenever the
	// first is set.
	asciiForm := append(append(append([]rune{}, localPart...), '@'), asciiDomain...)
	if err := validateLength(original, normalized, asciiForm); err != nil {
		return Validated{}, err
	}
	if hasDisplayName {
		return Validated{}, syntaxError("A display name and angle brackets around the email address are not permitted here.")
	}
	return Validated{Normalized: normalized, LocalPart: localPart}, nil
}
