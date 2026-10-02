package logging

import (
	"regexp"
	"strings"
)

// providerTokenPattern catches credentials of the providers the workers call
// by their documented prefixes wherever they appear, including prose with no
// key in front ("token ghp_... is invalid"): GitHub (ghp_, gho_, ghu_, ghs_,
// ghr_, github_pat_), GitLab (glpat-, gloas-, glrt-, gldt-, glptt-, glsoat-,
// glft-, glimt-, glagent-, glcbt-, glffct-), Linear (lin_api_, lin_oauth_),
// Atlassian (ATATT, ATCTT), PagerDuty OAuth (pdus+_) and LaunchDarkly
// (api-, sdk-, mob- followed by a UUID), LLM providers (sk-, sk-proj-, sk-ant-,
// sk-or-: OpenAI, Anthropic, OpenRouter), Google (AIza), Stripe (sk_/rk_
// live and test keys, whsec_), Slack (xox*-) and JWTs (three base64url parts).
// A credential with no prefix (a hex or base64url secret, a legacy 20-character
// key) cannot be told from an id by its shape: it stays with the registered-value
// redactor (secrets) and with the fixed-class error helpers (CHAOS-7937).
var providerTokenPattern = regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{16,}|github_pat_[A-Za-z0-9_]{16,}|gl(?:pat|oas|rt|dt|ptt|soat|ft|imt|agent|cbt|ffct)-[A-Za-z0-9_\-]{16,}|lin_(?:api|oauth)_[A-Za-z0-9]{16,}|AT[AC]TT[A-Za-z0-9_\-=]{16,}|pdus\+_[A-Za-z0-9_\-+/=]{16,}|(?:api|sdk|mob)-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`)

var vendorKeyPattern = regexp.MustCompile(`(?:sk-(?:(?:proj|ant|or|svcacct|admin)-[A-Za-z0-9_\-]{20,}|[A-Za-z0-9]{20,})|(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}|whsec_[A-Za-z0-9]{16,}|AIza[A-Za-z0-9_\-]{30,}|xox[abcdeprs]-[A-Za-z0-9\-]{10,}|eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,})`)

// vendorKeyLiterals are the literals vendorKeyPattern cannot match without.
var vendorKeyLiterals = []string{"sk-", "sk_", "rk_", "whsec_", "AIza", "xox", "eyJ"}

func mayHoldVendorKey(text string) bool {
	for _, literal := range vendorKeyLiterals {
		if strings.Contains(text, literal) {
			return true
		}
	}
	return false
}

// providerTokenLiterals are the literals providerTokenPattern cannot match
// without; a text holding none of them skips the pattern.
var providerTokenLiterals = []string{"gh", "github_pat_", "gl", "lin_", "ATATT", "ATCTT", "pdus+_", "api-", "sdk-", "mob-"}

func mayHoldProviderToken(text string) bool {
	for _, literal := range providerTokenLiterals {
		if strings.Contains(text, literal) {
			return true
		}
	}
	return false
}

// headerKeysToEndOfLine are header names whose value carries separators of its
// own ("Cookie: a=1; b=2", "Authorization: token abc"), so an unquoted value
// runs to the end of the line.
var headerKeysToEndOfLine = map[string]bool{
	"cookie": true, "set-cookie": true, "authorization": true, "proxy-authorization": true,
}

// redactKeyValues replaces the value of every protected key found in text:
// JSON (`"token":"v"`), JSON escaped inside a string at any depth
// (`\"token\":\"v\"`), a query string (`?access_token=v&x=1`), a header line
// (`Private-Token: v`), `key=value`, and Go's %v of a map or %+v of a struct
// (`map[token:v]`, `{Token:v}`). Quoting around a replaced value is kept.
//
// The scan is driven by the separators: at every ":" or "=" it reads the key
// that ends there (a word of letters, digits, "_", ".", "-" that starts with a
// letter or "_", optionally inside quotes at any escape depth, optionally
// followed by spaces or tabs) and, when that key is protected, measures and
// replaces the value that follows.
func redactKeyValues(text string, escaped []bool) string {
	if !strings.ContainsAny(text, ":=") || !mayHoldProtectedKey(text) {
		return text
	}
	var out strings.Builder
	written := 0
	position := 0
	for position < len(text) {
		separator := strings.IndexAny(text[position:], ":=")
		if separator >= 0 {
			separator += position
		}
		width := 1
		if separator < 0 {
			break
		}
		key, keyQuote, found := keyBefore(text, separator)
		quoted := keyQuote != ""
		if !found || !protectedKeyCached(key) {
			position = separator + width
			continue
		}
		valueStart := separator + width
		for valueStart < len(text) && (text[valueStart] == ' ' || text[valueStart] == '\t') {
			valueStart++
		}
		start, end, closing := protectedValueSpan(text, valueStart, strings.ToLower(key), quoted, escaped)
		if end <= start {
			position = separator + width
			continue
		}
		out.WriteString(text[written:start])
		if quoted && closing == "" && start == valueStart {
			// A quoted key's object, array or literal value becomes a
			// string at the key's own quoting, so the document stays
			// valid.
			out.WriteString(keyQuote + redacted + keyQuote)
		} else {
			out.WriteString(redacted)
			out.WriteString(closing)
		}
		written = end
		position = end
	}
	if written == 0 {
		return text
	}
	out.WriteString(text[written:])
	return out.String()
}

// keyBefore reads the key that ends at the separator at index separator.
// quote is the key's closing quote with its escaping ("\"", "\\\"", "'"),
// or "" for an unquoted key.
func keyBefore(text string, separator int) (key string, quote string, found bool) {
	end := separator
	for end > 0 && (text[end-1] == ' ' || text[end-1] == '\t') {
		end--
	}
	if end > 0 && (text[end-1] == '"' || text[end-1] == '\'') {
		quoteEnd := end
		end--
		for end > 0 && text[end-1] == '\\' {
			end--
		}
		quote = text[end:quoteEnd]
	}
	start := end
	for start > 0 && isKeyByte(text[start-1]) {
		start--
	}
	for start < end && !isKeyStartByte(text[start]) {
		start++
	}
	if start == end {
		return "", "", false
	}
	if quote == "" && start > 0 && (text[start-1] == '"' || text[start-1] == '\'') {
		quote = text[start-1 : start]
	}
	return text[start:end], quote, true
}

func isKeyStartByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isKeyByte(b byte) bool {
	return isKeyStartByte(b) || b == '.' || b == '-' || (b >= '0' && b <= '9')
}

// protectedValueSpan measures the value that starts at start. It returns the
// byte range to replace and the closing text to write after the marker (the
// value's own closing quote, when it had one; the range covers that quote).
// Every ambiguous shape resolves toward hiding: a quoted or bracketed value
// that never closes, or brackets that do not match, run to the end of the
// text.
func protectedValueSpan(text string, start int, key string, quotedKey bool, escaped []bool) (int, int, string) {
	if start >= len(text) {
		return start, start, ""
	}
	if !quotedKey && headerKeysToEndOfLine[key] {
		// A header value carries its own separators ("a=1; b=2") and may
		// follow ":" or "=" with or without a space: it runs to the end of
		// the line.
		end := strings.IndexAny(text[start:], "\r\n")
		if end < 0 {
			return start, len(text), ""
		}
		return start, start + end, ""
	}
	backslashes := 0
	for start+backslashes < len(text) && text[start+backslashes] == '\\' {
		backslashes++
	}
	if start+backslashes < len(text) && (text[start+backslashes] == '"' || text[start+backslashes] == '\'') {
		quote := text[start+backslashes]
		contentStart := start + backslashes + 1
		for index := contentStart; index < len(text); index++ {
			if text[index] != quote {
				continue
			}
			run := 0
			for run < index-contentStart && text[index-1-run] == '\\' {
				run++
			}
			if run == backslashes {
				return contentStart, index + 1, text[index-backslashes : index+1]
			}
		}
		return contentStart, len(text), ""
	}
	if text[start] == '{' || text[start] == '[' {
		return start, matchingBracket(text, start), ""
	}
	// A bare value runs to raw whitespace, "&", "," or ";" -- a delimiter
	// that was percent-encoded is part of the value. After a quoted (JSON)
	// key it is a number or literal, which also ends at "}" or "]".
	stops := " \t\r\n&,;"
	if quotedKey {
		stops += "}]"
	}
	for index := start; index < len(text); index++ {
		if strings.IndexByte(stops, text[index]) >= 0 && (escaped == nil || !escaped[index]) {
			return start, index, ""
		}
	}
	return start, len(text), ""
}

// matchingBracket returns the index after the bracket that closes the one at
// open, counting only the same bracket type and skipping quoted strings (at
// the escape depth of the first quote met). A mismatched or missing close
// returns the end of the text.
func matchingBracket(text string, open int) int {
	opening := text[open]
	closing := byte('}')
	if opening == '[' {
		closing = ']'
	}
	depth := 0
	inString := false
	quoteDepth := -1
	for index := open; index < len(text); index++ {
		b := text[index]
		if b == '"' {
			run := 0
			for run < index-open && text[index-1-run] == '\\' {
				run++
			}
			if quoteDepth < 0 {
				quoteDepth = run
			}
			if run == quoteDepth {
				inString = !inString
			}
			continue
		}
		if inString {
			continue
		}
		switch b {
		case opening:
			depth++
		case closing:
			depth--
			if depth == 0 {
				return index + 1
			}
		case '}', ']':
			// The other bracket type closing here means the value is not
			// well formed.
			return len(text)
		}
	}
	return len(text)
}

// proseCredentialPattern finds a credential word followed by whitespace and a
// value ("token ghp-abc", "api key 9f2c..."): a provider body written as
// prose rather than as key/value pairs.
var proseCredentialPattern = regexp.MustCompile(`(?i)\b(?:tokens?|secrets?|passwords?|passwd|apikeys?|api[ _-]keys?|client[ _-]secrets?|access[ _-]tokens?|private[ _-]tokens?|credentials?)[ \t]+([^\s"'<>,;()\[\]{}]+)`)

// contextCredentialPattern finds a credential word, at most three filler words of a rejection sentence ("provided:", "is",
// "rejected", "for"), and a long value: "Incorrect API key provided: <value>", "invalid key <value> rejected", "secret is
// <value>" (CHAOS-7937). The words are key, api key, secret, password, credential, token, authorization and bearer.
var contextCredentialPattern = regexp.MustCompile(`(?i)\b(?:keys?|secrets?|passwords?|passwd|credentials?|tokens?|authorization|bearer)\b(?:` + contextSeparator + `+(?:is|was|for|provided|invalid|incorrect|rejected|expired|revoked|unknown|bad|failed|the|a|of)){0,3}` + contextSeparator + `+([A-Za-z0-9_+/=.\-]{20,})`)

// contextSeparator is what stands between a credential word and its value: spaces, tabs, ":" and "=", an opening quote, bracket
// or parenthesis, and the escaped forms a JSON body carries (a backslash and t, n, r, a quote, or \u0022 / \u0027).
const contextSeparator = `(?:[ \t:=(\["']|\\[tnr"']|\\u00(?:22|27))`

// opaqueCredentialShaped: at least 20 bytes of the base64url / hex alphabet holding a digit AND a letter. A hyphenated word
// ("rotation-is-scheduled-for-next-quarter") has no digit and an id of digits only has no letter: both stay.
func opaqueCredentialShaped(value string) bool {
	if len(value) < 20 || value == redacted {
		return false
	}
	return strings.ContainsAny(value, "0123456789") && strings.ContainsAny(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
}

// redactProseCredentials replaces the value after a credential word when the value looks like a credential. The first rule
// (credential word, whitespace, value of at least 8 bytes holding a digit or one of "-_./+=") keeps "token is invalid" and
// "password expired" as they are; the second (contextCredentialPattern) reaches the value behind filler words.
func redactProseCredentials(text string) string {
	lower := strings.ToLower(text)
	if !strings.Contains(lower, "token") && !strings.Contains(lower, "secret") && !strings.Contains(lower, "passw") &&
		!strings.Contains(lower, "key") && !strings.Contains(lower, "credential") && !strings.Contains(lower, "authorization") &&
		!strings.Contains(lower, "bearer") {
		return text
	}
	text = redactAfterWord(text, proseCredentialPattern, credentialShaped)
	return redactAfterWord(text, contextCredentialPattern, opaqueCredentialShaped)
}

func redactAfterWord(text string, pattern *regexp.Regexp, shaped func(string) bool) string {
	matches := pattern.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return text
	}
	var out strings.Builder
	written := 0
	changed := false
	for _, match := range matches {
		start, end := match[2], match[3]
		if !shaped(text[start:end]) {
			continue
		}
		out.WriteString(text[written:start])
		out.WriteString(redacted)
		written = end
		changed = true
	}
	if !changed {
		return text
	}
	out.WriteString(text[written:])
	return out.String()
}

// RedactCredentialShapes is the credential part of RedactText for text that must keep its other bytes: provider tokens by
// documented prefix and a long value behind a credential word. The persisted error columns use it after their own
// patterns (CHAOS-7937).
func RedactCredentialShapes(value string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	if mayHoldProviderToken(value) {
		value = providerTokenPattern.ReplaceAllString(value, redacted)
	}
	if mayHoldVendorKey(value) {
		value = vendorKeyPattern.ReplaceAllString(value, redacted)
	}
	return redactProseCredentials(value)
}

func credentialShaped(value string) bool {
	if len(value) < 8 || value == redacted {
		return false
	}
	return strings.ContainsAny(value, "0123456789-_./+=")
}

// redactPathSegments hides protected path segments in any "/"-separated run:
// a segment that is exactly a protected name ("token", "api_key") hides the
// segment after it ("/token/<value>"); a longer segment holding a protected
// word ("token-abc123") is hidden itself.
func redactPathSegments(text string) string {
	if !strings.Contains(text, "/") || !mayHoldProtectedKey(text) {
		return text
	}
	var out strings.Builder
	written := 0
	hideNext := false
	segmentStart := -1
	flush := func(end int) {
		if segmentStart < 0 || end <= segmentStart {
			return
		}
		segment := text[segmentStart:end]
		hide := hideNext
		hideNext = false
		if !hide && segment != redacted && protectedKeyCached(segment) {
			if protectedName(segment) {
				hideNext = true
			} else {
				hide = true
			}
		}
		if hide && segment != redacted {
			out.WriteString(text[written:segmentStart])
			out.WriteString(redacted)
			written = end
		}
	}
	inPath := false
	for index := 0; index < len(text); index++ {
		b := text[index]
		switch {
		case b == '/':
			if inPath {
				flush(index)
			}
			inPath = true
			segmentStart = index + 1
		case inPath && strings.IndexByte(" \t\r\n\"'?#&;,<>()[]{}\\", b) >= 0:
			flush(index)
			inPath = false
			hideNext = false
			segmentStart = -1
		}
	}
	if inPath {
		flush(len(text))
	}
	if written == 0 {
		return text
	}
	out.WriteString(text[written:])
	return out.String()
}

// protectedName reports whether a segment is nothing but a protected name:
// every word of it is a listed word or part of a listed pair.
func protectedName(segment string) bool {
	words := keyWords(segment)
	if len(words) == 0 {
		return false
	}
	allListed := true
	for _, word := range words {
		if !protectedKeyWords[word] && !protectedPairWords[word] {
			allListed = false
		}
	}
	if allListed {
		return true
	}
	// A name run together ("sessioncookie", "apikeys") is protected when it
	// splits entirely into listed words, possibly plural.
	return splitsIntoListedWords(strings.TrimSuffix(strings.Join(words, ""), "s"))
}

func splitsIntoListedWords(joined string) bool {
	reachable := make([]bool, len(joined)+1)
	reachable[0] = true
	for end := 1; end <= len(joined); end++ {
		for start := 0; start < end && !reachable[end]; start++ {
			part := joined[start:end]
			if reachable[start] && (protectedKeyWords[part] || protectedPairWords[part]) {
				reachable[end] = true
			}
		}
	}
	return len(joined) > 0 && reachable[len(joined)]
}

// maxPercentDecodePasses bounds decoding of text encoded more than once
// ("%253D" -> "%3D" -> "=").
const maxPercentDecodePasses = 3

// percentDecoded decodes every valid %XX escape in text, repeating until the
// text stops changing or maxPercentDecodePasses is reached, and reports for
// each byte of the result whether it came from an escape. A "%" not followed
// by two hex digits is kept as it is. escaped is nil when nothing decoded.
func percentDecoded(text string) (string, []bool) {
	var escaped []bool
	for pass := 0; pass < maxPercentDecodePasses && strings.Contains(text, "%"); pass++ {
		decoded, mask := decodePercentOnce(text, escaped)
		if mask == nil {
			break
		}
		text, escaped = decoded, mask
	}
	return text, escaped
}

func decodePercentOnce(text string, escaped []bool) (string, []bool) {
	var out []byte
	var mask []bool
	written := 0
	copyRun := func(end int) {
		out = append(out, text[written:end]...)
		for index := written; index < end; index++ {
			mask = append(mask, escaped != nil && escaped[index])
		}
	}
	for index := 0; index+2 < len(text); index++ {
		if text[index] != '%' || !isHex(text[index+1]) || !isHex(text[index+2]) {
			continue
		}
		copyRun(index)
		out = append(out, unhex(text[index+1])<<4|unhex(text[index+2]))
		mask = append(mask, true)
		written = index + 3
		index += 2
	}
	if written == 0 {
		return text, nil
	}
	copyRun(len(text))
	return string(out), mask
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

func unhex(b byte) byte {
	switch {
	case b >= '0' && b <= '9':
		return b - '0'
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10
	}
	return b - 'A' + 10
}
