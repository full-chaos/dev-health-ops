package lizardcc

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file ports lizard_languages/php.py's PHPReader.generate_tokens
// (php.py:213-227). Unlike every reader ported so far, PHP's tokenizer has
// an OUTER layer of its own: PHP source is normally embedded in HTML, so
// everything OUTSIDE a `<?...?>` tag is emitted as ONE opaque quoted
// "string" token (never separately tokenized), and only the text INSIDE
// tags is fed through the ordinary CodeReader.generate_tokens pipeline
// (this package's buildTokenPattern + mergeTemplateQuestionRuns +
// accumulateMacros -- see below for why PHP needs accumulateMacros when
// Ruby, built on the exact same base pattern, never does).

// phpAddition ports php.py:221's own two additions (php.py:220-221):
// `$`-prefixed variables. The heredoc/nowdoc alternative
// (`\<{3}(?P<quote>\w+).*?(?P=quote)`, php.py:222) is DELIBERATELY NOT
// folded in here -- it is a named-group BACKREFERENCE (match whatever text
// the `\w+` group actually captured, again, later in the string), and RE2
// (Go's regexp engine) supports neither backreferences nor named-group
// reuse of that kind. phpExtractHeredocs below is this port's equivalent:
// a hand-rolled two-pointer scan for the exact same "<<<LABEL ... LABEL"
// shape, applied as its own pass BEFORE the regex tokenizer runs over each
// code block, splicing the matched span in as one opaque token exactly
// where Python's single combined regex would have matched it inline.
const phpAddition = `|(?:\$[\p{L}\p{N}_]+)`

var phpTokenPattern = buildTokenPattern(phpAddition)

// phpCodeBlockPattern ports php.py:224-226's code_block_pattern
// (`\<\?(?:php)?(.*?)(?:(\?\>)|\Z)`, re.M|re.S). Go's RE2 has no `\Z`
// (Python's "absolute end of string, even after a trailing newline");
// `\z` is RE2's equivalent name for the same anchor and needs no `m`/`s`
// flag interaction here since this pattern already carries `(?s)` for
// Python's DOTALL and never anchors `^`/`$` (which the `m` flag would
// otherwise affect) -- see typescript_tokenize.go/ruby_tokenize.go's own
// docs for other spots this package needed an RE2-specific substitution.
var phpCodeBlockPattern = regexp.MustCompile(`(?s)<\?(?:php)?(.*?)(?:\?>|\z)`)

// GenerateTokensPHP ports the full PHPReader.generate_tokens pipeline:
// split the source into `<?...?>` code blocks (everything else becomes one
// opaque HTML "string" token, php.py:225-226,229), then tokenize each code
// block's own text through phpTokenizeCodeBlock.
func GenerateTokensPHP(source string) []string {
	var out []string
	currentPos := 0
	for _, m := range phpCodeBlockPattern.FindAllStringSubmatchIndex(source, -1) {
		matchStart, matchEnd := m[0], m[1]
		codeStart, codeEnd := m[2], m[3]
		if html := source[currentPos:matchStart]; html != "" {
			out = append(out, `"`+html+`"`)
		}
		out = append(out, phpTokenizeCodeBlock(source[codeStart:codeEnd])...)
		currentPos = matchEnd
	}
	if html := source[currentPos:]; html != "" {
		out = append(out, `"`+html+`"`)
	}
	return out
}

// phpTokenizeCodeBlock ports ONE call to CodeReader.generate_tokens
// (php.py:226-227's inner loop body) for a single `<?...?>` block's text:
// heredoc-splice, then the SAME generic-question-mark-glue and
// macro-accumulation passes every buildTokenPattern-based reader in this
// package applies (tokenize.go).
//
// PHP DOES need accumulateMacros, unlike Ruby (ruby_tokenize.go's own
// doc): Ruby's tokenizer addition claims a leading "#" as a comment-to-EOL
// alternative at HIGHER priority than the base pattern's bare "#"
// alternative, so accumulateMacros's `token == "#"` trigger can never
// fire for Ruby. PHP's addition claims no such thing -- a literal "#"
// (PHP's own valid line-comment marker, which CCppCommentsMixin does NOT
// recognise as a comment) falls all the way through to the base pattern's
// bare "#" alternative, and it is accumulateMacros's generic gluing (not
// any comment mechanism) that keeps a "#comment containing if" line from
// being individually tokenized and miscounted -- confirmed against real
// lizard 1.23.0 (see control_flow_and_literals.php.txt's fixture).
func phpTokenizeCodeBlock(code string) []string {
	return accumulateMacros(mergeTemplateQuestionRuns(phpExtractHeredocs(code)))
}

// phpExtractHeredocs scans code for PHP heredoc/nowdoc openers ("<<<"
// followed immediately by an identifier), splicing each COMPLETE span
// found (opener through the first later literal recurrence of that exact
// identifier text) in as one opaque token, and running phpTokenPattern
// over every other stretch of text normally.
//
// KNOWN, DELIBERATE LIMITATION (documented, not silently accepted): this
// scan operates on the RAW code text, not token-boundary-aware the way a
// single combined regex naturally is -- a literal "<<<LABEL...LABEL"
// substring appearing INSIDE a comment or an ordinary string literal would
// be spliced out here even though Python's real regex, reaching that
// position only AFTER the comment/string alternative already consumed it
// whole, never would. This cannot occur in any fixture this package
// controls (see this package's corpus files), and "<<<" beginning a
// genuine heredoc is definitionally never inside a string/comment in
// working PHP, so the gap is real but has no practical trigger; flagged
// here rather than left implicit, matching this package's convention for
// every other RE2-forced deviation.
func phpExtractHeredocs(code string) []string {
	var out []string
	pos := 0
	for {
		idx := strings.Index(code[pos:], "<<<")
		if idx == -1 {
			out = append(out, phpTokenPattern.FindAllString(code[pos:], -1)...)
			return out
		}
		absIdx := pos + idx
		out = append(out, phpTokenPattern.FindAllString(code[pos:absIdx], -1)...)

		if closeEnd, ok := phpHeredocSpan(code, absIdx); ok {
			out = append(out, code[absIdx:closeEnd])
			pos = closeEnd
			continue
		}
		// No `\w+` label immediately follows "<<<", or that label never
		// recurs later in the text -- Python's heredoc alternative fails
		// to match at this position too, so "<<<" falls through to
		// ordinary tokenization (one char at a time via the base
		// pattern's catch-all, since PHP defines no "<<" combined
		// symbol), and scanning resumes right after it.
		out = append(out, phpTokenPattern.FindAllString(code[absIdx:absIdx+3], -1)...)
		pos = absIdx + 3
	}
}

// phpHeredocSpan ports php.py:222's `\<{3}(?P<quote>\w+).*?(?P=quote)` for
// one "<<<" found at position start: read the identifier immediately
// following (Unicode-aware, matching this package's established `\w`-is-
// ASCII-only fix elsewhere -- tokenize.go's own doc), then find the FIRST
// later literal occurrence of that exact text (no word-boundary check,
// exactly like Python's plain backreference). Reports the end offset just
// past that occurrence.
func phpHeredocSpan(code string, start int) (closeEnd int, ok bool) {
	i := start + 3
	labelStart := i
	for i < len(code) {
		r, size := utf8.DecodeRuneInString(code[i:])
		if r == utf8.RuneError && size <= 1 {
			break
		}
		if !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_') {
			break
		}
		i += size
	}
	if i == labelStart {
		return 0, false
	}
	label := code[labelStart:i]
	rel := strings.Index(code[i:], label)
	if rel == -1 {
		return 0, false
	}
	return i + rel + len(label), true
}
