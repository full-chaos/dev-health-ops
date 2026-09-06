package lizardcc

import (
	"regexp"
	"strings"
)

// This file ports typescript.py's TypeScriptReader.generate_tokens
// (typescript.py:67-113) and its @js_style_regex_expression decorator
// (js_style_regex_expression.py). JavaScriptReader (javascript.py) and
// VueReader (vue.py) both inherit this unchanged -- neither overrides
// generate_tokens -- so this single tokenizer serves "javascript",
// "typescript" and "vue" (compute.go's languageByExtension keys) alike.

// tsAddition is TypeScriptReader.generate_tokens's own `addition`
// (typescript.py:110-111): private-field `#name`, dollar-prefixed
// identifiers, an identifier immediately followed by `?` (TS optional
// chaining/property shorthand, glued as ONE token so e.g. `foo?` doesn't
// tokenize as "foo" then "?" -- the latter IS a member of conditions
// (ternary), so failing to glue this would over-count), and a backtick
// template-literal token (matched here as one opaque unit and split apart
// afterward by splitTemplateLiterals, mirroring Python's two-step order:
// CodeReader.generate_tokens produces ONE `...` token first, THEN
// TypeScriptReader's own body splits it).
const tsAddition = `|(?:#\w+)` + `|(?:\$\w+)` + `|(?:\w+\?)` + "|`.*?`"

var tsTokenPattern = buildTokenPattern(tsAddition)

// GenerateTokensTS ports the full TypeScript/JavaScript tokenizer
// pipeline against tsTokenPattern (TypeScriptReader.generate_tokens's own
// addition, no more) -- see generateTokensTSWith for the shared pipeline
// body Vue reuses against its OWN, wider pattern (vue_tokenize.go).
func GenerateTokensTS(source string) []string {
	return generateTokensTSWith(tsTokenPattern, source)
}

// generateTokensTSWith is the full TypeScript/JavaScript/Vue tokenizer
// pipeline, parameterized on the compiled token pattern: base regex
// tokenize -> generic-vs-comparison merge -> macro accumulation (all
// three shared with clike.go's GenerateTokens, since CodeReader.
// generate_tokens is the one base method every reader calls) ->
// template-literal splitting (TypeScript-specific) -> regex-literal-vs-
// division disambiguation (the js_style_regex_expression decorator, shared
// by every JS-family reader).
//
// Vue needs this parameterized rather than calling GenerateTokensTS
// directly because VueReader.generate_tokens (vue.py:19-22) calls
// `TypeScriptReader.generate_tokens(source_code, addition, token_class)`
// with an EXTRA addition appended (an HTML/Vue block-tag alternative,
// vue_tokenize.go's vueTagAddition) -- the underlying REGEX differs, but
// every step AFTER tokenizing is identical Python code
// (TypeScriptReader.generate_tokens's own body), so only the pattern
// varies, not the pipeline.
func generateTokensTSWith(pattern *regexp.Regexp, source string) []string {
	raw := pattern.FindAllString(source, -1)
	merged := mergeTemplateQuestionRuns(raw)
	withMacros := accumulateMacros(merged)
	withTemplates := splitTemplateLiterals(withMacros)
	return mergeJSRegexLiterals(withTemplates)
}

// splitTemplateLiterals ports the nested split_template_literal generator
// and its caller loop (typescript.py:73-108, 112-113): every backtick
// token the base tokenizer produced as one opaque unit is broken into
// quote/literal/`${`/expr/`}`/quote pieces. The `${...}` EXPRESSION body is
// NOT re-tokenized here -- it is yielded as one single raw-text token,
// exactly as Python's `yield expr` does (a `?`/`&&`/`||` inside a template
// interpolation is therefore invisible to condition_counter, matching
// lizard's own measured behaviour, not a bug this port introduces).
func splitTemplateLiterals(tokens []string) []string {
	out := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		if strings.HasPrefix(tok, "`") && strings.HasSuffix(tok, "`") && len(tok) > 1 {
			out = append(out, splitOneTemplateLiteral(tok)...)
			continue
		}
		out = append(out, tok)
	}
	return out
}

func splitOneTemplateLiteral(tok string) []string {
	const quote = "`"
	content := tok[1 : len(tok)-1]
	out := []string{quote}

	if !strings.Contains(content, "${") {
		if content != "" {
			out = append(out, quote+content+quote)
		}
		out = append(out, quote)
		return out
	}

	i := 0
	for i < len(content) {
		idx := strings.Index(content[i:], "${")
		if idx == -1 {
			if i < len(content) {
				out = append(out, quote+content[i:]+quote)
			}
			return append(out, quote)
		}
		idx += i
		if idx > i {
			out = append(out, quote+content[i:idx]+quote)
		}
		out = append(out, "${")
		i = idx + 2
		exprStart := i
		braceCount := 1
		for i < len(content) && braceCount > 0 {
			switch content[i] {
			case '{':
				braceCount++
			case '}':
				braceCount--
			}
			i++
		}
		if braceCount > 0 {
			// Unterminated expression: Python yields the ORIGINAL whole
			// token verbatim and returns, discarding everything already
			// yielded for this call (typescript.py:97-99) -- the whole
			// split is abandoned, not just this segment.
			return []string{tok}
		}
		expr := content[exprStart : i-1]
		out = append(out, expr, "}")
		content = content[i:]
		i = 0
	}
	return append(out, quote)
}

// jsRegexLiteralPattern ports js_style_regex_expression.py's regx_regx:
// a `/`, then one-or-more runs of "non-whitespace ending in a non-
// backslash-non-space character, then /", followed by optional "igm"
// flags.
var jsRegexLiteralPattern = regexp.MustCompile(`^/(\S*?[^\s\\]/)+?(igm)*`)

var jsRegexFlagsPattern = regexp.MustCompile(`^[igm]+$`)

// mergeJSRegexLiterals ports js_style_regex_expression's decorator body: a
// standalone "/" token that (by position/preceding-token context) looks
// like it opens a regex literal rather than a division operator is glued
// together with every following token up to and including the NEXT token
// ending in "/", plus an optional trailing flags token, PROVIDED the
// combined text actually matches regx_regx -- otherwise every gathered
// token is emitted ungrouped, exactly as Python's else branch does.
func mergeJSRegexLiterals(tokens []string) []string {
	result := make([]string, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		if token != "/" {
			result = append(result, token)
			continue
		}
		isRegex := i == 0
		if i > 0 {
			prev := strings.TrimSpace(tokens[i-1])
			if prev != "" && strings.ContainsRune(`=,({[?:!&|;`, rune(prev[len(prev)-1])) {
				isRegex = true
			}
		}
		if !isRegex {
			result = append(result, token)
			continue
		}
		regexTokens := []string{token}
		i++
		for i < len(tokens) && !strings.HasSuffix(tokens[i], "/") {
			regexTokens = append(regexTokens, tokens[i])
			i++
		}
		if i < len(tokens) {
			regexTokens = append(regexTokens, tokens[i])
			i++
			if i < len(tokens) && jsRegexFlagsPattern.MatchString(tokens[i]) {
				regexTokens = append(regexTokens, tokens[i])
				i++
			}
		}
		combined := strings.Join(regexTokens, "")
		if jsRegexLiteralPattern.MatchString(combined) {
			result = append(result, combined)
		} else {
			result = append(result, regexTokens...)
		}
		i-- // outer loop's i++ will advance past the last consumed token
	}
	return result
}
