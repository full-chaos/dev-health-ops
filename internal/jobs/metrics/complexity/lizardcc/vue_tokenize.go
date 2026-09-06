package lizardcc

// This file ports lizard_languages/vue.py's VueReader.generate_tokens
// (vue.py:19-22): TypeScriptReader's own tokenizer pipeline
// (typescript_tokenize.go), with ONE extra alternative appended to the
// base pattern -- an HTML/Vue block tag, `<tag ...>` or `</tag>` (Python:
// `r"|(?:\<\/?\w+.*?\>)"`, `\w` widened to this package's shared Unicode
// identifier class per tokenize.go's own established `\w`-is-ASCII-only
// fix). This is what lets vuePreprocess (vue.go) recognise `<script...>`/
// `</script>` as single opaque tokens to key off of, rather than each tag
// exploding into "<","script","setup",">" etc. through the base pattern's
// ordinary per-character/per-word alternatives.
//
// The non-greedy `.*?` before the closing `>` is DOTALL-scoped (buildTokenPattern's
// global `(?s)`), matching Python's `re.S`-wide compilation exactly: a tag
// whose attributes span multiple lines (`<script\n  lang="ts">`) still
// tokenizes as ONE token, and an unterminated `<` with no `>` anywhere
// later in the file swallows everything after it into one runaway token --
// a real, faithfully-reproduced consequence of this exact regex, not a
// bug this port introduces.
const vueTagAddition = `|(?:<\/?[\p{L}\p{N}_]+.*?>)`

var vueTokenPattern = buildTokenPattern(tsAddition + vueTagAddition)

// GenerateTokensVue runs the shared TypeScript/JavaScript/Vue tokenizer
// pipeline (typescript_tokenize.go's generateTokensTSWith) against Vue's
// own, wider pattern.
func GenerateTokensVue(source string) []string {
	return generateTokensTSWith(vueTokenPattern, source)
}
