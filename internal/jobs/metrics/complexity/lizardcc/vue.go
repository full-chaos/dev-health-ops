package lizardcc

import "strings"

// This file ports lizard_languages/vue.py's VueReader.preprocess
// (vue.py:24-33) and AnalyzeVue. VueReader adds NO state-machine override
// at all -- no VueStates class exists in lizard, only a tokenizer addition
// (vue_tokenize.go) and this preprocess override -- so TypeScriptStates
// (typescript.go's tsMachine/tsRoot) runs completely unchanged for Vue,
// the same "no override, reuse directly" shape java.go/objc.go/lua.go use
// for their respective shared machinery.

// vuePreprocess ports VueReader.preprocess (vue.py:24-33): a `.vue`
// Single-File-Component's `<template>`/`<style>` sections and any bare
// HTML between blocks are discarded ENTIRELY (never reach
// TypeScriptStates at all -- not even as whitespace), and only the
// content strictly BETWEEN a `<script...>` tag and its matching
// `</script...>` tag survives, filtered through the SAME generic
// whitespace rule filterWhitespaceKeepNewline applies everywhere else
// (drop an all-whitespace token unless it's a bare "\n"). The opening and
// closing script tags THEMSELVES are also dropped -- Python's `if`/`elif`
// branches that flip current_block carry no `yield` of their own, only
// the third branch (current_block == "script") ever yields.
//
// PRESERVED, NOT FIXED: `current_block` is a single flag, not a stack --
// a SECOND `<script>` block (Vue SFCs commonly have TWO: a plain
// `<script>` and a `<script setup>`) is handled correctly in sequence
// (each one's own content is captured independently), but any tag whose
// token TEXT happens to start with the literal substring "<script"
// (matched via HasPrefix, no word-boundary or attribute-aware check --
// e.g. a hypothetical "<scripted-thing>" custom element tag) would be
// mistaken for a real `<script>` opener too, exactly as Python's own
// `token.startswith('<script')` would.
func vuePreprocess(tokens []string) []string {
	out := make([]string, 0, len(tokens))
	inScript := false
	for _, tok := range tokens {
		switch {
		case strings.HasPrefix(tok, "<script"):
			inScript = true
		case strings.HasPrefix(tok, "</script"):
			inScript = false
		case inScript:
			if !isAllSpace(tok) || tok == "\n" {
				out = append(out, tok)
			}
		}
	}
	return out
}

// AnalyzeVue is the AnalyzerFunc for Vue (.vue). It reuses tsConditions
// and tsRoot/newTSMachine (typescript.go) directly -- VueReader adds no
// condition-category override and no state-machine override of its own,
// only the tokenizer addition (vue_tokenize.go) and vuePreprocess above.
func AnalyzeVue(path, source string) ([]int, bool, error) {
	tokens := vuePreprocess(GenerateTokensVue(source))
	ctx := NewContext()
	ctx.SetPath(path)
	root := tsRoot{newTSMachine(ctx)}
	return runGoLikeFamily(tokens, tsConditions, root, ctx)
}
