package lizardcc

import "strings"

// This file is the shared token-driving loop for every GoLikeStates-derived
// reader in this PR (Kotlin, Scala, Swift) -- the equivalent of clike.go's
// Analyze, but factored out once since none of the three differ in how
// tokens flow, only in tokenizer addition, condition set, preprocess, and
// which machine drives them.

// filterWhitespaceKeepNewline ports the generic `preprocessing()` fallback
// (lizard.py:526-529) used by any reader without its own `.preprocess`
// (Scala, scala.go): drop every all-whitespace token except a bare "\n".
func filterWhitespaceKeepNewline(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, tok := range raw {
		if !isAllSpace(tok) || tok == "\n" {
			out = append(out, tok)
		}
	}
	return out
}

// preprocessSwiftLabel ports SwiftReplaceLabel.preprocess (swift.py:8-21),
// used by both Kotlin and Swift (kotlin.py:9, swift.py's own class): filter
// whitespace exactly like filterWhitespaceKeepNewline, then rewrite an
// argument label that collides with a condition keyword (swiftlabel.go).
func preprocessSwiftLabel(raw []string, alphaConditions []string) []string {
	return ReplaceArgumentLabels(filterWhitespaceKeepNewline(raw), alphaConditions)
}

// runGoLikeFamily drives an already-whitespace-filtered token list through
// comment_counter, line_counter (Newline-tracking only; a bare "\n" is
// consumed, never forwarded, matching lizard.py:554-568 exactly) and
// condition_counter, then into root -- the single top-level subMachine for
// the whole file (a *kotlinMachine, *scalaMachine or *swiftMachine).
func runGoLikeFamily(tokens []string, conditions map[string]bool, root subMachine, ctx *Context) ([]int, bool, error) {
	newlineLocal := true // line_counter's initial local `newline = 1`
	for _, tok := range tokens {
		if tok == "\n" {
			newlineLocal = true
			continue
		}
		if isComment(tok) {
			// comment_counter re-emits one "\n" per embedded newline
			// (lizard.py:536); this package never needs the individual
			// tokens, only the practical effect on the Newline flag.
			if strings.Contains(tok, "\n") {
				newlineLocal = true
			}
			// Same `#lizard forgive`/"GENERATED CODE" handling as
			// clike.go's Analyze -- see HandleCommentDirectives' doc
			// (counter.go). Every GoLikeStates-derived reader (Go, Rust,
			// Kotlin, Scala, Swift) shares this one driver, so the fix
			// lands once here rather than five times.
			if ctx.HandleCommentDirectives(tok) {
				break
			}
			continue
		}
		ctx.Newline = strings.Contains(tok, "\n") || newlineLocal
		newlineLocal = false
		if conditions[tok] {
			ctx.AddCondition(1)
		}
		root.feed(tok)
	}
	// Ports the top-level EOF sweep (lizard.py:614's
	// `for state in self.parallel_states: state.statemachine_before_return()`,
	// code_reader.py:61-62's default no-op). Only ScalaStates overrides it
	// (scala.go); Kotlin/Swift's root types simply don't implement this
	// optional interface, so the assertion is skipped for them.
	if r, ok := root.(interface{ beforeReturn() }); ok {
		r.beforeReturn()
	}
	return ctx.Complexities, false, nil
}
