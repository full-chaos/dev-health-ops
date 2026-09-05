package lizardcc

// ReplaceArgumentLabels ports SwiftReplaceLabel.preprocess (swift.py:8-21),
// shared by SwiftReader and KotlinReader (kotlin.py:9, via the same
// mixin). Swift and Kotlin both allow any keyword as a call's argument
// label -- `foo(if: x)` or `foo(catch: y)` are ordinary calls, not an `if`
// or `catch` statement. Left alone, condition_counter would score the
// label as a real control-flow keyword and inflate the enclosing
// function's complexity by one for every such call. This rewrites the
// label to `_if`/`_catch`/... in place, immediately after '(' or ',' and
// immediately before ':' -- the only shape an argument label can take;
// everywhere else the same keyword still means what it always means.
//
// Built now, ahead of the Swift/Kotlin readers themselves (a later PR),
// because it is pure token-list preprocessing with no dependency on
// GoLikeStates: both readers can call it unchanged once they land.
//
// tokens must already have whitespace filtered out, matching Swift's own
// preprocess (swift.py:9, `t for t in tokens if not t.isspace() or t ==
// '\n'`) -- this package's own Preprocess (tokenize.go) already does that
// filtering for its own directive handling, so callers normally chain the
// two rather than filtering twice.
//
// alphaConditions is the reader's alphabetic condition keywords (swift.py's
// own generator expression: `k for k in self.conditions if k.isalpha()` --
// only a keyword can ever collide with an argument label; `&&`/`||`/`?`
// never can, so restricting to alpha entries is not an optimisation, it is
// the actual Python filter).
func ReplaceArgumentLabels(tokens []string, alphaConditions []string) []string {
	out := make([]string, len(tokens))
	copy(out, tokens)
	for _, k := range alphaConditions {
		replaceLabel(out, [3]string{"(", k, ":"})
		replaceLabel(out, [3]string{",", k, ":"})
	}
	return out
}

// replaceLabel finds every occurrence of the 3-token window `target` and
// rewrites its middle token to "_"+target[1], in place -- ports the nested
// replace_label closure (swift.py:11-17).
//
// Python's loop is `for i in range(0, len(tokens) - len(target))`, which
// stops one window short of the true end (range()'s stop is exclusive, and
// the correct stop for checking every window is `len(tokens)-len(target)+1`)
// -- the very last possible window is never inspected. Ported exactly as
// written rather than "fixed": a label in the final three tokens of a file
// is a missed rewrite in real lizard too, and this must reproduce that,
// not improve on it.
func replaceLabel(tokens []string, target [3]string) {
	n := len(tokens) - len(target)
	for i := 0; i < n; i++ {
		if tokens[i] == target[0] && tokens[i+1] == target[1] && tokens[i+2] == target[2] {
			tokens[i+1] = "_" + target[1]
		}
	}
}
