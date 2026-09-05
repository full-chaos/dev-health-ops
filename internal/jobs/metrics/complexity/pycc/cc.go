package pycc

import "strings"

// This file ports radon 6.0.1's cyclomatic-complexity rules
// (radon/visitors.py:225-330) to Go. Every rule below cites the radon line
// it reproduces, because the acceptance test for this package is numeric
// equality with radon on a fixed corpus -- not "looks like CC".
//
// RADON'S RULES, verbatim from ComplexityVisitor.generic_visit:
//
//	Try/TryExcept  += len(node.handlers) + bool(node.orelse)
//	BoolOp         += len(node.values) - 1
//	If/IfExp       += 1
//	Match          += max(0, len(node.cases) - <a bare capture case exists>)
//	For/While/     += bool(node.orelse) + 1
//	  AsyncFor
//	comprehension  += len(node.ifs) + 1
//	Assert         += 1 unless no_assert
//	Lambda            NOT counted (radon issue #68)
//
// TOKEN-LEVEL EQUIVALENCE, argued per rule rather than assumed.
//
// Several AST rules collapse onto a single keyword count, and where they do
// the equality is exact, not approximate:
//
//   - `if`/`elif`: an If statement, an elif (which is a nested If), a
//     ternary IfExp, and a comprehension's `ifs` all contribute exactly 1
//     per `if` keyword. The comprehension rule's len(ifs) term is therefore
//     already covered by counting `if` tokens inside it.
//   - `for`: a For/AsyncFor statement contributes 1, and a comprehension's
//     own +1 is contributed by its `for`. One count serves both.
//   - `and`/`or`: BoolOp adds len(values)-1, and an n-operand BoolOp is
//     written with exactly n-1 operators, so counting operators is the
//     same number. `a and b and c` is one BoolOp with 3 values (+2) and
//     two operators (+2).
//   - `except`: one handler each, so len(handlers) is the count.
//
// Two rules genuinely need block structure and are NOT keyword counts:
//
//   - `else` contributes 1 only for for/while/try (bool(orelse)); an
//     if/elif `else` contributes 0. Which opener an `else` belongs to is
//     recoverable only from the suite tree, so that is what buildSuite
//     produces.
//   - `match`/`case` are soft keywords. `match` is a statement only in
//     statement position with a `case` suite under it; elsewhere it is an
//     ordinary identifier, and this repository does bind regex results to a
//     variable called `match`.

// Block is one unit radon reports: a function, a class, or a method.
// radon's `blocks` property (visitors.py:196-207) is exactly
// top-level functions + each class + that class's methods. Closures are
// deliberately absent -- they hang off their parent Function and are never
// counted as blocks, nor does their complexity reach the parent.
type Block struct {
	Name       string
	Kind       BlockKind
	Line       int
	Col        int
	Complexity int
}

// BlockKind distinguishes the three block flavours radon emits.
type BlockKind int

const (
	// BlockFunction is a module-level (or otherwise non-method) function.
	BlockFunction BlockKind = iota
	// BlockClass is a class. Its complexity folds in its methods'
	// complexity, which is why a class and its methods both appear.
	BlockClass
	// BlockMethod is a function defined directly in a class body.
	BlockMethod
)

// Options mirrors the knobs radon's caller actually sets.
type Options struct {
	// NoAssert drops the Assert rule, matching radon's no_assert.
	// analytics/complexity.py never sets it, so the default (false, i.e.
	// asserts DO count) is the production behaviour.
	NoAssert bool
}

// Visit computes the blocks for one Python source file, reproducing
// radon.complexity.cc_visit.
//
// An error is returned for source this package cannot lex. The caller must
// treat that the way analytics/complexity.py:219-222 treats an exception
// from cc_visit -- skip the file entirely, producing NO row, rather than
// recording a zero. A zero and a skip are different rows downstream, and
// conflating them is how a parser regression would masquerade as a genuine
// drop in complexity.
func Visit(src string, opts Options) ([]Block, error) {
	tokens, err := Tokenize(src)
	if err != nil {
		return nil, err
	}
	suite, err := buildSuite(tokens)
	if err != nil {
		return nil, err
	}
	v := &visitor{opts: opts}
	v.visitSuite(suite, false)
	return v.blocks, nil
}

type visitor struct {
	opts   Options
	blocks []Block
}

// visitSuite walks one suite, emitting the blocks radon's `blocks` property
// reports: top-level functions, top-level classes, and each class's
// methods.
//
// TWO EXCLUSIONS, both verified against radon rather than inferred, because
// both look like omissions until you trace them:
//
//   - CLOSURES. visit_FunctionDef collects nested functions into the parent
//     Function's `closures` field (visitors.py:276-295) and `blocks` never
//     reads that field. So a nested def is neither a block of its own nor
//     part of its parent's complexity.
//   - NESTED CLASSES, and any class defined inside a function. `blocks`
//     iterates `self.classes` and each class's `methods`
//     (visitors.py:196-207); an inner class lands in the PARENT class's
//     `inner_classes` instead, and visit_FunctionDef discards a child
//     visitor's `classes` entirely. Either way it never reaches `blocks`,
//     and neither do its methods.
//
// Descending into ordinary compound statements IS correct: radon's
// generic_visit walks into an `if TYPE_CHECKING:` or a `try:` guard, so a
// def or class declared there is a genuine module-level block.
func (v *visitor) visitSuite(s *suite, inClass bool) {
	for _, st := range s.stmts {
		switch st.kind {
		case stmtDef:
			kind := BlockFunction
			if inClass {
				kind = BlockMethod
			}
			v.blocks = append(v.blocks, Block{
				Name:       st.name,
				Kind:       kind,
				Line:       st.line,
				Col:        st.col,
				Complexity: functionComplexity(st, v.opts),
			})
		case stmtClass:
			if inClass {
				// An inner class is not a block, and neither are its
				// methods -- so the walk stops here entirely.
				continue
			}
			v.blocks = append(v.blocks, Block{
				Name:       st.name,
				Kind:       BlockClass,
				Line:       st.line,
				Col:        st.col,
				Complexity: classComplexity(st, v.opts),
			})
			for _, m := range collectMethods(st.body) {
				v.blocks = append(v.blocks, Block{
					Name:       m.name,
					Kind:       BlockMethod,
					Line:       m.line,
					Col:        m.col,
					Complexity: functionComplexity(m, v.opts),
				})
			}
		default:
			if st.body != nil {
				v.visitSuite(st.body, inClass)
			}
		}
	}
}

// collectMethods returns a class body's methods in radon's sense: every def
// reachable without crossing into another def or class.
//
// The descent through plain compound statements matters -- a method guarded
// by `if sys.version_info >= (3, 12):` is still a method, because radon's
// per-child visitor walks into the If and visit_FunctionDef fires there.
func collectMethods(body *suite) []*stmt {
	if body == nil {
		return nil
	}
	var methods []*stmt
	var walk func(s *suite)
	walk = func(s *suite) {
		if s == nil {
			return
		}
		for _, st := range s.stmts {
			switch st.kind {
			case stmtDef:
				methods = append(methods, st)
			case stmtClass:
				// Stop: an inner class's defs belong to it, not here.
			default:
				walk(st.body)
			}
		}
	}
	walk(body)
	return methods
}

// functionComplexity reproduces visit_FunctionDef (visitors.py:268-295):
// body_complexity starts at 1 and accumulates each child's own complexity,
// explicitly EXCLUDING nested functions' complexity.
func functionComplexity(fn *stmt, opts Options) int {
	return 1 + suiteComplexity(fn.body, opts)
}

// classRealComplexity reproduces visit_ClassDef's body_complexity
// (visitors.py:297-321):
//
//	body_complexity = 1 + Σ_child (complexity + functions_complexity + len(functions))
//
// functions_complexity is `sum(method complexities) - len(methods)`
// (visitors.py:169-176), so the `- len(methods)` and the `+ len(functions)`
// cancel exactly and each method contributes its FULL complexity.
//
// An inner ClassDef child contributes 0, not its own complexity: the child
// visitor's `complexity` is untouched by visit_ClassDef and its `functions`
// list stays empty, because the inner class's methods are recorded on the
// inner Class object instead. Adding the inner class here was this port's
// first parity bug.
func classRealComplexity(cls *stmt, opts Options) int {
	if cls.body == nil {
		return 1
	}
	total := 1
	for _, st := range cls.body.stmts {
		switch st.kind {
		case stmtDef:
			total += functionComplexity(st, opts)
		case stmtClass:
			// Contributes nothing -- see the doc comment above.
		default:
			total += stmtComplexity(st, opts)
			for _, m := range collectMethods(st.body) {
				// A def nested inside a compound statement in the class
				// body is still a method, and its complexity still folds
				// in through the same child visitor.
				total += functionComplexity(m, opts)
			}
		}
	}
	return total
}

// classComplexity is what radon REPORTS for a class, which is not its
// body_complexity: Class.complexity (visitors.py, Class property) is an
// AVERAGE over the class's methods --
//
//	no methods -> real_complexity
//	otherwise  -> int(real_complexity / len(methods)) + (len(methods) > 1)
//
// The `+ (methods > 1)` term is a bare bool added to an int, so it
// contributes exactly 1 for a class with two or more methods and 0 for a
// class with one. Summing the methods instead (the obvious reading of
// visit_ClassDef) over-reports every multi-method class -- it scored
// `Simple` 6 against radon's 4 until this property was read.
func classComplexity(cls *stmt, opts Options) int {
	real := classRealComplexity(cls, opts)
	methods := collectMethods(cls.body)
	if len(methods) == 0 {
		return real
	}
	avg := real / len(methods) // int() truncation toward zero; real is > 0
	if len(methods) > 1 {
		avg++
	}
	return avg
}

// suiteComplexity sums the decision points of a suite WITHOUT descending
// into nested def/class, matching the "add general complexity but not
// closures' complexity" comment at visitors.py:285.
func suiteComplexity(s *suite, opts Options) int {
	if s == nil {
		return 0
	}
	total := 0
	for _, st := range s.stmts {
		if st.kind == stmtDef || st.kind == stmtClass {
			continue
		}
		total += stmtComplexity(st, opts)
	}
	return total
}

// stmtComplexity scores one statement and its nested suites.
func stmtComplexity(st *stmt, opts Options) int {
	total := inlineComplexity(st, opts)

	switch st.kind {
	case stmtIf:
		// The `if`/`elif` keyword itself is counted by inlineComplexity,
		// which sees it in the statement's own tokens.
	case stmtFor, stmtWhile:
		// The opening keyword is counted inline; bool(orelse) is added
		// by the else-clause statement itself (see stmtElse).
	case stmtExcept:
		// One handler, counted inline.
	case stmtMatch:
		total += matchComplexity(st)
	}

	total += suiteComplexity(st.body, opts)
	return total
}

// inlineComplexity counts the decision points visible in a statement's own
// token run: its opening keyword plus anything in its expressions.
func inlineComplexity(st *stmt, opts Options) int {
	total := 0
	for i, tok := range st.tokens {
		if tok.Kind != TokenName {
			continue
		}
		switch tok.Text {
		case "if", "elif":
			// Covers the If statement, an elif, a ternary IfExp, and a
			// comprehension's ifs -- all +1 each, all the same rule.
			total++
		case "for":
			// A for statement, an async-for statement, and a
			// comprehension's for: +1 each (visitors.py:249-254).
			total++
		case "while":
			total++
		case "except":
			total++
		case "and", "or":
			// BoolOp: len(values)-1 == number of operators.
			total++
		case "assert":
			if !opts.NoAssert {
				total++
			}
		case "else":
			// `else` on a for/while/try adds 1; on an if/elif it adds
			// nothing. Which one this is was decided when the suite tree
			// was built, so the token itself is inert here.
			_ = i
		}
	}
	if st.kind == stmtElse && st.elseOwner != stmtIf {
		// bool(node.orelse) for For/While/Try (visitors.py:232, 248).
		total++
	}
	return total
}

// matchComplexity reproduces the Match rule (visitors.py:240-245):
//
//	complexity += max(0, len(cases) - contain_underscore)
//
// where contain_underscore is a BOOLEAN -- so at most 1 is subtracted no
// matter how many bare captures exist.
//
// radon's test is `getattr(case.pattern, "pattern", False) is None`, which
// is true for any MatchAs carrying no sub-pattern. That covers `case _:`
// AND a bare capture like `case other:` -- both are irrefutable and act as
// the else. Matching only on `_` would over-count every match statement
// whose fallback is a named capture.
func matchComplexity(st *stmt) int {
	if st.body == nil {
		return 0
	}
	cases := 0
	bareCapture := false
	for _, child := range st.body.stmts {
		if child.kind != stmtCase {
			continue
		}
		cases++
		if isBareCapturePattern(child.tokens) {
			bareCapture = true
		}
	}
	n := cases
	if bareCapture {
		n--
	}
	if n < 0 {
		return 0
	}
	return n
}

// isBareCapturePattern reports whether a `case` clause is an irrefutable
// capture: `case _:` or `case name:` and nothing more. Any dot, bracket,
// comma, literal, guard or class pattern makes it refutable.
func isBareCapturePattern(tokens []Token) bool {
	// tokens[0] is the soft keyword `case`; the pattern runs to the colon.
	body := make([]Token, 0, len(tokens))
	for _, tok := range tokens[1:] {
		if tok.Kind == TokenOp && tok.Text == ":" {
			break
		}
		body = append(body, tok)
	}
	if len(body) != 1 {
		return false
	}
	return body[0].Kind == TokenName && !IsKeyword(body[0].Text)
}

// TotalComplexity sums a block list the way analytics/complexity.py's
// _build_result does: a plain sum over every block's complexity.
func TotalComplexity(blocks []Block) int {
	total := 0
	for _, b := range blocks {
		total += b.Complexity
	}
	return total
}

// CountAbove reports how many blocks exceed a threshold. The Python side
// uses a strict `>` (analytics/complexity.py:248-249), so a block exactly
// at the threshold is NOT counted.
func CountAbove(blocks []Block, threshold int) int {
	n := 0
	for _, b := range blocks {
		if b.Complexity > threshold {
			n++
		}
	}
	return n
}

// LineCount reproduces `len(code.splitlines())`, which is what the Python
// side stores as `loc`. Go's strings.Split would report one extra field for
// a trailing newline, so this is written to match splitlines exactly: a
// trailing line terminator does NOT create an empty final line.
func LineCount(code string) int {
	if code == "" {
		return 0
	}
	normalized := strings.ReplaceAll(code, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	n := strings.Count(normalized, "\n")
	if !strings.HasSuffix(normalized, "\n") {
		n++
	}
	return n
}
