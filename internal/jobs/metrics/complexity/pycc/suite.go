package pycc

// suite.go turns the token stream into the minimum tree the complexity
// rules need: statements, their nested suites, and enough classification to
// answer two questions a flat keyword count cannot.
//
//  1. Which opener owns an `else`? radon adds 1 for a for/while/try `else`
//     (bool(node.orelse)) and 0 for an if/elif `else`, because the If node
//     already counted. Only sibling order tells them apart.
//  2. Is this `match` a statement or a variable? PEP 634 made match/case
//     SOFT keywords: reserved only in statement position with a case suite
//     beneath. This repository binds regex results to `match` routinely, so
//     treating it as a hard keyword would invent decision points in
//     ordinary code.

type stmtKind int

const (
	stmtOther stmtKind = iota
	stmtDef
	stmtClass
	stmtIf
	stmtElse
	stmtFor
	stmtWhile
	stmtTry
	stmtExcept
	stmtFinally
	stmtWith
	stmtMatch
	stmtCase
)

type suite struct {
	stmts []*stmt
}

type stmt struct {
	kind stmtKind
	name string
	line int
	col  int
	// tokens is the statement's own header run: everything up to and
	// including the colon that opens its suite, or the whole logical line
	// for a simple statement. Decision points inside the header (a ternary
	// in a default argument, a comprehension in a call) are counted from
	// here, which is what makes `x = [i for i in y if i]` score.
	tokens []Token
	body   *suite
	// elseOwner is meaningful only for stmtElse and records which opener
	// this else belongs to, so the +1 is applied exactly when radon
	// applies it.
	elseOwner stmtKind
}

type parser struct {
	tokens []Token
	pos    int
}

// buildSuite parses a whole token stream into a module-level suite.
func buildSuite(tokens []Token) (*suite, error) {
	p := &parser{tokens: tokens}
	return p.parseSuite(), nil
}

func (p *parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Kind: TokenEOF}
	}
	return p.tokens[p.pos]
}

// parseSuite consumes statements until the suite ends (a DEDENT that closes
// it, or EOF).
func (p *parser) parseSuite() *suite {
	s := &suite{}
	for {
		tok := p.peek()
		switch tok.Kind {
		case TokenEOF:
			return s
		case TokenDedent:
			return s
		case TokenNewline, TokenIndent:
			// A stray INDENT here means a suite opened without a header
			// this parser recognised; consuming it keeps the stream
			// aligned rather than desynchronising the whole file.
			p.pos++
			continue
		}
		st := p.parseStatement(s)
		if st == nil {
			return s
		}
		s.stmts = append(s.stmts, st)
	}
}

// parseStatement reads one logical line and, if it opens a suite, that
// suite. parent is needed only to resolve an `else` against its siblings.
func (p *parser) parseStatement(parent *suite) *stmt {
	start := p.peek()
	if start.Kind == TokenEOF {
		return nil
	}

	header := make([]Token, 0, 8)
	colonAt := -1
	// depth tracks bracket nesting WITHIN this header. The tokenizer emits
	// every bracket and colon flatly regardless of nesting (it only uses
	// depth to decide whether a physical newline is significant), so this
	// parser must track it itself: a parameter annotation's colon in
	// `def f(x: int):` sits inside the `(...)`, and picking the FIRST
	// colon in the flat header without depth-awareness selects it, then
	// treats `):`'s trailing colon as garbage and drops the entire
	// function body -- CHAOS-4291 r1 P1.
	depth := 0
	for {
		tok := p.peek()
		if tok.Kind == TokenEOF || tok.Kind == TokenNewline {
			break
		}
		if tok.Kind == TokenIndent || tok.Kind == TokenDedent {
			break
		}
		if tok.Kind == TokenOp {
			switch tok.Text {
			case "(", "[", "{":
				depth++
			case ")", "]", "}":
				if depth > 0 {
					depth--
				}
			case ":":
				// The FIRST top-level (depth 0) colon opens the suite.
				// A colon inside brackets belongs to a dict, a slice, a
				// parameter/variable annotation, or a lambda used as an
				// argument -- never a suite opener.
				if depth == 0 && colonAt < 0 {
					colonAt = len(header)
				}
			}
		}
		header = append(header, tok)
		p.pos++
	}
	if p.peek().Kind == TokenNewline {
		p.pos++
	}

	if len(header) == 0 {
		return nil
	}

	st := &stmt{
		kind:   stmtOther,
		line:   start.Line,
		col:    start.Col,
		tokens: header,
	}
	classify(st, parent)

	// A suite follows either as an INDENT block, or inline after the colon
	// on the same line (`if x: return y`). Both branches also require
	// opensSuite(st.kind): a top-level colon that ends a header line by
	// coincidence (this parser does not fully validate every construct)
	// must never be treated as a suite opener for a statement kind that
	// cannot legally open one.
	if colonAt >= 0 && colonAt == len(header)-1 && opensSuite(st.kind) && p.peek().Kind == TokenIndent {
		p.pos++
		st.body = p.parseSuite()
		if p.peek().Kind == TokenDedent {
			p.pos++
		}
	} else if colonAt >= 0 && colonAt < len(header)-1 && opensSuite(st.kind) {
		inline := header[colonAt+1:]
		st.tokens = header[:colonAt+1]
		st.body = &suite{stmts: []*stmt{{
			kind:   stmtOther,
			line:   inline[0].Line,
			col:    inline[0].Col,
			tokens: inline,
		}}}
	}

	// `match` is only a statement if a `case` suite actually followed. If
	// not, it was an identifier and must score nothing extra.
	if st.kind == stmtMatch && !hasCaseChild(st.body) {
		st.kind = stmtOther
	}
	return st
}

func opensSuite(k stmtKind) bool {
	switch k {
	case stmtDef, stmtClass, stmtIf, stmtElse, stmtFor, stmtWhile,
		stmtTry, stmtExcept, stmtFinally, stmtWith, stmtMatch, stmtCase:
		return true
	}
	return false
}

func hasCaseChild(s *suite) bool {
	if s == nil {
		return false
	}
	for _, st := range s.stmts {
		if st.kind == stmtCase {
			return true
		}
	}
	return false
}

// classify assigns a statement kind from its opening keyword, and resolves
// an `else` to the opener that owns it.
func classify(st *stmt, parent *suite) {
	first := st.tokens[0]

	// `async def` / `async for` / `async with`: the kind is carried by the
	// second word, and radon treats AsyncFunctionDef and AsyncFor exactly
	// as their synchronous forms (visitors.py:262-266, 249).
	idx := 0
	if first.Kind == TokenName && first.Text == "async" && len(st.tokens) > 1 {
		idx = 1
		first = st.tokens[1]
	}
	if first.Kind != TokenName {
		return
	}

	switch first.Text {
	case "def":
		st.kind = stmtDef
		st.name = nameAfter(st.tokens, idx)
	case "class":
		st.kind = stmtClass
		st.name = nameAfter(st.tokens, idx)
	case "if", "elif":
		st.kind = stmtIf
	case "for":
		st.kind = stmtFor
	case "while":
		st.kind = stmtWhile
	case "try":
		st.kind = stmtTry
	case "except":
		st.kind = stmtExcept
	case "finally":
		st.kind = stmtFinally
	case "with":
		st.kind = stmtWith
	case "else":
		st.kind = stmtElse
		st.elseOwner = resolveElseOwner(parent)
	case "match":
		// Soft keyword: a match STATEMENT needs a trailing colon. A bare
		// `match = re.search(...)` has no colon and is left as stmtOther.
		if endsWithColon(st.tokens) {
			st.kind = stmtMatch
		}
	case "case":
		if endsWithColon(st.tokens) {
			st.kind = stmtCase
		}
	}
}

func endsWithColon(tokens []Token) bool {
	last := tokens[len(tokens)-1]
	return last.Kind == TokenOp && last.Text == ":"
}

func nameAfter(tokens []Token, idx int) string {
	if idx+1 < len(tokens) && tokens[idx+1].Kind == TokenName {
		return tokens[idx+1].Text
	}
	return ""
}

// resolveElseOwner walks back through the already-parsed siblings to find
// the opener this `else` closes.
//
// The search skips nothing and stops at the first opener it meets, which is
// what makes a try/except/else resolve to the TRY (its nearest preceding
// sibling is the last `except`) while an if/elif/else resolves to the IF.
// Returning stmtIf as the fallback is the safe default: it contributes 0,
// so a misparse can only ever under-count, never invent a decision point.
func resolveElseOwner(parent *suite) stmtKind {
	if parent == nil {
		return stmtIf
	}
	for i := len(parent.stmts) - 1; i >= 0; i-- {
		switch parent.stmts[i].kind {
		case stmtIf:
			return stmtIf
		case stmtFor:
			return stmtFor
		case stmtWhile:
			return stmtWhile
		case stmtExcept, stmtTry:
			// try/else. radon counts this through bool(node.orelse) on
			// the Try node (visitors.py:232), the same +1 a for/while
			// else gets.
			return stmtTry
		}
	}
	return stmtIf
}
