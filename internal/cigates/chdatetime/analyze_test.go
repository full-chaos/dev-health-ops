package chdatetime_test

import (
	"strings"
)

type class string

const (
	classSafe       class = "safe"
	classSkip       class = "skip"
	classFlag       class = "flag"
	classUnmeasured class = "unmeasured"
)

// A DateTime bound against a DateTime64 column is the unsafe pair: ClickHouse analyses a strict comparison of the two
// types wrongly in a key, partition or skip index. A Date bound and every DateTime64 bound are measured safe (the
// executed cells of CHAOS-7290).
var (
	safeFuncs = map[string]bool{
		"todatetime64": true, "now64": true, "fromunixtimestamp64milli": true, "fromunixtimestamp64micro": true,
		"fromunixtimestamp64nano": true, "parsedatetime64besteffort": true, "todate": true, "todate32": true,
		"today": true, "yesterday": true, "parsedatetime64": true,
	}
	flagFuncs = map[string]bool{"todatetime": true, "now": true, "parsedatetimebesteffort": true, "parsedatetime": true}
	castFuncs = map[string]bool{"cast": true, "_cast": true, "accuratecast": true, "accuratecastornull": true}
)

var boundaryWords = []string{
	"AND", "OR", "NOT", "WHERE", "PREWHERE", "ON", "HAVING", "WHEN", "THEN", "ELSE", "CASE", "END", "SELECT", "FROM", "JOIN",
	"USING", "GROUP", "ORDER", "LIMIT", "OFFSET", "UNION", "SETTINGS", "FORMAT", "IN", "BETWEEN", "IS", "LIKE", "ILIKE", "AS",
	"QUALIFY", "WINDOW", "BY", "FILTER",
}

func isBoundary(t tok) bool {
	switch {
	case t.isPunct(","), t.isPunct(";"), t.isPunct("["), t.isPunct("]"):
		return true
	case t.isOp("=", "<", ">", "<=", ">=", "<>", "!=", "==", "=>", "->"):
		return true
	}
	return t.isKeyword(boundaryWords...)
}

func leftOperand(toks []tok, i int) []tok {
	depth, j := 0, i-1
	for ; j >= 0; j-- {
		switch t := toks[j]; {
		case t.isPunct(")"):
			depth++
		case t.isPunct("("):
			if depth == 0 {
				return toks[j+1 : i]
			}
			depth--
		case depth == 0 && isBoundary(t):
			return toks[j+1 : i]
		}
	}
	return toks[:i]
}

func rightOperand(toks []tok, i int) []tok {
	depth, j := 0, i+1
	for ; j < len(toks); j++ {
		switch t := toks[j]; {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			if depth == 0 {
				return toks[i+1 : j]
			}
			depth--
		case depth == 0 && isBoundary(t):
			return toks[i+1 : j]
		}
	}
	return toks[i+1:]
}

// matchClose returns the index of the ")" that closes the "(" at open, or -1.
func matchClose(toks []tok, open int) int {
	depth := 0
	for i := open; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

func stripParens(toks []tok) []tok {
	for len(toks) >= 2 && toks[0].isPunct("(") && matchClose(toks, 0) == len(toks)-1 {
		toks = toks[1 : len(toks)-1]
	}
	return toks
}

// splitDepth0 splits on the tokens accepted by split that sit outside parentheses.
func splitDepth0(toks []tok, split func(tok) bool) [][]tok {
	var parts [][]tok
	depth, start := 0, 0
	for i, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && split(t):
			parts = append(parts, toks[start:i])
			start = i + 1
		}
	}
	return append(parts, toks[start:])
}

func typeText(toks []tok) string {
	var b strings.Builder
	for _, t := range toks {
		b.WriteString(t.Text)
	}
	return strings.Trim(b.String(), "'\" ")
}

func classifyType(text string) class {
	lower := strings.ToLower(strings.TrimSpace(text))
	lower = strings.TrimPrefix(lower, "nullable(")
	switch {
	case strings.HasPrefix(lower, "datetime64"):
		return classSafe
	case strings.HasPrefix(lower, "datetime"):
		return classFlag
	case strings.HasPrefix(lower, "date"):
		return classSafe
	case strings.HasPrefix(lower, "timestamp"):
		return classSkip
	}
	return classUnmeasured
}

// analyze reads one operand: the class of its type and, when the operand is a plain column reference whose name is a
// DateTime64 column, that name. Arithmetic keeps the type of its first term.
func analyze(toks []tok, names map[string]bool) (class, string) {
	toks = stripParens(toks)
	if len(toks) == 0 {
		return classUnmeasured, ""
	}
	term := splitDepth0(toks, func(t tok) bool { return t.isOp("+", "-", "*", "/", "||") })[0]
	if len(term) == 0 {
		if len(toks) > 1 && toks[0].isOp("-") && toks[1].Kind == tNumber {
			return classSkip, ""
		}
		return classUnmeasured, ""
	}
	if parts := splitDepth0(term, func(t tok) bool { return t.isOp("::") }); len(parts) > 1 {
		return classifyType(typeText(parts[len(parts)-1])), ""
	}
	if len(term) != len(toks) {
		if term = stripParens(term); len(term) == 0 {
			return classUnmeasured, ""
		}
	}
	if term[0].isPunct("(") {
		return analyze(term, names)
	}
	if len(term) == 1 {
		switch t := term[0]; t.Kind {
		case tQMark:
			return classFlag, ""
		case tDollar, tNumber:
			return classSkip, ""
		case tParam:
			return classifyType(t.Text), ""
		case tString:
			return classSafe, ""
		case tTmpl:
			return classUnmeasured, ""
		case tIdent, tQuoted:
			if names[t.Text] {
				return classSafe, t.Text
			}
			return classUnmeasured, ""
		}
		return classUnmeasured, ""
	}
	if isPath(term) {
		if last := term[len(term)-1].Text; names[last] {
			return classSafe, last
		}
		return classUnmeasured, ""
	}
	if term[0].Kind == tIdent && term[1].isPunct("(") && matchClose(term, 1) == len(term)-1 {
		name := strings.ToLower(term[0].Text)
		args := splitDepth0(term[2:len(term)-1], func(t tok) bool { return t.isPunct(",") })
		switch {
		case castFuncs[name]:
			if len(args) == 2 {
				return classifyType(typeText(args[1])), ""
			}
			if parts := splitDepth0(args[0], func(t tok) bool { return t.isKeyword("AS") }); len(parts) > 1 {
				return classifyType(typeText(parts[1])), ""
			}
		case safeFuncs[name]:
			return classSafe, ""
		case flagFuncs[name]:
			return classFlag, ""
		}
	}
	return classUnmeasured, ""
}

func isPath(toks []tok) bool {
	if len(toks)%2 == 0 {
		return false
	}
	for i, t := range toks {
		if i%2 == 0 && t.Kind != tIdent && t.Kind != tQuoted || i%2 == 1 && !t.isPunct(".") {
			return false
		}
	}
	return true
}

func render(toks []tok, text string) string {
	if len(toks) == 0 {
		return ""
	}
	last := toks[len(toks)-1]
	end := last.Pos + len(last.Text) + 2
	return snippet(text[toks[0].Pos:min(end, len(text))])
}

func snippet(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 60 {
		s = s[:60]
	}
	return s
}

type hit struct {
	File   string
	Line   int
	Column string
	Op     string
	Bound  string
	Class  class
}

// text is one SQL text: a Go string constant or a .sql file.
type text struct {
	File string
	Line int
	Body string
}

func pairHit(t text, op string, pos int, left, right []tok, names map[string]bool) (hit, bool) {
	if len(left) == 0 || len(right) == 0 {
		return hit{}, false
	}
	leftClass, leftCol := analyze(left, names)
	rightClass, rightCol := analyze(right, names)
	h := hit{File: t.File, Line: t.Line + strings.Count(t.Body[:pos], "\n"), Op: op}
	switch {
	case leftCol != "" && rightCol != "":
		h.Column, h.Class, h.Bound = leftCol, classSafe, render(right, t.Body)
	case leftCol != "":
		h.Column, h.Class, h.Bound = leftCol, rightClass, render(right, t.Body)
	case rightCol != "":
		h.Column, h.Class, h.Bound = rightCol, leftClass, "(bound before the operator) "+render(left, t.Body)
	default:
		return hit{}, false
	}
	return h, true
}

// strictHits finds every strict comparison of a DateTime64 column (a name in names) in one SQL text: the operators `<`
// and `>` and the functions less(a, b) and greater(a, b). A comparison that ends the text has no bound to read and is
// left to the text that continues it.
func strictHits(t text, names map[string]bool) []hit {
	toks := lex(t.Body)
	var out []hit
	for i, tk := range toks {
		switch {
		case tk.isOp("<", ">"):
			if h, ok := pairHit(t, tk.Text, tk.Pos, leftOperand(toks, i), rightOperand(toks, i), names); ok {
				out = append(out, h)
			}
		case tk.Kind == tIdent && (strings.EqualFold(tk.Text, "less") || strings.EqualFold(tk.Text, "greater")) &&
			i+1 < len(toks) && toks[i+1].isPunct("("):
			closeAt := matchClose(toks, i+1)
			if closeAt < 0 {
				continue
			}
			args := splitDepth0(toks[i+2:closeAt], func(x tok) bool { return x.isPunct(",") })
			if len(args) != 2 {
				continue
			}
			if h, ok := pairHit(t, strings.ToLower(tk.Text), tk.Pos, args[0], args[1], names); ok {
				out = append(out, h)
			}
		}
	}
	return out
}
