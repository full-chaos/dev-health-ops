package chdatetime_test

import (
	"strings"
)

type tokKind int

const (
	tIdent  tokKind = iota // bare identifier or keyword
	tQuoted                // `name` or "name"
	tNumber
	tString
	tParam  // {name:Type}
	tQMark  // ?
	tDollar // $1
	tTmpl   // %s, %[1]s, %d
	tOp     // operator
	tPunct  // ( ) , ; . [ ] { }
)

type tok struct {
	Kind tokKind
	Text string // identifier without quotes; param: the type text; otherwise the source text
	Pos  int
}

func isIdentStart(c byte) bool { return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool      { return c >= '0' && c <= '9' }

// lex splits one SQL text into tokens. Comments are dropped; a string literal is one token whose content never reaches
// a comparison; a backtick or double-quoted identifier is one token that carries the name without the quotes.
func lex(s string) []tok {
	var out []tok
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				i = len(s)
			} else {
				i += end + 4
			}
		case c == '\'':
			start := i
			i++
			for i < len(s) {
				if s[i] == '\\' {
					i += 2
					continue
				}
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			if i < len(s) {
				i++
			}
			out = append(out, tok{tString, s[start:min(i, len(s))], start})
		case c == '`' || c == '"':
			start := i
			i++
			for i < len(s) && s[i] != c {
				if s[i] == '\\' {
					i++
				}
				i++
			}
			end := min(i, len(s))
			if i < len(s) {
				i++
			}
			out = append(out, tok{tQuoted, s[start+1 : end], start})
		case c == '{':
			if end, typ, ok := lexParam(s, i); ok {
				out = append(out, tok{tParam, typ, i})
				i = end
				continue
			}
			out = append(out, tok{tPunct, "{", i})
			i++
		case c == '?':
			out = append(out, tok{tQMark, "?", i})
			i++
		case c == '$' && i+1 < len(s) && isDigit(s[i+1]):
			start := i
			i++
			for i < len(s) && isDigit(s[i]) {
				i++
			}
			out = append(out, tok{tDollar, s[start:i], start})
		case c == '%':
			start := i
			j := i + 1
			if j < len(s) && s[j] == '[' {
				for j < len(s) && s[j] != ']' {
					j++
				}
				j++
			}
			if j < len(s) && (s[j] == 's' || s[j] == 'd' || s[j] == 'v' || s[j] == 'q') {
				out = append(out, tok{tTmpl, s[start : j+1], start})
				i = j + 1
				continue
			}
			out = append(out, tok{tOp, "%", i})
			i++
		case isIdentStart(c):
			start := i
			for i < len(s) && (isIdentStart(s[i]) || isDigit(s[i])) {
				i++
			}
			out = append(out, tok{tIdent, s[start:i], start})
		case isDigit(c):
			start := i
			for i < len(s) && (isDigit(s[i]) || s[i] == '.' && i+1 < len(s) && isDigit(s[i+1])) {
				i++
			}
			out = append(out, tok{tNumber, s[start:i], start})
		default:
			start := i
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "<=", ">=", "<>", "!=", "==", "->", "::", "||", "<<", ">>", "=>":
					out = append(out, tok{tOp, s[i : i+2], start})
					i += 2
					continue
				}
			}
			kind := tOp
			if strings.ContainsRune("(),;.[]}", rune(c)) {
				kind = tPunct
			}
			out = append(out, tok{kind, string(c), start})
			i++
		}
	}
	return out
}

// lexParam reads `{name:Type}` where Type may hold parentheses and quotes; it returns the index after the closing brace
// and the type text.
func lexParam(s string, open int) (end int, typ string, ok bool) {
	i := open + 1
	for i < len(s) && (isIdentStart(s[i]) || isDigit(s[i])) {
		i++
	}
	if i == open+1 || i >= len(s) || s[i] != ':' {
		return 0, "", false
	}
	typeStart := i + 1
	depth, quote := 0, false
	for j := typeStart; j < len(s); j++ {
		switch c := s[j]; {
		case quote && c == '\\':
			j++
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == '}' && depth == 0:
			t := strings.TrimSpace(s[typeStart:j])
			if t == "" || t[0] < 'A' || t[0] > 'Z' {
				return 0, "", false
			}
			return j + 1, t, true
		case c == '\n' || c == '{':
			return 0, "", false
		}
	}
	return 0, "", false
}

func (t tok) isKeyword(words ...string) bool {
	if t.Kind != tIdent {
		return false
	}
	for _, w := range words {
		if strings.EqualFold(t.Text, w) {
			return true
		}
	}
	return false
}

func (t tok) isOp(ops ...string) bool {
	if t.Kind != tOp {
		return false
	}
	for _, o := range ops {
		if t.Text == o {
			return true
		}
	}
	return false
}

func (t tok) isPunct(p string) bool { return t.Kind == tPunct && t.Text == p }
