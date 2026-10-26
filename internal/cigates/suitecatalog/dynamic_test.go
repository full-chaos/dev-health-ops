package suitecatalog_test

import (
	"regexp"
	"strings"
)

var (
	assignWord   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\+?=`)
	funcDefine   = regexp.MustCompile(`(?m)^\s*(?:function\s+([A-Za-z_][A-Za-z0-9_-]*)|([A-Za-z_][A-Za-z0-9_-]*)\s*\(\)\s*\{?)`)
	aliasDefine  = regexp.MustCompile(`(?m)^\s*alias\s+([A-Za-z_][A-Za-z0-9_-]*)=`)
	heredocStart = regexp.MustCompile(`<<-?\s*(?:'([A-Za-z0-9_]+)'|"([A-Za-z0-9_]+)"|\\?([A-Za-z0-9_]+))`)
)

// leadingKeywords introduce a command that follows them; the command word is what is left after them.
var leadingKeywords = map[string]bool{"if": true, "elif": true, "then": true, "else": true, "do": true, "while": true, "until": true, "!": true, "{": true, "(": true, "time": true, "exec": true, "command": true, "sudo": true, "nohup": true}

// skipClauses start a clause that runs no command word of its own.
var skipClauses = map[string]bool{"for": true, "case": true, "select": true, "esac": true, "fi": true, "done": true, "}": true, ")": true, ";;": true, "function": true, "in": true}

// stripHeredocs removes here-document bodies: their text is data, not commands.
func stripHeredocs(run string) string {
	var out []string
	end := ""
	for _, line := range strings.Split(run, "\n") {
		if end != "" {
			if strings.TrimSpace(line) == end {
				end = ""
			}
			continue
		}
		out = append(out, line)
		if m := heredocStart.FindStringSubmatch(line); m != nil {
			end = m[1] + m[2] + m[3]
		}
	}
	return strings.Join(out, "\n")
}

// splitCommands splits a run block on unquoted && || | ; and newlines, joining backslash continuations.
func splitCommands(run string) []string {
	run = strings.ReplaceAll(stripHeredocs(run), "\\\n", " ")
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	var quote byte
	depth := 0
	for i := 0; i < len(run); i++ {
		c := run[i]
		switch {
		case quote != 0:
			cur.WriteByte(c)
			if c == '\\' && quote == '"' && i+1 < len(run) {
				i++
				cur.WriteByte(run[i])
			} else if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
			cur.WriteByte(c)
		case c == '#' && (cur.Len() == 0 || strings.HasSuffix(cur.String(), " ")) && depth == 0:
			for i < len(run) && run[i] != '\n' {
				i++
			}
			flush()
		case c == '$' && i+1 < len(run) && run[i+1] == '(':
			depth++
			cur.WriteString("$(")
			i++
		case c == '(' && depth > 0:
			depth++
			cur.WriteByte(c)
		case c == ')' && depth > 0:
			depth--
			cur.WriteByte(c)
		case depth > 0:
			cur.WriteByte(c)
		case c == '\n' || c == ';':
			flush()
		case (c == '&' || c == '|') && i+1 < len(run) && run[i+1] == c:
			flush()
			i++
		case c == '|':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return out
}

// shellWords splits one command on unquoted white space, keeping each word's raw text.
func shellWords(cmd string) []string {
	var words []string
	var cur strings.Builder
	var quote byte
	depth := 0
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		switch {
		case quote != 0:
			cur.WriteByte(c)
			if c == '\\' && quote == '"' && i+1 < len(cmd) {
				i++
				cur.WriteByte(cmd[i])
			} else if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
			cur.WriteByte(c)
		case c == '$' && i+1 < len(cmd) && cmd[i+1] == '(':
			depth++
			cur.WriteString("$(")
			i++
		case c == '(' && depth > 0:
			depth++
			cur.WriteByte(c)
		case c == ')' && depth > 0:
			depth--
			cur.WriteByte(c)
		case depth > 0:
			cur.WriteByte(c)
		case c == ' ' || c == '\t':
			if cur.Len() > 0 {
				words = append(words, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		words = append(words, cur.String())
	}
	return words
}

func dynamicWord(w string) bool {
	return strings.ContainsAny(w, "$`")
}

// dynamicCommands names every command in a run block whose command word is not a literal. It never resolves a value.
type dynHit struct{ word, text string }

func dynamicCommands(run string) []dynHit {
	defined := map[string]bool{}
	for _, m := range funcDefine.FindAllStringSubmatch(run, -1) {
		defined[m[1]+m[2]] = true
	}
	for _, m := range aliasDefine.FindAllStringSubmatch(run, -1) {
		defined[m[1]] = true
	}
	var out []dynHit
	for _, cmd := range splitCommands(run) {
		words := shellWords(cmd)
		for len(words) > 0 && (leadingKeywords[words[0]] || assignWord.MatchString(words[0])) {
			words = words[1:]
		}
		if len(words) > 0 && strings.HasSuffix(words[0], ")") && !strings.Contains(words[0], "(") && len(words) > 1 {
			words = words[1:]
		}
		for len(words) > 0 && (leadingKeywords[words[0]] || assignWord.MatchString(words[0])) {
			words = words[1:]
		}
		if len(words) == 0 || skipClauses[words[0]] {
			continue
		}
		w := words[0]
		switch {
		case dynamicWord(w):
			out = append(out, dynHit{w, "variable-built command: " + cmd})
		case w == "eval" || w == "xargs":
			out = append(out, dynHit{w, "variable-built command: " + w + " runs a string it builds: " + cmd})
		case (w == "bash" || w == "sh" || w == "zsh") && len(words) > 2 && words[1] == "-c" && dynamicWord(words[2]):
			out = append(out, dynHit{w, "variable-built command: " + w + " -c with a non-literal string: " + cmd})
		case defined[w]:
			out = append(out, dynHit{w, "variable-built command: " + w + " is a function or alias defined in the same block: " + cmd})
		}
	}
	return out
}
