package venueoracle

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// RedactJSON replaces scalar values in a JSON body without re-rendering
// it: every byte it does not replace stays as the plane wrote it. A
// normalizer that decodes a body and renders it again rewrites every
// other value's spelling (a float's digits among them) the same way on
// both planes, which hides a real difference; this one cannot.
//
// replace receives each scalar value's path (object keys, and "[]" for an
// array element) and its raw text; it returns the replacement JSON text
// and true to replace it. The result is body unchanged when the text is
// not a JSON document.
func RedactJSON(body string, replace func(path []string, raw string) (string, bool)) string {
	walker := &redactWalker{text: body, replace: replace}
	start := walker.space(0)
	end, ok := walker.value(start, nil)
	if !ok || walker.space(end) != len(body) {
		return body
	}
	walker.out.WriteString(body[walker.copied:])
	return walker.out.String()
}

type redactWalker struct {
	text    string
	replace func(path []string, raw string) (string, bool)
	out     strings.Builder
	copied  int
}

func (w *redactWalker) space(index int) int {
	for index < len(w.text) && strings.IndexByte(" \t\r\n", w.text[index]) >= 0 {
		index++
	}
	return index
}

func (w *redactWalker) value(index int, path []string) (int, bool) {
	if index >= len(w.text) {
		return index, false
	}
	switch w.text[index] {
	case '{':
		index = w.space(index + 1)
		if index < len(w.text) && w.text[index] == '}' {
			return index + 1, true
		}
		for {
			keyEnd, ok := w.string(index)
			if !ok {
				return index, false
			}
			key, err := pyjson.DecodeString(w.text[index:keyEnd])
			name, isString := key.(string)
			if err != nil || !isString {
				return index, false
			}
			index = w.space(keyEnd)
			if index >= len(w.text) || w.text[index] != ':' {
				return index, false
			}
			index, ok = w.value(w.space(index+1), append(append([]string{}, path...), name))
			if !ok {
				return index, false
			}
			index = w.space(index)
			if index < len(w.text) && w.text[index] == ',' {
				index = w.space(index + 1)
				continue
			}
			if index < len(w.text) && w.text[index] == '}' {
				return index + 1, true
			}
			return index, false
		}
	case '[':
		index = w.space(index + 1)
		if index < len(w.text) && w.text[index] == ']' {
			return index + 1, true
		}
		for {
			var ok bool
			index, ok = w.value(index, append(append([]string{}, path...), "[]"))
			if !ok {
				return index, false
			}
			index = w.space(index)
			if index < len(w.text) && w.text[index] == ',' {
				index = w.space(index + 1)
				continue
			}
			if index < len(w.text) && w.text[index] == ']' {
				return index + 1, true
			}
			return index, false
		}
	}
	end, ok := w.scalar(index)
	if !ok {
		return index, false
	}
	if replacement, replace := w.replace(path, w.text[index:end]); replace {
		w.out.WriteString(w.text[w.copied:index])
		w.out.WriteString(replacement)
		w.copied = end
	}
	return end, true
}

func (w *redactWalker) string(index int) (int, bool) {
	if index >= len(w.text) || w.text[index] != '"' {
		return index, false
	}
	for position := index + 1; position < len(w.text); position++ {
		switch w.text[position] {
		case '\\':
			position++
		case '"':
			return position + 1, true
		}
	}
	return index, false
}

func (w *redactWalker) scalar(index int) (int, bool) {
	if w.text[index] == '"' {
		return w.string(index)
	}
	end := index
	for end < len(w.text) && strings.IndexByte(",}] \t\r\n", w.text[end]) < 0 {
		end++
	}
	if end == index {
		return index, false
	}
	if _, err := pyjson.DecodeString(w.text[index:end]); err != nil {
		return index, false
	}
	return end, true
}
