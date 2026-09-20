package reports

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqljson"
)

// jsonObject is a parsed object that keeps the position of a key's first
// appearance and the value of its last, as a Python dict built by json.loads
// does. Keys are compared by their decoded text.
type jsonObject struct {
	keys  []string // decoded keys, in first-appearance order
	raws  map[string]string
	items map[string]any
}

// jsonValue re-encodes a stored JSON document the way the Python plane sees it
// after json.loads: an object with a repeated key (compared by decoded text,
// so "a" and "a" are one key) keeps the first key position and the last
// value. Strings and numbers keep their stored text, so an escape a UTF-8
// decoder cannot represent (a lone surrogate) reaches the client unchanged. A
// stored SQL NULL and the JSON literal null both answer null.
func jsonValue(text *string) (graphqljson.JSON, error) {
	if text == nil {
		return graphqljson.Null, nil
	}
	p := &jsonParser{text: *text}
	value, err := p.value(0)
	if err != nil {
		return nil, fmt.Errorf("stored json document is not valid: %w", err)
	}
	p.skipSpace()
	if p.pos != len(p.text) {
		return nil, fmt.Errorf("stored json document has trailing data")
	}
	var out bytes.Buffer
	writeJSON(&out, value)
	return graphqljson.JSON(out.Bytes()), nil
}

// maxJSONDepth is the deepest array/object nesting Python 3.14's json.loads
// followed by json.dumps accepts (measured); deeper documents raise
// RecursionError there and are an error here.
const maxJSONDepth = 65199

type jsonParser struct {
	text string
	pos  int
}

// rawToken is a scalar kept as its stored text.
type rawToken string

func (p *jsonParser) skipSpace() {
	for p.pos < len(p.text) {
		switch p.text[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *jsonParser) value(depth int) (any, error) {
	if depth >= maxJSONDepth {
		return nil, fmt.Errorf("nesting too deep")
	}
	p.skipSpace()
	if p.pos >= len(p.text) {
		return nil, fmt.Errorf("unexpected end")
	}
	switch c := p.text[p.pos]; {
	case c == '{':
		return p.object(depth)
	case c == '[':
		return p.array(depth)
	case c == '"':
		s, err := p.str()
		return rawToken(s), err
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	}
	for _, literal := range []string{"true", "false", "null"} {
		if strings.HasPrefix(p.text[p.pos:], literal) {
			p.pos += len(literal)
			return rawToken(literal), nil
		}
	}
	return nil, fmt.Errorf("unexpected character at %d", p.pos)
}

func (p *jsonParser) number() (any, error) {
	start := p.pos
	for p.pos < len(p.text) && strings.IndexByte("+-0123456789.eE", p.text[p.pos]) >= 0 {
		p.pos++
	}
	token := p.text[start:p.pos]
	if !json.Valid([]byte(token)) {
		return nil, fmt.Errorf("invalid number %q", token)
	}
	return rawToken(token), nil
}

// str reads one string token and returns its stored text, quotes included.
func (p *jsonParser) str() (string, error) {
	start := p.pos
	p.pos++ // opening quote
	for p.pos < len(p.text) {
		switch c := p.text[p.pos]; {
		case c == '"':
			p.pos++
			return p.text[start:p.pos], nil
		case c < 0x20:
			return "", fmt.Errorf("control character in string")
		case c == '\\':
			p.pos++
			if p.pos >= len(p.text) {
				return "", fmt.Errorf("unterminated escape")
			}
			switch p.text[p.pos] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				p.pos++
			case 'u':
				if p.pos+5 > len(p.text) || !isHex4(p.text[p.pos+1:p.pos+5]) {
					return "", fmt.Errorf("invalid unicode escape")
				}
				p.pos += 5
			default:
				return "", fmt.Errorf("invalid escape")
			}
		default:
			p.pos++
		}
	}
	return "", fmt.Errorf("unterminated string")
}

func isHex4(s string) bool {
	for i := 0; i < 4; i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

func (p *jsonParser) object(depth int) (any, error) {
	p.pos++ // {
	object := &jsonObject{raws: map[string]string{}, items: map[string]any{}}
	p.skipSpace()
	if p.pos < len(p.text) && p.text[p.pos] == '}' {
		p.pos++
		return object, nil
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.text) || p.text[p.pos] != '"' {
			return nil, fmt.Errorf("object key expected at %d", p.pos)
		}
		raw, err := p.str()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.text) || p.text[p.pos] != ':' {
			return nil, fmt.Errorf("colon expected at %d", p.pos)
		}
		p.pos++
		value, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		key := decodeKey(raw)
		if _, seen := object.items[key]; !seen {
			object.keys = append(object.keys, key)
			object.raws[key] = raw
		}
		object.items[key] = value
		p.skipSpace()
		if p.pos >= len(p.text) {
			return nil, fmt.Errorf("unexpected end")
		}
		switch p.text[p.pos] {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return object, nil
		default:
			return nil, fmt.Errorf("comma or brace expected at %d", p.pos)
		}
	}
}

func (p *jsonParser) array(depth int) (any, error) {
	p.pos++ // [
	list := []any{}
	p.skipSpace()
	if p.pos < len(p.text) && p.text[p.pos] == ']' {
		p.pos++
		return list, nil
	}
	for {
		value, err := p.value(depth + 1)
		if err != nil {
			return nil, err
		}
		list = append(list, value)
		p.skipSpace()
		if p.pos >= len(p.text) {
			return nil, fmt.Errorf("unexpected end")
		}
		switch p.text[p.pos] {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return list, nil
		default:
			return nil, fmt.Errorf("comma or bracket expected at %d", p.pos)
		}
	}
}

// decodeKey is the decoded text of a stored string token. A lone surrogate
// escape keeps its own three-byte form, which is not valid UTF-8 and so never
// equals any other key.
func decodeKey(raw string) string {
	inner := raw[1 : len(raw)-1]
	if !strings.Contains(inner, `\`) {
		return inner
	}
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		i++
		switch inner[i] {
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'u':
			unit := hexUnit(inner[i+1 : i+5])
			i += 4
			if unit >= 0xd800 && unit < 0xdc00 && i+6 < len(inner)+0 && inner[i+1] == '\\' && inner[i+2] == 'u' && isHex4(inner[i+3:i+7]) {
				if low := hexUnit(inner[i+3 : i+7]); low >= 0xdc00 && low < 0xe000 {
					b.WriteRune(rune(0x10000 + (unit-0xd800)<<10 + (low - 0xdc00)))
					i += 6
					continue
				}
			}
			if unit >= 0xd800 && unit < 0xe000 {
				b.WriteByte(0xed)
				b.WriteByte(byte(0x80 | (unit>>6)&0x3f))
				b.WriteByte(byte(0x80 | unit&0x3f))
				continue
			}
			var buf [4]byte
			b.Write(buf[:utf8.EncodeRune(buf[:], rune(unit))])
		default: // " \ /
			b.WriteByte(inner[i])
		}
	}
	return b.String()
}

func hexUnit(s string) int {
	n := 0
	for i := 0; i < 4; i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			n = n<<4 | int(c-'0')
		case c >= 'a' && c <= 'f':
			n = n<<4 | int(c-'a'+10)
		default:
			n = n<<4 | int(c-'A'+10)
		}
	}
	return n
}

func writeJSON(out *bytes.Buffer, value any) {
	switch v := value.(type) {
	case *jsonObject:
		out.WriteByte('{')
		for i, key := range v.keys {
			if i > 0 {
				out.WriteByte(',')
			}
			out.WriteString(v.raws[key])
			out.WriteByte(':')
			writeJSON(out, v.items[key])
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			writeJSON(out, item)
		}
		out.WriteByte(']')
	case rawToken:
		out.WriteString(string(v))
	}
}
