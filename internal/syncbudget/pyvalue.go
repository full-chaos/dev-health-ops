package syncbudget

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The estimators read decrypted credential mappings, dataset options and
// processor flags exactly as the Python estimators did: values that
// json.loads produced, read through Python's truthiness, str() and
// json.dumps(sort_keys=True, separators=(",", ":")). A credential field can
// hold any JSON value, not only a string, and each of those three operations
// treats the non-string values in its own way. The helpers below reproduce
// them over a decoded JSON tree whose shape matches Python's:
//
//	JSON null   -> nil
//	true/false  -> bool
//	integer     -> int64, or *big.Int past int64 (Python ints are unbounded)
//	other number-> float64 (Python float; 1e400 is +Inf, as in json.loads)
//	string      -> string
//	array       -> []any
//	object      -> *object (insertion order kept, as in a Python dict)

// object is a JSON object in Python dict order: a repeated key keeps its
// first position and takes its last value, as json.loads builds it.
type object struct {
	keys   []string
	values map[string]any
}

func newObject() *object { return &object{values: map[string]any{}} }

func (o *object) get(key string) (any, bool) {
	if o == nil {
		return nil, false
	}
	value, ok := o.values[key]
	return value, ok
}

func (o *object) set(key string, value any) {
	if _, ok := o.values[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

// merged returns {**base, **over}: base's keys first, over's new keys after,
// over's values winning on a shared key.
func merged(base, over *object) *object {
	result := newObject()
	for _, source := range []*object{base, over} {
		if source == nil {
			continue
		}
		for _, key := range source.keys {
			result.set(key, source.values[key])
		}
	}
	return result
}

var errNotJSON = errors.New("value is not a single JSON document")

// errUnicodeEncode is the UnicodeEncodeError str.encode("utf-8") raises for
// a lone surrogate.
var errUnicodeEncode = errors.New("'utf-8' codec can't encode a surrogate")

// hasSurrogate reports a WTF-8 encoded surrogate (ED A0..BF xx) in text.
func hasSurrogate(text string) bool {
	for index := 0; index+2 < len(text); index++ {
		if text[index] == 0xed && text[index+1] >= 0xa0 && text[index+1] <= 0xbf {
			return true
		}
	}
	return false
}

// surrogateAt returns the surrogate code point encoded at text[index:], if
// one is.
func surrogateAt(text string, index int) (rune, bool) {
	if index+2 < len(text) && text[index] == 0xed && text[index+1] >= 0xa0 && text[index+1] <= 0xbf {
		return rune(text[index]&0x0f)<<12 | rune(text[index+1]&0x3f)<<6 | rune(text[index+2]&0x3f), true
	}
	return 0, false
}

// decodeJSON is json.loads: one document, surrounding whitespace allowed,
// and -- unlike encoding/json -- the NaN, Infinity and -Infinity literals
// Python's decoder accepts (a credential written with one still decrypts to
// a mapping in Python). Numbers keep Python's int/float split; objects keep
// dict order with the last value of a repeated key.
func decodeJSON(raw []byte) (any, error) {
	parser := &jsonParser{text: string(raw)}
	parser.skipSpace()
	value, err := parser.value()
	if err != nil {
		return nil, err
	}
	parser.skipSpace()
	if parser.pos != len(parser.text) {
		return nil, errNotJSON
	}
	return value, nil
}

type jsonParser struct {
	text string
	pos  int
}

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

func (p *jsonParser) literal(word string) bool {
	if strings.HasPrefix(p.text[p.pos:], word) {
		p.pos += len(word)
		return true
	}
	return false
}

func (p *jsonParser) value() (any, error) {
	if p.pos >= len(p.text) {
		return nil, errNotJSON
	}
	switch char := p.text[p.pos]; {
	case char == '{':
		return p.object()
	case char == '[':
		return p.array()
	case char == '"':
		return p.string()
	case p.literal("null"):
		return nil, nil
	case p.literal("true"):
		return true, nil
	case p.literal("false"):
		return false, nil
	case p.literal("NaN"):
		return math.NaN(), nil
	case p.literal("Infinity"):
		return math.Inf(1), nil
	case p.literal("-Infinity"):
		return math.Inf(-1), nil
	case char == '-' || (char >= '0' && char <= '9'):
		return p.number()
	}
	return nil, errNotJSON
}

func (p *jsonParser) object() (any, error) {
	p.pos++
	result := newObject()
	p.skipSpace()
	if p.pos < len(p.text) && p.text[p.pos] == '}' {
		p.pos++
		return result, nil
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.text) || p.text[p.pos] != '"' {
			return nil, errNotJSON
		}
		key, err := p.string()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.pos >= len(p.text) || p.text[p.pos] != ':' {
			return nil, errNotJSON
		}
		p.pos++
		p.skipSpace()
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		result.set(key.(string), value)
		p.skipSpace()
		if p.pos >= len(p.text) {
			return nil, errNotJSON
		}
		switch p.text[p.pos] {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return result, nil
		default:
			return nil, errNotJSON
		}
	}
}

func (p *jsonParser) array() (any, error) {
	p.pos++
	result := []any{}
	p.skipSpace()
	if p.pos < len(p.text) && p.text[p.pos] == ']' {
		p.pos++
		return result, nil
	}
	for {
		p.skipSpace()
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		result = append(result, value)
		p.skipSpace()
		if p.pos >= len(p.text) {
			return nil, errNotJSON
		}
		switch p.text[p.pos] {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return result, nil
		default:
			return nil, errNotJSON
		}
	}
}

// string is py_scanstring with strict=True: no raw control characters;
// \uXXXX pairs combine into one code point. A lone surrogate, which Python
// keeps, becomes U+FFFD (Go strings cannot hold it).
func (p *jsonParser) string() (any, error) {
	p.pos++
	var builder strings.Builder
	for {
		if p.pos >= len(p.text) {
			return nil, errNotJSON
		}
		char := p.text[p.pos]
		switch {
		case char == '"':
			p.pos++
			return builder.String(), nil
		case char < 0x20:
			return nil, errNotJSON
		case char != '\\':
			builder.WriteByte(char)
			p.pos++
			continue
		}
		p.pos++
		if p.pos >= len(p.text) {
			return nil, errNotJSON
		}
		escape := p.text[p.pos]
		p.pos++
		switch escape {
		case '"', '\\', '/':
			builder.WriteByte(escape)
		case 'b':
			builder.WriteByte('\b')
		case 'f':
			builder.WriteByte('\f')
		case 'n':
			builder.WriteByte('\n')
		case 'r':
			builder.WriteByte('\r')
		case 't':
			builder.WriteByte('\t')
		case 'u':
			code, ok := p.hex4()
			if !ok {
				return nil, errNotJSON
			}
			if code >= 0xd800 && code <= 0xdbff && strings.HasPrefix(p.text[p.pos:], "\\u") {
				saved := p.pos
				p.pos += 2
				if low, ok := p.hex4(); ok && low >= 0xdc00 && low <= 0xdfff {
					builder.WriteRune(0x10000 + (code-0xd800)<<10 + (low - 0xdc00))
					continue
				}
				p.pos = saved
			}
			if code >= 0xd800 && code <= 0xdfff {
				// A lone surrogate: kept as its WTF-8 bytes, which are
				// not valid UTF-8, so the operations that fail on it in
				// Python can fail here too.
				builder.WriteByte(byte(0xe0 | code>>12))
				builder.WriteByte(byte(0x80 | (code>>6)&0x3f))
				builder.WriteByte(byte(0x80 | code&0x3f))
				continue
			}
			builder.WriteRune(code)
		default:
			return nil, errNotJSON
		}
	}
}

func (p *jsonParser) hex4() (rune, bool) {
	if p.pos+4 > len(p.text) {
		return 0, false
	}
	value, err := strconv.ParseUint(p.text[p.pos:p.pos+4], 16, 32)
	if err != nil {
		return 0, false
	}
	p.pos += 4
	return rune(value), true
}

// number matches Python's NUMBER_RE: -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][-+]?[0-9]+)?
func (p *jsonParser) number() (any, error) {
	start := p.pos
	if p.text[p.pos] == '-' {
		p.pos++
	}
	digits := func() int {
		begin := p.pos
		for p.pos < len(p.text) && p.text[p.pos] >= '0' && p.text[p.pos] <= '9' {
			p.pos++
		}
		return p.pos - begin
	}
	if p.pos < len(p.text) && p.text[p.pos] == '0' {
		p.pos++
	} else if digits() == 0 {
		return nil, errNotJSON
	}
	if p.pos < len(p.text) && p.text[p.pos] == '.' {
		saved := p.pos
		p.pos++
		if digits() == 0 {
			p.pos = saved
		}
	}
	if p.pos < len(p.text) && (p.text[p.pos] == 'e' || p.text[p.pos] == 'E') {
		saved := p.pos
		p.pos++
		if p.pos < len(p.text) && (p.text[p.pos] == '+' || p.text[p.pos] == '-') {
			p.pos++
		}
		if digits() == 0 {
			p.pos = saved
		}
	}
	return decodeNumber(p.text[start:p.pos])
}

func decodeNumber(text string) (any, error) {
	if !strings.ContainsAny(text, ".eE") {
		value, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return nil, errNotJSON
		}
		if value.IsInt64() {
			return value.Int64(), nil
		}
		return value, nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return nil, errNotJSON
	}
	return value, nil
}

// truthy is Python's bool(value).
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case int64:
		return typed != 0
	case *big.Int:
		return typed.Sign() != 0
	case float64:
		return typed != 0
	case string:
		return typed != ""
	case []any:
		return len(typed) > 0
	case *object:
		return typed != nil && len(typed.keys) > 0
	}
	return true
}

// firstTruthy is Python's `a or b or ...`: the first truthy value, else the
// last one.
func firstTruthy(values ...any) any {
	var last any
	for _, value := range values {
		if truthy(value) {
			return value
		}
		last = value
	}
	return last
}

// get is Mapping.get(key) on a value that may not be a mapping: callers that
// reach here have already checked isinstance(x, Mapping).
func get(mapping *object, key string) any {
	value, _ := mapping.get(key)
	return value
}

// pyStr is Python's str(value) for a JSON-derived value.
func pyStr(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return pyRepr(value)
}

// pyRepr is Python's repr(value) for a JSON-derived value (str() of a list
// or dict uses repr() for every element).
func pyRepr(value any) string {
	switch typed := value.(type) {
	case nil:
		return "None"
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case int64:
		return strconv.FormatInt(typed, 10)
	case *big.Int:
		return typed.String()
	case float64:
		return pythonparity.Repr(typed)
	case string:
		return reprString(typed)
	case []any:
		parts := make([]string, len(typed))
		for index, element := range typed {
			parts[index] = pyRepr(element)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *object:
		parts := make([]string, 0, len(typed.keys))
		for _, key := range typed.keys {
			parts = append(parts, reprString(key)+": "+pyRepr(typed.values[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprintf("%v", value)
}

// reprString is CPython's unicode_repr: single quotes unless the text holds
// a single quote and no double quote; backslash, the chosen quote, \t \n \r
// escaped; other non-printable code points as \xNN, \uNNNN or \UNNNNNNNN.
func reprString(text string) string {
	quote := byte('\'')
	if strings.Contains(text, "'") && !strings.Contains(text, "\"") {
		quote = '"'
	}
	var builder strings.Builder
	builder.WriteByte(quote)
	for index := 0; index < len(text); {
		if code, ok := surrogateAt(text, index); ok {
			fmt.Fprintf(&builder, `\u%04x`, code)
			index += 3
			continue
		}
		char, size := utf8.DecodeRuneInString(text[index:])
		index += size
		switch {
		case char == rune(quote) || char == '\\':
			builder.WriteByte('\\')
			builder.WriteRune(char)
		case char == '\t':
			builder.WriteString(`\t`)
		case char == '\n':
			builder.WriteString(`\n`)
		case char == '\r':
			builder.WriteString(`\r`)
		case char < 0x20 || char == 0x7f:
			fmt.Fprintf(&builder, `\x%02x`, char)
		case char < 0x7f:
			builder.WriteRune(char)
		case isPrintable(char):
			builder.WriteRune(char)
		case char <= 0xff:
			fmt.Fprintf(&builder, `\x%02x`, char)
		case char <= 0xffff:
			fmt.Fprintf(&builder, `\u%04x`, char)
		default:
			fmt.Fprintf(&builder, `\U%08x`, char)
		}
	}
	builder.WriteByte(quote)
	return builder.String()
}

// isPrintable is str.isprintable for one code point: every category but
// Cc, Cf, Cs, Co, Cn, Zl, Zp and Zs (except the ASCII space).
func isPrintable(char rune) bool {
	if char == ' ' {
		return true
	}
	return unicode.IsPrint(char)
}

// dumpsSortedCompact is json.dumps(value, sort_keys=True, default=str,
// separators=(",", ":")) with the default ensure_ascii=True and
// allow_nan=True, over the JSON-derived tree. default=str never runs: every
// value in the tree is JSON-native.
func dumpsSortedCompact(value any) []byte {
	return appendDump(make([]byte, 0, 128), value)
}

func appendDump(dst []byte, value any) []byte {
	switch typed := value.(type) {
	case nil:
		return append(dst, "null"...)
	case bool:
		if typed {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case int64:
		return strconv.AppendInt(dst, typed, 10)
	case *big.Int:
		return append(dst, typed.String()...)
	case float64:
		switch {
		case math.IsNaN(typed):
			return append(dst, "NaN"...)
		case math.IsInf(typed, 1):
			return append(dst, "Infinity"...)
		case math.IsInf(typed, -1):
			return append(dst, "-Infinity"...)
		}
		return append(dst, pythonparity.Repr(typed)...)
	case string:
		return appendJSONString(dst, typed)
	case []any:
		dst = append(dst, '[')
		for index, element := range typed {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = appendDump(dst, element)
		}
		return append(dst, ']')
	case *object:
		keys := append([]string(nil), typed.keys...)
		sort.Strings(keys)
		dst = append(dst, '{')
		for index, key := range keys {
			if index > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, key)
			dst = append(dst, ':')
			dst = appendDump(dst, typed.values[key])
		}
		return append(dst, '}')
	}
	return append(dst, pythonparity.AppendPythonJSONString(nil, fmt.Sprintf("%v", value))...)
}

// appendJSONString is the ensure_ascii string encoder, escaping a kept lone
// surrogate as \udXXX the way Python does.
func appendJSONString(dst []byte, text string) []byte {
	if !hasSurrogate(text) {
		return pythonparity.AppendPythonJSONString(dst, text)
	}
	encoded := []byte{'"'}
	start := 0
	for index := 0; index < len(text); {
		code, ok := surrogateAt(text, index)
		if !ok {
			index++
			continue
		}
		chunk := pythonparity.AppendPythonJSONString(nil, text[start:index])
		encoded = append(encoded, chunk[1:len(chunk)-1]...)
		encoded = append(encoded, fmt.Sprintf(`\u%04x`, code)...)
		index += 3
		start = index
	}
	chunk := pythonparity.AppendPythonJSONString(nil, text[start:])
	encoded = append(encoded, chunk[1:len(chunk)-1]...)
	return append(append(dst, encoded...), '"')
}

var errNotDict = errors.New("dict() of a value that is not a mapping or an iterable of pairs")

// dictEntry is one (key, value) of a Python dict built from JSON: the key
// is any hashable JSON value, not only a string.
type dictEntry struct {
	key   any
	value any
}

// dictEntries is dict(value or {}) over a JSON-derived value, in dict order.
// A falsy value is empty; an object is copied; an array is an iterable of
// pairs, each a 2-element array, a 2-character string or a 2-key object
// (dict() iterates each element); any other shape raises, as dict() does.
// Keys that Python compares equal (1, 1.0 and True; 0, 0.0 and False)
// collapse into the first key with the last value.
func dictEntries(value any) ([]dictEntry, error) {
	if !truthy(value) {
		return nil, nil
	}
	var pairs [][2]any
	switch typed := value.(type) {
	case *object:
		for _, key := range typed.keys {
			pairs = append(pairs, [2]any{key, typed.values[key]})
		}
	case []any:
		for _, element := range typed {
			switch item := element.(type) {
			case []any:
				if len(item) != 2 {
					return nil, errNotDict
				}
				pairs = append(pairs, [2]any{item[0], item[1]})
			case string:
				runes := []rune(item)
				if len(runes) != 2 {
					return nil, errNotDict
				}
				pairs = append(pairs, [2]any{string(runes[0]), string(runes[1])})
			case *object:
				if len(item.keys) != 2 {
					return nil, errNotDict
				}
				pairs = append(pairs, [2]any{item.keys[0], item.keys[1]})
			default:
				return nil, errNotDict
			}
		}
	default:
		return nil, errNotDict
	}
	var entries []dictEntry
	index := map[string]int{}
	for _, pair := range pairs {
		identity, hashable := keyIdentity(pair[0])
		if !hashable {
			return nil, errNotDict
		}
		if position, seen := index[identity]; seen {
			entries[position].value = pair[1]
			continue
		}
		index[identity] = len(entries)
		entries = append(entries, dictEntry{key: pair[0], value: pair[1]})
	}
	return entries, nil
}

// keyIdentity is the equality class of a dict key: numbers compare by value
// across int, float and bool; strings and None by themselves. A list or a
// dict is unhashable.
func keyIdentity(key any) (string, bool) {
	switch typed := key.(type) {
	case nil:
		return "none", true
	case string:
		return "s:" + typed, true
	case bool:
		if typed {
			return "n:1", true
		}
		return "n:0", true
	case int64:
		return "n:" + strconv.FormatInt(typed, 10), true
	case *big.Int:
		return "n:" + typed.String(), true
	case float64:
		if typed == math.Trunc(typed) && !math.IsInf(typed, 0) {
			integral, _ := new(big.Float).SetFloat64(typed).Int(nil)
			return "n:" + integral.String(), true
		}
		if math.IsNaN(typed) {
			// NaN never equals itself; every NaN key is its own entry.
			return fmt.Sprintf("nan:%p", &typed), true
		}
		return "f:" + strconv.FormatFloat(typed, 'g', -1, 64), true
	}
	return "", false
}

// dictOrEmpty is dict(value or {}) for a JSON column, as an *object: only
// its string keys, since every caller reads it with a string key and no
// other key can equal a string.
func dictOrEmpty(value any) (*object, error) {
	entries, err := dictEntries(value)
	if err != nil {
		return nil, err
	}
	result := newObject()
	for _, entry := range entries {
		if key, isString := entry.key.(string); isString {
			result.set(key, entry.value)
		}
	}
	return result, nil
}

// processorFlags is SyncTaskBootstrap.load's
// {str(k): bool(v) for k, v in dict(unit.processor_flags or {}).items()}.
func processorFlags(value any) (map[string]bool, error) {
	entries, err := dictEntries(value)
	if err != nil {
		return nil, err
	}
	flags := make(map[string]bool, len(entries))
	for _, entry := range entries {
		flags[pyStr(entry.key)] = truthy(entry.value)
	}
	return flags, nil
}
