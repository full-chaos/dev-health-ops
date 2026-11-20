package renderdiff

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// yamlError is the class of PyYAML's YAMLError: the kit catches it on a render ("not valid YAML").
type yamlError struct{ msg string }

func (e *yamlError) Error() string { return e.msg }

// crashError is any other Python exception on the kit's path (ValueError, KeyError, RecursionError, ...): the kit
// exits 1 with a traceback on stderr. msg starts with the Python exception type; fn, when set, is the innermost kit
// function of that traceback. The port exits 1 with one stderr line that holds only the class token (crashToken).
type crashError struct{ msg, fn string }

func (e *crashError) Error() string { return e.msg }

const yamlTag = "tag:yaml.org,2002:"

// The PyYAML 6.0.1 implicit resolvers (resolver.py), in their registration order, keyed by the first character.
// Python's `$` also matches before a final newline; `\n?$` keeps that.
var implicitResolvers = []struct {
	tag   string
	re    *regexp.Regexp
	first string
}{
	{"bool", regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)\n?$`), "yYnNtTfFoO"},
	{"float", regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))\n?$`), "-+0123456789."},
	{"int", regexp.MustCompile(`^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)\n?$`), "-+0123456789"},
	{"merge", regexp.MustCompile(`^(?:<<)\n?$`), "<"},
	{"null", regexp.MustCompile(`^(?:~|null|Null|NULL|)\n?$`), "~nN\x00"},
	{"timestamp", regexp.MustCompile(`^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)\n?$`), "0123456789"},
	{"value", regexp.MustCompile(`^(?:=)\n?$`), "="},
}

// resolveImplicit is BaseResolver.resolve for a scalar with implicit[0] true: the tag suffix, "str" when no resolver
// matches.
func resolveImplicit(value string) string {
	for _, r := range implicitResolvers {
		if value == "" {
			if !strings.Contains(r.first, "\x00") {
				continue
			}
		} else {
			first, _ := firstRune(value)
			if !strings.ContainsRune(r.first, first) {
				continue
			}
		}
		if r.re.MatchString(value) {
			return r.tag
		}
	}
	return "str"
}

func firstRune(s string) (rune, int) {
	for i, r := range s {
		return r, i
	}
	return 0, 0
}

var timestampRe = regexp.MustCompile(`^([0-9][0-9][0-9][0-9])-([0-9][0-9]?)-([0-9][0-9]?)(?:(?:[Tt]|[ \t]+)([0-9][0-9]?):([0-9][0-9]):([0-9][0-9])(?:\.([0-9]*))?(?:[ \t]*(Z|([-+])([0-9][0-9]?)(?::([0-9][0-9]))?))?)?\n?$`)

// pythonText is what the kit reads through open() / sys.stdin in text mode: UTF-8 (strict) with universal newlines.
func pythonText(raw []byte) (string, error) {
	s := string(raw)
	if !utf8.ValidString(s) {
		return "", &crashError{msg: "UnicodeDecodeError: the input is not UTF-8", fn: "docs_of"}
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\r", "\n"), nil
}

// loadAll is yaml.safe_load_all over a text: every document, or the first error.
func loadAll(text string) ([]any, error) {
	for _, r := range text {
		if !(r == 0x09 || r == 0x0A || r == 0x0D || (r >= 0x20 && r <= 0x7E) || r == 0x85 || (r >= 0xA0 && r <= 0xD7FF) || (r >= 0xE000 && r <= 0xFFFD) || (r >= 0x10000 && r <= 0x10FFFF)) {
			return nil, &yamlError{msg: "unacceptable character"}
		}
	}
	dec := yaml.NewDecoder(bytes.NewReader([]byte(text)))
	var docs []any
	for {
		var n yaml.Node
		err := dec.Decode(&n)
		if errors.Is(err, io.EOF) {
			return docs, nil
		}
		if err != nil {
			return nil, &yamlError{msg: err.Error()}
		}
		if len(n.Content) == 0 {
			docs = append(docs, nil)
			continue
		}
		l := &loader{memo: map[*yaml.Node]any{}, busy: map[*yaml.Node]bool{}, defined: map[*yaml.Node]bool{}, anchors: map[string]bool{}}
		if err := l.checkAnchors(n.Content[0]); err != nil {
			return nil, err
		}
		v, err := l.construct(n.Content[0], false)
		if err != nil {
			return nil, err
		}
		docs = append(docs, v)
	}
}

type loader struct {
	memo    map[*yaml.Node]any
	busy    map[*yaml.Node]bool
	defined map[*yaml.Node]bool
	anchors map[string]bool
}

// checkAnchors holds PyYAML's composer rules that yaml.v3 does not: an anchor is defined once per document, and an
// alias names an anchor defined earlier in the same document.
func (l *loader) checkAnchors(n *yaml.Node) error {
	if n.Kind == yaml.AliasNode {
		if n.Alias == nil || !l.defined[n.Alias] {
			return &yamlError{msg: "found undefined alias " + strconv.Quote(n.Value)}
		}
		return nil
	}
	if n.Anchor != "" {
		if l.anchors[n.Anchor] {
			return &yamlError{msg: "found duplicate anchor " + strconv.Quote(n.Anchor)}
		}
		l.anchors[n.Anchor] = true
		l.defined[n] = true
	}
	for _, c := range n.Content {
		if err := l.checkAnchors(c); err != nil {
			return err
		}
	}
	return nil
}

func deref(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	return n
}

func explicitTag(n *yaml.Node) (string, bool) {
	if n.Style&yaml.TaggedStyle == 0 {
		return "", false
	}
	t := n.Tag
	switch {
	case strings.HasPrefix(t, "!!"):
		return yamlTag + t[2:], true
	case t == "!":
		return "", false
	}
	return t, true
}

// scalarTag is the composer's tag of a scalar node (a full tag).
func scalarTag(n *yaml.Node) string {
	if t, ok := explicitTag(n); ok {
		return t
	}
	quoted := n.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0
	if quoted {
		return yamlTag + "str"
	}
	return yamlTag + resolveImplicit(n.Value)
}

func (l *loader) construct(n *yaml.Node, isKey bool) (any, error) {
	n = deref(n)
	if v, ok := l.memo[n]; ok {
		return v, nil
	}
	if l.busy[n] {
		return nil, &crashError{msg: "RecursionError: a recursive alias", fn: "redact"}
	}
	l.busy[n] = true
	defer delete(l.busy, n)
	var v any
	var err error
	switch n.Kind {
	case yaml.ScalarNode:
		tag := scalarTag(n)
		if isKey && tag == yamlTag+"value" {
			tag = yamlTag + "str"
		}
		v, err = constructScalar(tag, n.Value)
	case yaml.SequenceNode:
		if t, ok := explicitTag(n); ok && t != yamlTag+"seq" {
			return nil, unknownTag(t)
		}
		list := []any{}
		for _, c := range n.Content {
			e, err := l.construct(c, false)
			if err != nil {
				return nil, err
			}
			list = append(list, e)
		}
		v = list
	case yaml.MappingNode:
		if t, ok := explicitTag(n); ok && t != yamlTag+"map" {
			return nil, unknownTag(t)
		}
		v, err = l.constructMapping(n)
	default:
		return nil, &yamlError{msg: fmt.Sprintf("unexpected node kind %d", n.Kind)}
	}
	if err != nil {
		return nil, err
	}
	l.memo[n] = v
	return v, nil
}

func unknownTag(t string) error {
	return &yamlError{msg: "could not determine a constructor for the tag " + pyStrRepr(t)}
}

func isMergeKey(k *yaml.Node) bool {
	k = deref(k)
	return k.Kind == yaml.ScalarNode && scalarTag(k) == yamlTag+"merge"
}

// flatten is SafeConstructor.flatten_mapping: the merged pairs first, then the mapping's own pairs.
func flatten(n *yaml.Node) ([][2]*yaml.Node, error) {
	var merge, own [][2]*yaml.Node
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if !isMergeKey(k) {
			own = append(own, [2]*yaml.Node{k, v})
			continue
		}
		vv := deref(v)
		switch vv.Kind {
		case yaml.MappingNode:
			sub, err := flatten(vv)
			if err != nil {
				return nil, err
			}
			merge = append(merge, sub...)
		case yaml.SequenceNode:
			var subs [][][2]*yaml.Node
			for _, s := range vv.Content {
				s = deref(s)
				if s.Kind != yaml.MappingNode {
					return nil, &yamlError{msg: "expected a mapping for merging"}
				}
				sub, err := flatten(s)
				if err != nil {
					return nil, err
				}
				subs = append(subs, sub)
			}
			for i := len(subs) - 1; i >= 0; i-- {
				merge = append(merge, subs[i]...)
			}
		default:
			return nil, &yamlError{msg: "expected a mapping or list of mappings for merging"}
		}
	}
	return append(merge, own...), nil
}

func (l *loader) constructMapping(n *yaml.Node) (any, error) {
	pairs, err := flatten(n)
	if err != nil {
		return nil, err
	}
	d := newDict()
	for _, p := range pairs {
		k, err := l.construct(p[0], true)
		if err != nil {
			return nil, err
		}
		if _, ok := hashKey(k); !ok {
			return nil, &yamlError{msg: "found unhashable key"}
		}
		v, err := l.construct(p[1], false)
		if err != nil {
			return nil, err
		}
		if err := d.set(k, v); err != nil {
			return nil, &yamlError{msg: "found unhashable key"}
		}
	}
	return d, nil
}

func constructScalar(tag, value string) (any, error) {
	switch strings.TrimPrefix(tag, yamlTag) {
	case "null":
		return nil, nil
	case "bool":
		switch strings.ToLower(value) {
		case "yes", "true", "on":
			return true, nil
		case "no", "false", "off":
			return false, nil
		}
		return nil, &crashError{msg: "KeyError: " + pyStrRepr(strings.ToLower(value))}
	case "int":
		return constructInt(value)
	case "float":
		return constructFloat(value)
	case "binary":
		for _, r := range value {
			if r > 0x7f {
				return nil, &yamlError{msg: "failed to convert base64 data into ascii"}
			}
		}
		b, err := pyDecodeBase64(value)
		if err != nil {
			return nil, &yamlError{msg: "failed to decode base64 data"}
		}
		return pyBytes(b), nil
	case "timestamp":
		return constructTimestamp(value)
	case "str":
		return value, nil
	}
	return nil, unknownTag(tag)
}

// pyNumText is what int()/float() parse: the text without outer whitespace, every Unicode decimal digit as ASCII.
func pyNumText(s string) string {
	s = strings.TrimFunc(s, pyIsSpace)
	return strings.Map(func(r rune) rune {
		if r > 0x7f && unicode.Is(unicode.Nd, r) {
			start := r
			for unicode.Is(unicode.Nd, start-1) {
				start--
			}
			return '0' + (r-start)%10
		}
		return r
	}, s)
}

// intOf is int(s, base); base 10 keeps CPython's maxStrDigits limit (power-of-two bases have none).
func intOf(s string, base int) (*big.Int, error) {
	t := pyNumText(s)
	if base == 10 && len(strings.TrimLeft(t, "+-")) > maxStrDigits {
		return nil, &crashError{msg: "ValueError: Exceeds the limit (4300 digits) for integer string conversion"}
	}
	v, ok := new(big.Int).SetString(t, base)
	if !ok || strings.HasPrefix(t, "+-") || strings.HasPrefix(t, "-+") {
		return nil, &crashError{msg: "ValueError: invalid literal for int()"}
	}
	return v, nil
}

func constructInt(value string) (any, error) {
	value = strings.ReplaceAll(value, "_", "")
	if value == "" {
		return nil, &crashError{msg: "IndexError: string index out of range"}
	}
	sign := int64(1)
	if value[0] == '-' {
		sign = -1
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	if value == "" {
		return nil, &crashError{msg: "IndexError: string index out of range"}
	}
	var v *big.Int
	var err error
	switch {
	case value == "0":
		return big.NewInt(0), nil
	case strings.HasPrefix(value, "0b"):
		v, err = intOf(value[2:], 2)
	case strings.HasPrefix(value, "0x"):
		v, err = intOf(value[2:], 16)
	case value != "" && value[0] == '0':
		v, err = intOf(value, 8)
	case strings.Contains(value, ":"):
		v = big.NewInt(0)
		base := big.NewInt(1)
		parts := strings.Split(value, ":")
		for i := len(parts) - 1; i >= 0; i-- {
			d, e := intOf(strings.TrimSpace(parts[i]), 10)
			if e != nil {
				return nil, e
			}
			v.Add(v, new(big.Int).Mul(d, base))
			base.Mul(base, big.NewInt(60))
		}
	default:
		v, err = intOf(value, 10)
	}
	if err != nil {
		return nil, err
	}
	return v.Mul(v, big.NewInt(sign)), nil
}

func pyFloatOf(s string) (float64, error) {
	t := pyNumText(s)
	if strings.ContainsAny(strings.ToLower(t), "xp") {
		return 0, &crashError{msg: "ValueError: could not convert string to float"}
	}
	f, err := strconv.ParseFloat(t, 64)
	if err != nil {
		var ne *strconv.NumError
		if errors.As(err, &ne) && errors.Is(ne.Err, strconv.ErrRange) {
			return f, nil
		}
		return 0, &crashError{msg: "ValueError: could not convert string to float"}
	}
	return f, nil
}

func constructFloat(value string) (any, error) {
	value = strings.ToLower(strings.ReplaceAll(value, "_", ""))
	if value == "" {
		return nil, &crashError{msg: "IndexError: string index out of range"}
	}
	sign := 1.0
	if value[0] == '-' {
		sign = -1
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	switch {
	case value == ".inf":
		return sign * math.Inf(1), nil
	case value == ".nan":
		return math.NaN(), nil
	case strings.Contains(value, ":"):
		v, base := 0.0, 1.0
		parts := strings.Split(value, ":")
		for i := len(parts) - 1; i >= 0; i-- {
			d, err := pyFloatOf(parts[i])
			if err != nil {
				return nil, err
			}
			v += d * base
			base *= 60
		}
		return sign * v, nil
	}
	f, err := pyFloatOf(value)
	if err != nil {
		return nil, err
	}
	return sign * f, nil
}

var daysIn = [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func validDate(y, m, d int) bool {
	if y < 1 || y > 9999 || m < 1 || m > 12 || d < 1 {
		return false
	}
	n := daysIn[m]
	if m == 2 && (y%4 == 0 && (y%100 != 0 || y%400 == 0)) {
		n = 29
	}
	return d <= n
}

func constructTimestamp(value string) (any, error) {
	m := timestampRe.FindStringSubmatch(value)
	if m == nil {
		return nil, &crashError{msg: "AttributeError: 'NoneType' object has no attribute 'groupdict'"}
	}
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	y, mo, d := atoi(m[1]), atoi(m[2]), atoi(m[3])
	if !validDate(y, mo, d) {
		return nil, &crashError{msg: "ValueError: invalid date"}
	}
	if m[4] == "" {
		return &pyDate{y: y, m: mo, d: d}, nil
	}
	t := &pyDateTime{y: y, mo: mo, d: d, h: atoi(m[4]), mi: atoi(m[5]), s: atoi(m[6])}
	if t.h > 23 || t.mi > 59 || t.s > 59 {
		return nil, &crashError{msg: "ValueError: invalid time"}
	}
	if m[7] != "" {
		f := m[7]
		if len(f) > 6 {
			f = f[:6]
		}
		for len(f) < 6 {
			f += "0"
		}
		t.us = atoi(f)
	}
	switch {
	case m[9] != "":
		off := atoi(m[10])*3600 + atoi(m[11])*60
		if m[9] == "-" {
			off = -off
		}
		if off <= -86400 || off >= 86400 {
			return nil, &crashError{msg: "ValueError: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24)"}
		}
		t.aware, t.offset = true, off
	case m[8] != "":
		t.aware = true
	}
	return t, nil
}

// pyDecodeBase64 is base64.decodebytes: non-alphabet bytes are skipped, padding is required.
func pyDecodeBase64(s string) ([]byte, error) {
	// binascii.a2b_base64, non-strict: skip bytes outside the alphabet; enough '=' after two or three data
	// characters ends the input; a lone '=' elsewhere is skipped.
	var out []byte
	quadPos, pads := 0, 0
	var left byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '=' {
			if quadPos >= 2 {
				pads++
				if quadPos+pads >= 4 {
					return out, nil
				}
			}
			continue
		}
		v := strings.IndexByte(b64Alphabet, c)
		if v < 0 {
			continue
		}
		b := byte(v)
		pads = 0
		switch quadPos {
		case 0:
			quadPos, left = 1, b
		case 1:
			quadPos = 2
			out = append(out, left<<2|b>>4)
			left = b & 0x0f
		case 2:
			quadPos = 3
			out = append(out, left<<4|b>>2)
			left = b & 0x03
		case 3:
			quadPos = 0
			out = append(out, left<<6|b)
			left = 0
		}
	}
	if quadPos != 0 {
		return nil, errors.New("incorrect padding")
	}
	return out, nil
}

const b64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
