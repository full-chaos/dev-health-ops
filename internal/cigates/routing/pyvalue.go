package routing

import (
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

// The four tools were Python scripts reading YAML with PyYAML's safe_load. Their output depends on how a value loads
// (an unquoted `true` is a bool, so str() of it is "True"; `yes` is a bool too, YAML 1.1). This file loads YAML into
// the values PyYAML builds: *pyMap (dict, insertion ordered), []any (list), string, bool, nil, pyInt, float64.

type pyInt string // the decimal digits of a Python int, canonical (no leading zeros, "-" for a negative)

type pyMap struct {
	keys []any
	vals []any
}

func (m *pyMap) index(k any) int {
	for i, key := range m.keys {
		if pyEqual(key, k) {
			return i
		}
	}
	return -1
}

// set assigns as a Python dict does: an existing key keeps its position and takes the new value.
func (m *pyMap) set(k, v any) {
	if i := m.index(k); i >= 0 {
		m.vals[i] = v
		return
	}
	m.keys = append(m.keys, k)
	m.vals = append(m.vals, v)
}

func (m *pyMap) lookup(k any) (any, bool) {
	if i := m.index(k); i >= 0 {
		return m.vals[i], true
	}
	return nil, false
}

func pyEqual(a, b any) bool {
	switch x := a.(type) {
	case *pyMap, []any:
		return false
	default:
		switch b.(type) {
		case *pyMap, []any:
			return false
		}
		return x == b
	}
}

// pyRaise is a Python exception: Run prints the message to stderr and exits 1, as an uncaught exception or a
// SystemExit with a message does.
type pyRaise struct{ msg string }

func raise(format string, args ...any) { panic(pyRaise{fmt.Sprintf(format, args...)}) }

// guarded runs body and turns a raised error into the exit code and message Python would give.
func guarded(stderr io.Writer, body func() int) (code int) {
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(pyRaise)
			if !ok {
				panic(r)
			}
			fmt.Fprintln(stderr, e.msg)
			code = 1
		}
	}()
	return body()
}

func pyTypeName(v any) string {
	switch x := v.(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case pyInt:
		return "int"
	case float64:
		return "float"
	case string:
		return "str"
	case []any:
		return "list"
	case *pyMap:
		return "dict"
	case pyTime:
		return x.typ
	}
	return fmt.Sprintf("%T", v)
}

// item is v[k] for a dict.
func item(v, k any) any {
	m, ok := v.(*pyMap)
	if !ok {
		raise("TypeError: %s object is not subscriptable by %s", pyTypeName(v), pyRepr(k))
	}
	val, found := m.lookup(k)
	if !found {
		raise("KeyError: %s", pyRepr(k))
	}
	return val
}

// get is v.get(k) (None when absent); v must be a dict.
func get(v, k any) any {
	m, ok := v.(*pyMap)
	if !ok {
		raise("AttributeError: '%s' object has no attribute 'get'", pyTypeName(v))
	}
	val, _ := m.lookup(k)
	return val
}

// contains is `k in v`.
func contains(v, k any) bool {
	switch x := v.(type) {
	case *pyMap:
		return x.index(k) >= 0
	case []any:
		for _, e := range x {
			if pyEqual(e, k) {
				return true
			}
		}
		return false
	case string:
		s, ok := k.(string)
		if !ok {
			raise("TypeError: 'in <string>' requires string as left operand, not %s", pyTypeName(k))
		}
		return strings.Contains(x, s)
	}
	raise("TypeError: argument of type '%s' is not iterable", pyTypeName(v))
	return false
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case pyInt:
		return x != "0"
	case float64:
		return x != 0
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case *pyMap:
		return len(x.keys) > 0
	}
	return true
}

// or is Python's `a or b`.
func or(a, b any) any {
	if truthy(a) {
		return a
	}
	return b
}

// iter is `for e in v`: a list's items, a dict's keys, a string's characters.
func iter(v any) []any {
	switch x := v.(type) {
	case []any:
		return x
	case *pyMap:
		return append([]any(nil), x.keys...)
	case string:
		var out []any
		for _, r := range x {
			out = append(out, string(r))
		}
		return out
	}
	raise("TypeError: '%s' object is not iterable", pyTypeName(v))
	return nil
}

func length(v any) int {
	switch x := v.(type) {
	case []any:
		return len(x)
	case *pyMap:
		return len(x.keys)
	case string:
		return utf8.RuneCountInString(x)
	}
	raise("TypeError: object of type '%s' has no len()", pyTypeName(v))
	return 0
}

// str is the string a value must be for a string method to apply.
func str(v any, what string) string {
	s, ok := v.(string)
	if !ok {
		raise("TypeError: %s must be str, not %s", what, pyTypeName(v))
	}
	return s
}

// pyStr is Python's str(v), which an f-string field also gives for these types.
func pyStr(v any) string {
	switch x := v.(type) {
	case pyTime:
		return x.str
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case pyInt:
		return string(x)
	case float64:
		return pyFloatRepr(x)
	case string:
		return x
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = pyRepr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *pyMap:
		parts := make([]string, len(x.keys))
		for i, k := range x.keys {
			parts[i] = pyRepr(k) + ": " + pyRepr(x.vals[i])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(v)
}

// pyIsSpace is str.isspace() for one character: Unicode White_Space plus the separators U+001C..U+001F.
func pyIsSpace(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) }

// pyStrip is str.strip().
func pyStrip(s string) string { return strings.TrimFunc(s, pyIsSpace) }

// pyRepr is Python's repr(v).
func pyRepr(v any) string {
	if t, ok := v.(pyTime); ok {
		return t.repr
	}
	s, ok := v.(string)
	if !ok {
		return pyStr(v)
	}
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r > 0x7f && !unicode.IsPrint(r):
			switch {
			case r <= 0xff:
				fmt.Fprintf(&b, `\x%02x`, r)
			case r <= 0xffff:
				fmt.Fprintf(&b, `\u%04x`, r)
			default:
				fmt.Fprintf(&b, `\U%08x`, r)
			}
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// pyFloatRepr is repr(float): the shortest digits that round-trip, fixed notation for exponents -4..15.
func pyFloatRepr(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64)
	mant, expText, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expText)
	sign := ""
	if strings.HasPrefix(mant, "-") {
		sign, mant = "-", mant[1:]
	}
	digits := strings.Replace(mant, ".", "", 1)
	if exp < -4 || exp >= 16 {
		m := digits[:1]
		if len(digits) > 1 {
			m += "." + digits[1:]
		}
		es := "+"
		if exp < 0 {
			es, exp = "-", -exp
		}
		return fmt.Sprintf("%s%se%s%02d", sign, m, es, exp)
	}
	if exp < 0 {
		return sign + "0." + strings.Repeat("0", -exp-1) + digits
	}
	if len(digits) <= exp+1 {
		return sign + digits + strings.Repeat("0", exp+1-len(digits)) + ".0"
	}
	return sign + digits[:exp+1] + "." + digits[exp+1:]
}

// ---- loading ----

// YAML 1.1 implicit resolvers, as PyYAML's Resolver registers them (plain scalars only).
var (
	reBool  = regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	reNull  = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	reInt   = regexp.MustCompile(`^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$`)
	reFloat = regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9_]+(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$`)
)

func boolValue(s string) bool {
	switch strings.ToLower(s) {
	case "yes", "true", "on":
		return true
	}
	return false
}

// intValue is SafeConstructor.construct_yaml_int.
func intValue(s string) (pyInt, error) {
	v := strings.ReplaceAll(s, "_", "")
	sign := ""
	if v != "" && (v[0] == '-' || v[0] == '+') {
		if v[0] == '-' {
			sign = "-"
		}
		v = v[1:]
	}
	n := new(big.Int)
	var ok bool
	switch {
	case v == "0":
		n, ok = n.SetInt64(0), true
	case strings.HasPrefix(v, "0b"):
		n, ok = n.SetString(v[2:], 2)
	case strings.HasPrefix(v, "0x"):
		n, ok = n.SetString(v[2:], 16)
	case strings.HasPrefix(v, "0"):
		n, ok = n.SetString(v[1:], 8)
	case strings.Contains(v, ":"):
		ok = true
		base := big.NewInt(1)
		parts := strings.Split(v, ":")
		for i := len(parts) - 1; i >= 0; i-- {
			d, good := new(big.Int).SetString(parts[i], 10)
			if !good {
				return "", fmt.Errorf("invalid int %q", s)
			}
			n.Add(n, d.Mul(d, base))
			base = new(big.Int).Mul(base, big.NewInt(60))
		}
	default:
		n, ok = n.SetString(v, 10)
	}
	if !ok {
		return "", fmt.Errorf("invalid int %q", s)
	}
	if sign == "-" {
		n.Neg(n)
	}
	return pyInt(n.String()), nil
}

// floatValue is SafeConstructor.construct_yaml_float.
func floatValue(s string) (float64, error) {
	v := strings.ToLower(strings.ReplaceAll(s, "_", ""))
	sign := 1.0
	if v != "" && (v[0] == '-' || v[0] == '+') {
		if v[0] == '-' {
			sign = -1
		}
		v = v[1:]
	}
	switch {
	case v == ".inf":
		return sign * math.Inf(1), nil
	case v == ".nan":
		return math.NaN(), nil
	case strings.Contains(v, ":"):
		parts := strings.Split(v, ":")
		base, total := 1.0, 0.0
		for i := len(parts) - 1; i >= 0; i-- {
			d, err := strconv.ParseFloat(parts[i], 64)
			if err != nil {
				return 0, err
			}
			total += d * base
			base *= 60
		}
		return sign * total, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	return sign * f, err
}

type loader struct {
	// composeTags accepts local tags (`!reset`, `!override`) as plain nodes, as check-web-env-parity's loader did.
	composeTags bool
	// built is every node already constructed: a shared anchor is built once, however many aliases name it.
	built map[*yaml.Node]any
	// building is the collections being constructed. An alias to one of them is a recursive alias, which is refused:
	// PyYAML builds a cyclic value there, but these tools read the repository's own values files, where a recursive
	// alias is a defect, not an input to route.
	building map[*yaml.Node]bool
}

// loadYAML is yaml.safe_load: one document or none (nil).
func loadYAML(text []byte, composeTags bool) (any, error) {
	dec := yaml.NewDecoder(strings.NewReader(string(text)))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return nil, errors.New("expected a single document in the stream, but found another document")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	l := &loader{composeTags: composeTags, built: map[*yaml.Node]any{}, building: map[*yaml.Node]bool{}}
	if doc.Kind == yaml.DocumentNode {
		if len(doc.Content) == 0 {
			return nil, nil
		}
		return l.node(doc.Content[0])
	}
	return l.node(&doc)
}

func (l *loader) explicitTag(n *yaml.Node) (string, bool) {
	if n.Style&yaml.TaggedStyle == 0 {
		return "", false
	}
	return n.Tag, true
}

// target is the node an alias names; an alias to a collection still being built is refused.
func (l *loader) target(n *yaml.Node) (*yaml.Node, error) {
	if n.Kind != yaml.AliasNode {
		return n, nil
	}
	if l.building[n.Alias] {
		return nil, fmt.Errorf("recursive alias *%s (line %d) inside its own anchor &%s (line %d) is refused", n.Value, n.Line, n.Alias.Anchor, n.Alias.Line)
	}
	return n.Alias, nil
}

func (l *loader) node(n *yaml.Node) (any, error) {
	n, err := l.target(n)
	if err != nil {
		return nil, err
	}
	if v, ok := l.built[n]; ok {
		return v, nil
	}
	switch n.Kind {
	case yaml.SequenceNode:
		if tag, ok := l.explicitTag(n); ok && tag != "!!seq" && !l.local(tag) {
			return nil, l.noConstructor(n, tag)
		}
		l.building[n] = true
		defer delete(l.building, n)
		out := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			v, err := l.node(c)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		l.built[n] = out
		return out, nil
	case yaml.MappingNode:
		if tag, ok := l.explicitTag(n); ok && tag != "!!map" && !l.local(tag) {
			return nil, l.noConstructor(n, tag)
		}
		return l.mapping(n)
	case yaml.ScalarNode:
		v, err := l.scalar(n)
		if err == nil {
			l.built[n] = v
		}
		return v, err
	}
	return nil, fmt.Errorf("line %d: unsupported YAML node", n.Line)
}

func (l *loader) local(tag string) bool {
	return l.composeTags && strings.HasPrefix(tag, "!") && !strings.HasPrefix(tag, "!!")
}

// noConstructor refuses a tag the loader does not read: everything but the core str, int, float, bool, null, seq and map
// tags (and, in a compose file, a local tag such as !reset). PyYAML's safe_load also built sets, ordered maps, pairs,
// bytes and datetimes from explicit tags; these tools read the repository's own files, which use none of them.
func (l *loader) noConstructor(n *yaml.Node, tag string) error {
	return fmt.Errorf("tag %s (line %d) is refused: only the core str, int, float, bool, null, seq and map tags are read", tag, n.Line)
}

// mapping is construct_mapping after flatten_mapping: merged (`<<`) pairs first, then the node's own pairs, assigned in
// order so a later key keeps the first key's position and takes the later value.
func (l *loader) mapping(n *yaml.Node) (*pyMap, error) {
	if v, ok := l.built[n]; ok {
		return v.(*pyMap), nil
	}
	l.building[n] = true
	defer delete(l.building, n)
	var merged []*pyMap
	out := &pyMap{}
	type pair struct{ k, v *yaml.Node }
	var own []pair
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind != yaml.ScalarNode || k.Tag != "!!merge" {
			own = append(own, pair{k, v})
			continue
		}
		src, err := l.target(v)
		if err != nil {
			return nil, err
		}
		switch src.Kind {
		case yaml.MappingNode:
			m, err := l.mapping(src)
			if err != nil {
				return nil, err
			}
			merged = append(merged, m)
		case yaml.SequenceNode:
			for j := len(src.Content) - 1; j >= 0; j-- {
				sub, err := l.target(src.Content[j])
				if err != nil {
					return nil, err
				}
				if sub.Kind != yaml.MappingNode {
					return nil, fmt.Errorf("line %d: expected a mapping for merging", sub.Line)
				}
				m, err := l.mapping(sub)
				if err != nil {
					return nil, err
				}
				merged = append(merged, m)
			}
		default:
			return nil, fmt.Errorf("line %d: expected a mapping or list of mappings for merging", src.Line)
		}
	}
	for _, m := range merged {
		for i, k := range m.keys {
			out.set(k, m.vals[i])
		}
	}
	for _, p := range own {
		key, err := l.node(p.k)
		if err != nil {
			return nil, err
		}
		switch key.(type) {
		case []any, *pyMap:
			return nil, fmt.Errorf("line %d: found unhashable key", p.k.Line)
		}
		val, err := l.node(p.v)
		if err != nil {
			return nil, err
		}
		out.set(key, val)
	}
	l.built[n] = out
	return out, nil
}

func (l *loader) scalar(n *yaml.Node) (any, error) {
	tag, explicit := l.explicitTag(n)
	if explicit {
		if l.local(tag) {
			return n.Value, nil
		}
		switch tag {
		case "!!str", "!":
			return n.Value, nil
		case "!!null":
			return nil, nil
		case "!!bool":
			if !reBool.MatchString(n.Value) {
				return nil, fmt.Errorf("KeyError: %s (line %d)", pyRepr(strings.ToLower(n.Value)), n.Line)
			}
			return boolValue(n.Value), nil
		case "!!int":
			return intValue(n.Value)
		case "!!float":
			return floatValue(n.Value)
		}
		return nil, l.noConstructor(n, tag)
	}
	if n.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
		return n.Value, nil
	}
	s := n.Value
	switch {
	case reBool.MatchString(s):
		return boolValue(s), nil
	case reNull.MatchString(s):
		return nil, nil
	case reInt.MatchString(s):
		return intValue(s)
	case reFloat.MatchString(s):
		return floatValue(s)
	case reTimestamp.MatchString(s):
		return timestampValue(s)
	}
	return s, nil
}
