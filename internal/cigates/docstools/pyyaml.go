package docstools

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// yaml.safe_load of PyYAML 6.0.3 for the verbs that read YAML (check-freshness, check-external-links), prefix yl. The
// text is parsed by yaml.v3; every node is then typed and constructed the way PyYAML's SafeLoader does (its YAML 1.1
// implicit resolvers, its int/float/bool/null/timestamp constructors and their ValueErrors, last-wins duplicate keys),
// so a value that is a str to Python is a str here and a document Python refuses is refused here.
//
// Stricter than PyYAML, on purpose (each is an error here, while Python may accept): anchors and aliases, merge keys
// (<<) and explicit tags ("!" alone is not one: PyYAML resolves it as an untagged plain scalar, and so does this).

// ylError is a refusal of the YAML loader: kind is the Python exception line's kind.
type ylError struct{ kind, msg string }

func (e ylError) Error() string { return e.kind + ": " + e.msg }

type ylKind int

const (
	ylNone ylKind = iota
	ylBool
	ylInt
	ylFloat
	ylStr
	ylDate
	ylDateTime
	ylList
	ylMap
)

type ylVal struct {
	kind                     ylKind
	b                        bool
	i                        *big.Int
	f                        float64
	s                        string
	y, mo, d, h, mi, sec, us int
	tz                       *int
	items                    []*ylVal
	keys, vals               []*ylVal
}

// ylNonPrintable is PyYAML Reader.NON_PRINTABLE.
var ylNonPrintable = regexp.MustCompile("[^\\x{9}\\x{A}\\x{D}\\x{20}-\\x{7E}\\x{85}\\x{A0}-\\x{D7FF}\\x{E000}-\\x{FFFD}\\x{10000}-\\x{10FFFF}]")

const ylConstructor = "yaml.constructor.ConstructorError"

// ylLoad is yaml.safe_load(text); a document with nothing in it is None.
func ylLoad(text string) (*ylVal, error) {
	if loc := ylNonPrintable.FindStringIndex(text); loc != nil {
		r, _ := utf8.DecodeRuneInString(text[loc[0]:])
		return nil, ylError{"yaml.reader.ReaderError", fmt.Sprintf("unacceptable character #x%04x: special characters are not allowed", r)}
	}
	dec := yaml.NewDecoder(strings.NewReader(text))
	var doc yaml.Node
	if err := dec.Decode(&doc); err == io.EOF {
		return &ylVal{kind: ylNone}, nil
	} else if err != nil {
		return nil, ylError{"yaml.YAMLError", err.Error()}
	}
	var more yaml.Node
	if err := dec.Decode(&more); err != io.EOF {
		return nil, ylError{"yaml.composer.ComposerError", "expected a single document in the stream"}
	}
	if len(doc.Content) == 0 {
		return &ylVal{kind: ylNone}, nil
	}
	c := ylConstruct{lines: ylLines(text)}
	return c.node(doc.Content[0], false)
}

// ylLines splits text at the line breaks of the YAML reader, so a node's Line and Column (in characters) index it.
func ylLines(text string) [][]rune {
	var lines [][]rune
	var cur []rune
	for _, r := range text {
		switch r {
		case '\n', 0x85, 0x2028, 0x2029:
			lines = append(lines, cur)
			cur = nil
		default:
			cur = append(cur, r)
		}
	}
	return append(lines, cur)
}

type ylConstruct struct{ lines [][]rune }

func (c ylConstruct) refuse(n *yaml.Node, what string) error {
	return ylError{"RefusedYAML", fmt.Sprintf("%s at line %d, column %d is not supported (stricter than yaml.safe_load)", what, n.Line, n.Column)}
}

// bang reports the non-specific tag "!" on the node: yaml.v3 drops it, but the node starts at it. PyYAML resolves a
// "!" scalar as a plain one, quoted or not.
func (c ylConstruct) bang(n *yaml.Node) bool {
	if n.Line < 1 || n.Line > len(c.lines) {
		return false
	}
	line := c.lines[n.Line-1]
	if n.Line == 1 && len(line) > 0 && line[0] == 0xFEFF {
		line = line[1:]
	}
	return n.Column >= 1 && n.Column <= len(line) && line[n.Column-1] == '!'
}

func (c ylConstruct) node(n *yaml.Node, isKey bool) (*ylVal, error) {
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return nil, c.refuse(n, "an anchor or alias")
	}
	if n.Style&yaml.TaggedStyle != 0 {
		return nil, c.refuse(n, "an explicit tag")
	}
	switch n.Kind {
	case yaml.ScalarNode:
		if !c.bang(n) && n.Style&(yaml.DoubleQuotedStyle|yaml.SingleQuotedStyle|yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
			return &ylVal{kind: ylStr, s: n.Value}, nil
		}
		return c.plain(n, isKey)
	case yaml.SequenceNode:
		out := &ylVal{kind: ylList}
		for _, child := range n.Content {
			v, err := c.node(child, false)
			if err != nil {
				return nil, err
			}
			out.items = append(out.items, v)
		}
		return out, nil
	case yaml.MappingNode:
		out := &ylVal{kind: ylMap}
		for i := 0; i+1 < len(n.Content); i += 2 {
			kn, vn := n.Content[i], n.Content[i+1]
			if kn.Kind != yaml.ScalarNode {
				if kn.Kind == yaml.AliasNode || kn.Anchor != "" || kn.Style&yaml.TaggedStyle != 0 {
					return nil, c.refuse(kn, "an anchor, alias or tag")
				}
				return nil, ylError{ylConstructor, "while constructing a mapping: found unhashable key"}
			}
			k, err := c.node(kn, true)
			if err != nil {
				return nil, err
			}
			v, err := c.node(vn, false)
			if err != nil {
				return nil, err
			}
			out.set(k, v)
		}
		return out, nil
	}
	return nil, c.refuse(n, "a node")
}

var (
	ylBoolRe    = regexp.MustCompile(`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	ylFloatRe   = regexp.MustCompile(`^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$`)
	ylIntRe     = regexp.MustCompile(`^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$`)
	ylNullRe    = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	ylTimeRe    = regexp.MustCompile(`^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$`)
	ylTimeParts = regexp.MustCompile(`^([0-9][0-9][0-9][0-9])-([0-9][0-9]?)-([0-9][0-9]?)(?:(?:[Tt]|[ \t]+)([0-9][0-9]?):([0-9][0-9]):([0-9][0-9])(?:\.([0-9]*))?(?:[ \t]*(Z|([-+])([0-9][0-9]?)(?::([0-9][0-9]))?))?)?$`)
)

// plain types a plain scalar with PyYAML's implicit resolvers, in their order for the first character, and constructs it.
func (c ylConstruct) plain(n *yaml.Node, isKey bool) (*ylVal, error) {
	v := n.Value
	// Python's $ also matches before one final newline; a "!"-tagged block scalar can end with one.
	m := strings.TrimSuffix(v, "\n")
	first := byte(0)
	if v != "" {
		first = v[0]
	}
	in := func(set string) bool { return v != "" && strings.IndexByte(set, first) >= 0 }
	switch {
	case in("yYnNtTfFoO") && ylBoolRe.MatchString(m):
		l := strings.ToLower(v)
		if v != m {
			return nil, ylError{"KeyError", a1Repr(l)}
		}
		return &ylVal{kind: ylBool, b: l == "yes" || l == "true" || l == "on"}, nil
	case in("-+0123456789.") && ylFloatRe.MatchString(m):
		return ylParseFloat(v)
	case in("-+0123456789") && ylIntRe.MatchString(m):
		return ylParseInt(v)
	case first == '<' && m == "<<":
		return nil, c.refuse(n, "a merge key")
	case (v == "" || in("~nN")) && ylNullRe.MatchString(m):
		return &ylVal{kind: ylNone}, nil
	case in("0123456789") && ylTimeRe.MatchString(m):
		return ylTimestamp(m)
	case first == '=' && m == "=":
		if isKey {
			return &ylVal{kind: ylStr, s: v}, nil
		}
		return nil, ylError{ylConstructor, "could not determine a constructor for the tag 'tag:yaml.org,2002:value'"}
	}
	return &ylVal{kind: ylStr, s: v}, nil
}

func ylSign(v string) (int, string) {
	switch {
	case strings.HasPrefix(v, "-"):
		return -1, v[1:]
	case strings.HasPrefix(v, "+"):
		return 1, v[1:]
	}
	return 1, v
}

func ylParseInt(raw string) (*ylVal, error) {
	sign, v := ylSign(strings.ReplaceAll(raw, "_", ""))
	n := new(big.Int)
	parse := func(s string, base int) error {
		t := a1Strip(s)
		if _, ok := n.SetString(t, base); !ok || t == "" || strings.ContainsAny(t, "+-") {
			return ylError{"ValueError", fmt.Sprintf("invalid literal for int() with base %d: %s", base, a1Repr(s))}
		}
		return nil
	}
	var err error
	switch {
	case v == "0":
	case strings.HasPrefix(v, "0b"):
		err = parse(v[2:], 2)
	case strings.HasPrefix(v, "0x"):
		err = parse(v[2:], 16)
	case strings.HasPrefix(v, "0"):
		err = parse(v, 8)
	case strings.Contains(v, ":"):
		parts := strings.Split(v, ":")
		base := big.NewInt(1)
		for i := len(parts) - 1; i >= 0; i-- {
			d, _ := new(big.Int).SetString(a1Strip(parts[i]), 10)
			n.Add(n, new(big.Int).Mul(d, base))
			base.Mul(base, big.NewInt(60))
		}
	default:
		err = parse(v, 10)
	}
	if err != nil {
		return nil, err
	}
	if sign < 0 {
		n.Neg(n)
	}
	return &ylVal{kind: ylInt, i: n}, nil
}

func ylParseFloat(raw string) (*ylVal, error) {
	sign, v := ylSign(strings.ToLower(strings.ReplaceAll(raw, "_", "")))
	var f float64
	switch {
	case v == ".inf":
		f = math.Inf(1)
	case v == ".nan":
		return &ylVal{kind: ylFloat, f: math.NaN()}, nil
	case strings.Contains(v, ":"):
		parts := strings.Split(v, ":")
		base := 1.0
		for i := len(parts) - 1; i >= 0; i-- {
			d, _ := strconv.ParseFloat(a1Strip(parts[i]), 64)
			f += d * base
			base *= 60
		}
	default:
		t := a1Strip(v)
		parsed, err := strconv.ParseFloat(t, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) || strings.HasPrefix(t, ".inf") || strings.HasPrefix(t, ".nan") {
			return nil, ylError{"ValueError", "could not convert string to float: " + a1Repr(v)}
		}
		f = parsed
	}
	return &ylVal{kind: ylFloat, f: float64(sign) * f}, nil
}

func ylDaysIn(y, m int) int {
	return time.Date(y, time.Month(m)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// ylCheckDate is the ValueError of datetime.date(y, m, d).
func ylCheckDate(y, m, d int) error {
	switch {
	case y < 1 || y > 9999:
		return ylError{"ValueError", fmt.Sprintf("year must be in 1..9999, not %d", y)}
	case m < 1 || m > 12:
		return ylError{"ValueError", fmt.Sprintf("month must be in 1..12, not %d", m)}
	case d < 1 || d > ylDaysIn(y, m):
		return ylError{"ValueError", fmt.Sprintf("day %d must be in range 1..%d for month %d in year %d", d, ylDaysIn(y, m), m, y)}
	}
	return nil
}

func ylTimestamp(v string) (*ylVal, error) {
	m := ylTimeParts.FindStringSubmatch(v)
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	out := &ylVal{kind: ylDate, y: atoi(m[1]), mo: atoi(m[2]), d: atoi(m[3])}
	if m[4] == "" {
		return out, ylCheckDate(out.y, out.mo, out.d)
	}
	out.kind, out.h, out.mi, out.sec = ylDateTime, atoi(m[4]), atoi(m[5]), atoi(m[6])
	if m[7] != "" {
		frac := m[7]
		if len(frac) > 6 {
			frac = frac[:6]
		}
		out.us = atoi(frac + strings.Repeat("0", 6-len(frac)))
	}
	if m[9] != "" {
		off := atoi(m[10])*3600 + atoi(m[11])*60
		if m[9] == "-" {
			off = -off
		}
		if off <= -86400 || off >= 86400 {
			return nil, ylError{"ValueError", "offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not " + ylTimedeltaRepr(off)}
		}
		out.tz = &off
	} else if m[8] == "Z" {
		zero := 0
		out.tz = &zero
	}
	if err := ylCheckDate(out.y, out.mo, out.d); err != nil {
		return nil, err
	}
	switch {
	case out.h > 23:
		return nil, ylError{"ValueError", fmt.Sprintf("hour must be in 0..23, not %d", out.h)}
	case out.mi > 59:
		return nil, ylError{"ValueError", fmt.Sprintf("minute must be in 0..59, not %d", out.mi)}
	case out.sec > 59:
		return nil, ylError{"ValueError", fmt.Sprintf("second must be in 0..59, not %d", out.sec)}
	}
	return out, nil
}

// set is mapping[key] = value: an equal key keeps its first place and takes the new value.
func (m *ylVal) set(k, v *ylVal) {
	for i, old := range m.keys {
		if ylEqual(old, k) {
			m.vals[i] = v
			return
		}
	}
	m.keys = append(m.keys, k)
	m.vals = append(m.vals, v)
}

// get is dict.get(name) for a str key.
func (m *ylVal) get(name string) *ylVal {
	for i, k := range m.keys {
		if k.kind == ylStr && k.s == name {
			return m.vals[i]
		}
	}
	return nil
}

func ylNumber(v *ylVal) (*big.Float, bool) {
	switch v.kind {
	case ylBool:
		if v.b {
			return big.NewFloat(1), true
		}
		return big.NewFloat(0), true
	case ylInt:
		return new(big.Float).SetPrec(0).SetInt(v.i), true
	case ylFloat:
		if math.IsNaN(v.f) || math.IsInf(v.f, 0) {
			return nil, false
		}
		return big.NewFloat(v.f), true
	}
	return nil, false
}

// ylEqual is == between two hashable keys of a constructed mapping.
func ylEqual(a, b *ylVal) bool {
	if fa, ok := ylNumber(a); ok {
		if fb, ok := ylNumber(b); ok {
			return fa.Cmp(fb) == 0
		}
		return false
	}
	if a.kind == ylFloat && b.kind == ylFloat {
		// PyYAML returns one shared nan object for every .nan, so two .nan keys are the same key.
		return a.f == b.f || math.IsNaN(a.f) && math.IsNaN(b.f)
	}
	if a.kind != b.kind {
		return false
	}
	switch a.kind {
	case ylNone:
		return true
	case ylStr:
		return a.s == b.s
	case ylDate:
		return a.y == b.y && a.mo == b.mo && a.d == b.d
	case ylDateTime:
		if (a.tz == nil) != (b.tz == nil) {
			return false
		}
		ta, tb := ylInstant(a), ylInstant(b)
		return ta.Equal(tb)
	}
	return false
}

func ylInstant(v *ylVal) time.Time {
	off := 0
	if v.tz != nil {
		off = *v.tz
	}
	return time.Date(v.y, time.Month(v.mo), v.d, v.h, v.mi, v.sec, v.us*1000, time.FixedZone("", off))
}

// falsy is the value being false in `x or []`.
func (v *ylVal) falsy() bool {
	switch v.kind {
	case ylNone:
		return true
	case ylBool:
		return !v.b
	case ylInt:
		return v.i.Sign() == 0
	case ylFloat:
		return v.f == 0
	case ylStr:
		return v.s == ""
	case ylList:
		return len(v.items) == 0
	case ylMap:
		return len(v.keys) == 0
	}
	return false
}

// repr is Python's repr() of the constructed value.
func (v *ylVal) repr() string {
	switch v.kind {
	case ylNone:
		return "None"
	case ylBool:
		if v.b {
			return "True"
		}
		return "False"
	case ylInt:
		return v.i.String()
	case ylFloat:
		return ylFloatRepr(v.f)
	case ylStr:
		return a1Repr(v.s)
	case ylDate:
		return fmt.Sprintf("datetime.date(%d, %d, %d)", v.y, v.mo, v.d)
	case ylDateTime:
		fields := []int{v.y, v.mo, v.d, v.h, v.mi, v.sec, v.us}
		if v.us == 0 {
			fields = fields[:6]
			if v.sec == 0 {
				fields = fields[:5]
			}
		}
		parts := make([]string, len(fields))
		for i, f := range fields {
			parts[i] = strconv.Itoa(f)
		}
		s := "datetime.datetime(" + strings.Join(parts, ", ")
		if v.tz != nil {
			s += ", tzinfo=" + ylTZRepr(*v.tz)
		}
		return s + ")"
	case ylList:
		parts := make([]string, len(v.items))
		for i, it := range v.items {
			parts[i] = it.repr()
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case ylMap:
		parts := make([]string, len(v.keys))
		for i := range v.keys {
			parts[i] = v.keys[i].repr() + ": " + v.vals[i].repr()
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return "?"
}

func ylTZRepr(off int) string {
	if off == 0 {
		return "datetime.timezone.utc"
	}
	return "datetime.timezone(" + ylTimedeltaRepr(off) + ")"
}

// ylTimedeltaRepr is repr(timedelta(seconds=secs)): days floored, zero fields left out.
func ylTimedeltaRepr(secs int) string {
	days := secs / 86400
	if secs%86400 < 0 {
		days--
	}
	rem := secs - days*86400
	var parts []string
	if days != 0 {
		parts = append(parts, fmt.Sprintf("days=%d", days))
	}
	if rem != 0 {
		parts = append(parts, fmt.Sprintf("seconds=%d", rem))
	}
	if len(parts) == 0 {
		return "datetime.timedelta(0)"
	}
	return "datetime.timedelta(" + strings.Join(parts, ", ") + ")"
}

// ylFloatRepr is repr(float): the shortest round-trip digits, positional for exponents -4..15, else scientific.
func ylFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64)
	mant, expText, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expText)
	if exp >= -4 && exp < 16 {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		if !strings.Contains(s, ".") {
			s += ".0"
		}
		return s
	}
	sign := "+"
	if exp < 0 {
		sign, exp = "-", -exp
	}
	return fmt.Sprintf("%se%s%02d", mant, sign, exp)
}
