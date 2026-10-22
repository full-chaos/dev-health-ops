package docstools

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// SafeConstructor of the PyYAML port: the nodes of the composer become Python values. The generators of the original
// (a collection is made empty, registered, and filled later) are closures kept in stateGens, so the order in which
// errors appear is the original's.

type dcYConstructor struct {
	constructed map[*dcYNode]any
	stateGens   []func() *dcYErr
}

const (
	dcYTagValue = "tag:yaml.org,2002:value"
	dcYTagMerge = "tag:yaml.org,2002:merge"
)

func dcYConsErr(context, problem string) *dcYErr {
	return dcYMarked(dcYConstructErr, context, problem)
}

// dcYAMLSafeLoad is yaml.safe_load(text) for a text that is already str.
func dcYAMLSafeLoad(text string) (any, *dcYErr) {
	p, err := newDcYParser(text)
	if err != nil {
		return nil, err
	}
	node, err := p.getSingleNode()
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	c := &dcYConstructor{constructed: map[*dcYNode]any{}}
	data, err := c.constructObject(node)
	if err != nil {
		return nil, err
	}
	for len(c.stateGens) > 0 {
		gens := c.stateGens
		c.stateGens = nil
		for _, g := range gens {
			if err := g(); err != nil {
				return nil, err
			}
		}
	}
	return data, nil
}

func (c *dcYConstructor) constructObject(node *dcYNode) (any, *dcYErr) {
	if v, ok := c.constructed[node]; ok {
		return v, nil
	}
	var data any
	var gen func() *dcYErr
	var err *dcYErr
	switch node.tag {
	case "tag:yaml.org,2002:null":
		if _, err = c.constructScalar(node); err == nil {
			data = nil
		}
	case "tag:yaml.org,2002:bool":
		data, err = c.constructBool(node)
	case "tag:yaml.org,2002:int":
		data, err = c.constructInt(node)
	case "tag:yaml.org,2002:float":
		data, err = c.constructFloat(node)
	case "tag:yaml.org,2002:binary":
		data, err = c.constructBinary(node)
	case "tag:yaml.org,2002:timestamp":
		data, err = c.constructTimestamp(node)
	case "tag:yaml.org,2002:omap":
		data, gen = c.constructOmapOrPairs(node, "while constructing an ordered map")
	case "tag:yaml.org,2002:pairs":
		data, gen = c.constructOmapOrPairs(node, "while constructing pairs")
	case "tag:yaml.org,2002:set":
		set := &pySet{}
		data = set
		gen = func() *dcYErr {
			m, err := c.constructMapping(node)
			if err != nil {
				return err
			}
			for _, k := range m.keys {
				set.items = append(set.items, k)
			}
			return nil
		}
	case "tag:yaml.org,2002:str":
		data, err = c.constructScalarStr(node)
	case "tag:yaml.org,2002:seq":
		list := &pyList{}
		data = list
		gen = func() *dcYErr {
			if node.kind != nkSeq {
				return dcYConsErr("", fmt.Sprintf("expected a sequence node, but found %s", node.id()))
			}
			for _, child := range node.items {
				v, err := c.constructObject(child)
				if err != nil {
					return err
				}
				list.items = append(list.items, v)
			}
			return nil
		}
	case "tag:yaml.org,2002:map":
		dict := newPyDict()
		data = dict
		gen = func() *dcYErr {
			m, err := c.constructMapping(node)
			if err != nil {
				return err
			}
			for i := range m.keys {
				dict.set(m.keys[i], m.vals[i])
			}
			return nil
		}
	default:
		if strings.HasPrefix(node.tag, "\x00trailing-newline ") {
			return nil, &dcYErr{"ValueError", "a plain scalar that matches an implicit type only through the newline Python's $ allows is refused (STRICTER): " + a1Repr(node.value)}
		}
		return nil, dcYConsErr("", fmt.Sprintf("could not determine a constructor for the tag %s", a1Repr(node.tag)))
	}
	if err != nil {
		return nil, err
	}
	if gen != nil {
		c.stateGens = append(c.stateGens, gen)
	}
	c.constructed[node] = data
	return data, nil
}

// constructScalar is SafeConstructor.construct_scalar.
func (c *dcYConstructor) constructScalar(node *dcYNode) (string, *dcYErr) {
	if node.kind == nkMap {
		for _, pr := range node.pairs {
			if pr[0].tag == dcYTagValue {
				return c.constructScalar(pr[1])
			}
		}
	}
	if node.kind != nkScalar {
		return "", dcYConsErr("", fmt.Sprintf("expected a scalar node, but found %s", node.id()))
	}
	return node.value, nil
}

func (c *dcYConstructor) constructScalarStr(node *dcYNode) (any, *dcYErr) {
	s, err := c.constructScalar(node)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (c *dcYConstructor) flattenMapping(node *dcYNode) *dcYErr {
	var merge [][2]*dcYNode
	index := 0
	for index < len(node.pairs) {
		keyNode, valueNode := node.pairs[index][0], node.pairs[index][1]
		switch {
		case keyNode.tag == dcYTagMerge:
			node.pairs = append(node.pairs[:index:index], node.pairs[index+1:]...)
			switch valueNode.kind {
			case nkMap:
				if err := c.flattenMapping(valueNode); err != nil {
					return err
				}
				merge = append(merge, valueNode.pairs...)
			case nkSeq:
				var sub [][][2]*dcYNode
				for _, subnode := range valueNode.items {
					if subnode.kind != nkMap {
						return dcYConsErr("while constructing a mapping", fmt.Sprintf("expected a mapping for merging, but found %s", subnode.id()))
					}
					if err := c.flattenMapping(subnode); err != nil {
						return err
					}
					sub = append(sub, subnode.pairs)
				}
				for i := len(sub) - 1; i >= 0; i-- {
					merge = append(merge, sub[i]...)
				}
			default:
				return dcYConsErr("while constructing a mapping", fmt.Sprintf("expected a mapping or list of mappings for merging, but found %s", valueNode.id()))
			}
		case keyNode.tag == dcYTagValue:
			keyNode.tag = "tag:yaml.org,2002:str"
			index++
		default:
			index++
		}
	}
	if len(merge) > 0 {
		node.pairs = append(append([][2]*dcYNode(nil), merge...), node.pairs...)
	}
	return nil
}

func dcYHashable(v any) bool {
	switch v.(type) {
	case *pyList, *pyDict, *pySet:
		return false
	}
	return true
}

// constructMapping is SafeConstructor.construct_mapping: the key and value lists in order (a later equal key replaces the
// value of the first when the caller stores them in a dict).
func (c *dcYConstructor) constructMapping(node *dcYNode) (*pyDict, *dcYErr) {
	if node.kind == nkMap {
		if err := c.flattenMapping(node); err != nil {
			return nil, err
		}
	}
	if node.kind != nkMap {
		return nil, dcYConsErr("", fmt.Sprintf("expected a mapping node, but found %s", node.id()))
	}
	out := newPyDict()
	for _, pr := range node.pairs {
		key, err := c.constructObject(pr[0])
		if err != nil {
			return nil, err
		}
		if !dcYHashable(key) {
			return nil, dcYConsErr("while constructing a mapping", "found unhashable key")
		}
		val, err := c.constructObject(pr[1])
		if err != nil {
			return nil, err
		}
		out.set(key, val)
	}
	return out, nil
}

func (c *dcYConstructor) constructOmapOrPairs(node *dcYNode, context string) (any, func() *dcYErr) {
	list := &pyList{}
	return list, func() *dcYErr {
		if node.kind != nkSeq {
			return dcYConsErr(context, fmt.Sprintf("expected a sequence, but found %s", node.id()))
		}
		for _, sub := range node.items {
			if sub.kind != nkMap {
				return dcYConsErr(context, fmt.Sprintf("expected a mapping of length 1, but found %s", sub.id()))
			}
			if len(sub.pairs) != 1 {
				return dcYConsErr(context, fmt.Sprintf("expected a single mapping item, but found %d items", len(sub.pairs)))
			}
			k, err := c.constructObject(sub.pairs[0][0])
			if err != nil {
				return err
			}
			v, err := c.constructObject(sub.pairs[0][1])
			if err != nil {
				return err
			}
			list.items = append(list.items, pyTuple{k, v})
		}
		return nil
	}
}

func (c *dcYConstructor) constructBool(node *dcYNode) (any, *dcYErr) {
	value, err := c.constructScalar(node)
	if err != nil {
		return nil, err
	}
	switch low := dcLower(value); low {
	case "yes", "true", "on":
		return true, nil
	case "no", "false", "off":
		return false, nil
	default:
		return nil, &dcYErr{"KeyError", a1Repr(low)}
	}
}

// pyIntParse is int(s, base) for ASCII text (STRICTER: a non-ASCII digit, which Python accepts, is refused).
func pyIntParse(s string, base int) (*big.Int, *dcYErr) {
	bad := func() *dcYErr {
		return &dcYErr{"ValueError", fmt.Sprintf("invalid literal for int() with base %d: %s", base, a1Repr(s))}
	}
	t := a1Strip(s)
	for _, r := range t {
		if r >= 0x80 && unicode.Is(unicode.Nd, r) {
			return nil, &dcYErr{"ValueError", fmt.Sprintf("a non-ASCII decimal digit in an integer literal is refused (STRICTER): %s", a1Repr(s))}
		}
	}
	neg := false
	if t != "" && (t[0] == '+' || t[0] == '-') {
		neg = t[0] == '-'
		t = t[1:]
	}
	if len(t) >= 2 && t[0] == '0' {
		switch {
		case base == 16 && (t[1] == 'x' || t[1] == 'X'), base == 8 && (t[1] == 'o' || t[1] == 'O'), base == 2 && (t[1] == 'b' || t[1] == 'B'):
			t = t[2:]
			if strings.HasPrefix(t, "_") {
				t = t[1:]
			}
		}
	}
	if t == "" || strings.HasPrefix(t, "_") || strings.HasSuffix(t, "_") || strings.Contains(t, "__") {
		return nil, bad()
	}
	t = strings.ReplaceAll(t, "_", "")
	if t == "" {
		return nil, bad()
	}
	if base == 10 && len(t) > 4300 {
		return nil, &dcYErr{"ValueError", fmt.Sprintf("Exceeds the limit (4300 digits) for integer string conversion: value has %d digits; use sys.set_int_max_str_digits() to increase the limit", len(t))}
	}
	n, ok := new(big.Int).SetString(t, base)
	if !ok || strings.ContainsAny(t, "+-") {
		return nil, bad()
	}
	if neg {
		n.Neg(n)
	}
	return n, nil
}

func (c *dcYConstructor) constructInt(node *dcYNode) (any, *dcYErr) {
	value, err := c.constructScalar(node)
	if err != nil {
		return nil, err
	}
	value = strings.ReplaceAll(value, "_", "")
	indexErr := &dcYErr{"IndexError", "string index out of range"}
	if value == "" {
		return nil, indexErr
	}
	sign := int64(1)
	if value[0] == '-' {
		sign = -1
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	mul := func(n *big.Int) *big.Int {
		if sign < 0 {
			return new(big.Int).Neg(n)
		}
		return n
	}
	switch {
	case value == "0":
		return big.NewInt(0), nil
	case strings.HasPrefix(value, "0b"):
		n, err := pyIntParse(value[2:], 2)
		if err != nil {
			return nil, err
		}
		return mul(n), nil
	case strings.HasPrefix(value, "0x"):
		n, err := pyIntParse(value[2:], 16)
		if err != nil {
			return nil, err
		}
		return mul(n), nil
	case value == "":
		return nil, indexErr
	case value[0] == '0':
		n, err := pyIntParse(value, 8)
		if err != nil {
			return nil, err
		}
		return mul(n), nil
	case strings.Contains(value, ":"):
		parts := strings.Split(value, ":")
		digits := make([]*big.Int, len(parts))
		for i, part := range parts {
			n, err := pyIntParse(part, 10)
			if err != nil {
				return nil, err
			}
			digits[i] = n
		}
		total, base := new(big.Int), big.NewInt(1)
		for i := len(digits) - 1; i >= 0; i-- {
			total.Add(total, new(big.Int).Mul(digits[i], base))
			base = new(big.Int).Mul(base, big.NewInt(60))
		}
		return mul(total), nil
	}
	n, err := pyIntParse(value, 10)
	if err != nil {
		return nil, err
	}
	return mul(n), nil
}

var dcYFloatText = regexp.MustCompile(`\A[+-]?(?:(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|inf|infinity|nan)\z`)

// pyFloatParse is float(s) for ASCII text.
func pyFloatParse(s string) (float64, *dcYErr) {
	t := a1Strip(s)
	for _, r := range s {
		if r >= 0x80 && unicode.Is(unicode.Nd, r) {
			return 0, &dcYErr{"ValueError", "a non-ASCII decimal digit in a float literal is refused (STRICTER): " + a1Repr(s)}
		}
	}
	if !dcYFloatText.MatchString(strings.ToLower(t)) {
		return 0, &dcYErr{"ValueError", "could not convert string to float: " + a1Repr(s)}
	}
	lt := strings.ToLower(t)
	switch strings.TrimLeft(lt, "+-") {
	case "inf", "infinity":
		if strings.HasPrefix(lt, "-") {
			return math.Inf(-1), nil
		}
		return math.Inf(1), nil
	case "nan":
		return math.NaN(), nil
	}
	f, err := strconv.ParseFloat(lt, 64)
	if err != nil && !strings.Contains(err.Error(), "range") {
		return 0, &dcYErr{"ValueError", "could not convert string to float: " + a1Repr(s)}
	}
	return f, nil
}

func (c *dcYConstructor) constructFloat(node *dcYNode) (any, *dcYErr) {
	value, err := c.constructScalar(node)
	if err != nil {
		return nil, err
	}
	value = dcLower(strings.ReplaceAll(value, "_", ""))
	if value == "" {
		return nil, &dcYErr{"IndexError", "string index out of range"}
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
		parts := strings.Split(value, ":")
		digits := make([]float64, len(parts))
		for i, part := range parts {
			f, err := pyFloatParse(part)
			if err != nil {
				return nil, err
			}
			digits[i] = f
		}
		total, base := 0.0, 1.0
		for i := len(digits) - 1; i >= 0; i-- {
			total += digits[i] * base
			base *= 60
		}
		return sign * total, nil
	}
	f, err := pyFloatParse(value)
	if err != nil {
		return nil, err
	}
	return sign * f, nil
}

func (c *dcYConstructor) constructBinary(node *dcYNode) (any, *dcYErr) {
	text, err := c.constructScalar(node)
	if err != nil {
		return nil, err
	}
	runes := []rune(text)
	for i, r := range runes {
		if r > 0x7f {
			j := i
			for j+1 < len(runes) && runes[j+1] > 0x7f {
				j++
			}
			var msg string
			if j == i {
				esc := fmt.Sprintf(`\x%02x`, r)
				switch {
				case r > 0xffff:
					esc = fmt.Sprintf(`\U%08x`, r)
				case r > 0xff:
					esc = fmt.Sprintf(`\u%04x`, r)
				}
				msg = fmt.Sprintf("'ascii' codec can't encode character '%s' in position %d: ordinal not in range(128)", esc, i)
			} else {
				msg = fmt.Sprintf("'ascii' codec can't encode characters in position %d-%d: ordinal not in range(128)", i, j)
			}
			return nil, dcYConsErr("", "failed to convert base64 data into ascii: "+msg)
		}
	}
	out, derr := dcB64DecodeBytes([]byte(text))
	if derr != "" {
		return nil, dcYConsErr("", "failed to decode base64 data: "+derr)
	}
	return pyBytes(out), nil
}

// dcB64DecodeBytes is base64.decodebytes (binascii.a2b_base64, not strict): characters outside the alphabet are skipped.
func dcB64DecodeBytes(in []byte) ([]byte, string) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var out []byte
	quad, left, pads := 0, 0, 0
	padDone := false
	for _, ch := range in {
		if ch == '=' {
			if quad >= 2 {
				pads++
				if quad+pads >= 4 {
					padDone = true
				}
			}
			continue
		}
		v := strings.IndexByte(alphabet, ch)
		if v < 0 {
			continue
		}
		pads, padDone = 0, false
		switch quad {
		case 0:
			quad, left = 1, v
		case 1:
			quad = 2
			out = append(out, byte(left<<2|v>>4))
			left = v & 0x0f
		case 2:
			quad = 3
			out = append(out, byte(left<<4|v>>2))
			left = v & 0x03
		case 3:
			quad = 0
			out = append(out, byte(left<<6|v))
			left = 0
		}
	}
	if quad != 0 && !(padDone && quad >= 2) {
		if quad == 1 {
			return nil, fmt.Sprintf("Invalid base64-encoded string: number of data characters (%d) cannot be 1 more than a multiple of 4", len(out)/3*4+1)
		}
		return nil, "Incorrect padding"
	}
	return out, ""
}

var dcYTimestampRe = regexp.MustCompile(`\A(?P<year>[0-9][0-9][0-9][0-9])-(?P<month>[0-9][0-9]?)-(?P<day>[0-9][0-9]?)(?:(?:[Tt]|[ \t]+)(?P<hour>[0-9][0-9]?):(?P<minute>[0-9][0-9]):(?P<second>[0-9][0-9])(?:\.(?P<fraction>[0-9]*))?(?:[ \t]*(?P<tz>Z|(?P<tz_sign>[-+])(?P<tz_hour>[0-9][0-9]?)(?::(?P<tz_minute>[0-9][0-9]))?))?)?\z`)

func dcDaysInMonth(y, m int) int {
	switch m {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	}
	if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
		return 29
	}
	return 28
}

func (c *dcYConstructor) constructTimestamp(node *dcYNode) (any, *dcYErr) {
	if _, err := c.constructScalar(node); err != nil {
		return nil, err
	}
	if node.kind != nkScalar {
		return nil, &dcYErr{"TypeError", "expected string or bytes-like object, got 'list'"}
	}
	text := node.value
	m := dcYTimestampRe.FindStringSubmatch(text)
	if m == nil && strings.HasSuffix(text, "\n") {
		m = dcYTimestampRe.FindStringSubmatch(text[:len(text)-1])
	}
	if m == nil {
		return nil, &dcYErr{"AttributeError", "'NoneType' object has no attribute 'groupdict'"}
	}
	g := map[string]string{}
	for i, name := range dcYTimestampRe.SubexpNames() {
		if name != "" {
			g[name] = m[i]
		}
	}
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	year, month, day := atoi(g["year"]), atoi(g["month"]), atoi(g["day"])
	checkDate := func() *dcYErr {
		if year < 1 {
			return &dcYErr{"ValueError", fmt.Sprintf("year must be in 1..9999, not %d", year)}
		}
		if month < 1 || month > 12 {
			return &dcYErr{"ValueError", fmt.Sprintf("month must be in 1..12, not %d", month)}
		}
		if dim := dcDaysInMonth(year, month); day < 1 || day > dim {
			return &dcYErr{"ValueError", fmt.Sprintf("day %d must be in range 1..%d for month %d in year %d", day, dim, month, year)}
		}
		return nil
	}
	if g["hour"] == "" {
		if err := checkDate(); err != nil {
			return nil, err
		}
		return pyDate{year, month, day}, nil
	}
	hour, minute, second := atoi(g["hour"]), atoi(g["minute"]), atoi(g["second"])
	fraction := 0
	if g["fraction"] != "" {
		f := g["fraction"]
		if len(f) > 6 {
			f = f[:6]
		}
		for len(f) < 6 {
			f += "0"
		}
		fraction = atoi(f)
	}
	dt := pyDatetime{y: year, mo: month, d: day, h: hour, mi: minute, s: second, us: fraction}
	if g["tz_sign"] != "" {
		delta := atoi(g["tz_hour"])*3600 + atoi(g["tz_minute"])*60
		if g["tz_sign"] == "-" {
			delta = -delta
		}
		if delta <= -86400 || delta >= 86400 {
			return nil, &dcYErr{"ValueError", "offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not " + pyTimedeltaRepr(delta)}
		}
		dt.hasTZ, dt.offsetSeconds = true, delta
	} else if g["tz"] != "" {
		dt.hasTZ = true
	}
	if err := checkDate(); err != nil {
		return nil, err
	}
	switch {
	case hour > 23:
		return nil, &dcYErr{"ValueError", fmt.Sprintf("hour must be in 0..23, not %d", hour)}
	case minute > 59:
		return nil, &dcYErr{"ValueError", fmt.Sprintf("minute must be in 0..59, not %d", minute)}
	case second > 59:
		return nil, &dcYErr{"ValueError", fmt.Sprintf("second must be in 0..59, not %d", second)}
	}
	return dt, nil
}
