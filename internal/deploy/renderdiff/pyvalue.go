package renderdiff

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The kit tool is Python. The port keeps Python's value model, because its output prints Python str()/repr() of the
// loaded values and of the rows file, and its comparisons follow Python types (True is not 1 in same()).
//
//	None      -> nil
//	bool      -> bool
//	int       -> *big.Int
//	float     -> float64
//	str       -> string
//	bytes     -> pyBytes
//	date      -> *pyDate
//	datetime  -> *pyDateTime
//	dict      -> *pyDict (insertion order; Python key equality: 1 == 1.0 == True)
//	list      -> []any
//	tuple     -> pyTuple (the rows file signatures only)
//	_NONE     -> missing
type pyBytes string

type pyTuple []any

type missingType struct{}

var missing = &missingType{}

type pyDate struct{ y, m, d int }

type pyDateTime struct {
	y, mo, d, h, mi, s, us int
	aware                  bool
	offset                 int // seconds east of UTC; only when aware
}

type pyDict struct {
	keys []any
	vals []any
	idx  map[string]int
	nan  int
}

func newDict() *pyDict { return &pyDict{idx: map[string]int{}} }

func (d *pyDict) Len() int { return len(d.keys) }

// set keeps the first key object and its position and replaces the value (Python dict assignment).
func (d *pyDict) set(k, v any) error {
	h, ok := d.hashKey(k)
	if !ok {
		return errUnhashable
	}
	if i, found := d.idx[h]; found {
		d.vals[i] = v
		return nil
	}
	d.idx[h] = len(d.keys)
	d.keys = append(d.keys, k)
	d.vals = append(d.vals, v)
	return nil
}

func (d *pyDict) get(k any) (any, bool) {
	h, ok := hashKey(k)
	if !ok {
		return nil, false
	}
	i, found := d.idx[h]
	if !found {
		return nil, false
	}
	return d.vals[i], true
}

func (d *pyDict) has(k any) bool {
	_, ok := d.get(k)
	return ok
}

func (d *pyDict) copy() *pyDict {
	c := &pyDict{keys: append([]any(nil), d.keys...), vals: append([]any(nil), d.vals...), idx: make(map[string]int, len(d.idx)), nan: d.nan}
	for k, v := range d.idx {
		c.idx[k] = v
	}
	return c
}

type unhashableError struct{}

func (unhashableError) Error() string { return "unhashable key" }

var errUnhashable error = unhashableError{}

// hashKey is the dict key identity under Python equality. A NaN key is never equal to another (Python keeps each NaN
// object apart), so each one gets its own slot.
func (d *pyDict) hashKey(k any) (string, bool) {
	if f, ok := k.(float64); ok && math.IsNaN(f) && !isSharedNaN(f) {
		d.nan++
		return "nan:" + strconv.Itoa(d.nan), true
	}
	return hashKey(k)
}

func hashKey(k any) (string, bool) {
	switch v := asKit(k).(type) {
	case nil:
		return "N", true
	case bool:
		if v {
			return "n:1", true
		}
		return "n:0", true
	case *big.Int:
		return "n:" + v.String(), true
	case float64:
		if isSharedNaN(v) {
			return "nan:shared", true
		}
		if math.IsNaN(v) {
			return "", true
		}
		if !math.IsInf(v, 0) && v == math.Trunc(v) {
			bf := new(big.Float).SetFloat64(v)
			bi, _ := bf.Int(nil)
			return "n:" + bi.String(), true
		}
		return "f:" + strconv.FormatFloat(v, 'g', -1, 64), true
	case string:
		return "s:" + v, true
	case pyBytes:
		return "b:" + string(v), true
	case *pyDate:
		return fmt.Sprintf("d:%d-%d-%d", v.y, v.m, v.d), true
	case *pyDateTime:
		if v.aware {
			return "A:" + strconv.FormatInt(v.utcMicros(), 10), true
		}
		return fmt.Sprintf("T:%d-%d-%d-%d-%d-%d-%d", v.y, v.mo, v.d, v.h, v.mi, v.s, v.us), true
	case pyTuple:
		parts := make([]string, len(v))
		for i, e := range v {
			h, ok := hashKey(e)
			if !ok {
				return "", false
			}
			parts[i] = strconv.Quote(h)
		}
		return "t:(" + strings.Join(parts, ",") + ")", true
	}
	return "", false
}

// sharedNaN is PyYAML's SafeConstructor.nan_value: every `.nan` scalar is that ONE float object, so all of them are
// one dict key (Python's dict compares a key by identity first). A NaN from
// float('nan') (a JSON NaN, an explicit !!float nan) is a new object each time. The port tells them apart by payload.
const sharedNaNBits = 0x7ff8000000000ace

var sharedNaN = math.Float64frombits(sharedNaNBits)

func isSharedNaN(f float64) bool { return math.Float64bits(f) == sharedNaNBits }

func (t *pyDateTime) utcMicros() int64 {
	days := daysFromCivil(t.y, t.mo, t.d)
	secs := days*86400 + int64(t.h*3600+t.mi*60+t.s) - int64(t.offset)
	return secs*1_000_000 + int64(t.us)
}

func daysFromCivil(y, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := y / 400
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return int64(era)*146097 + int64(doe) - 719468
}

// pyEq is Python `x == y` for the values same() and rows_gate meet (scalars, empty containers, tuples).
func pyEq(a, b any) bool {
	a, b = asKit(a), asKit(b)
	if na, ok := numeric(a); ok {
		nb, ok := numeric(b)
		return ok && numEq(na, nb)
	}
	switch x := a.(type) {
	case nil:
		return b == nil
	case string:
		y, ok := b.(string)
		return ok && x == y
	case pyBytes:
		y, ok := b.(pyBytes)
		return ok && x == y
	case *pyDate:
		y, ok := b.(*pyDate)
		return ok && *x == *y
	case *pyDateTime:
		y, ok := b.(*pyDateTime)
		if !ok || x.aware != y.aware {
			return false
		}
		if x.aware {
			return x.utcMicros() == y.utcMicros()
		}
		return *x == *y
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !(identical(x[i], y[i]) || pyEq(x[i], y[i])) {
				return false
			}
		}
		return true
	case pyTuple:
		y, ok := b.(pyTuple)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !(identical(x[i], y[i]) || pyEq(x[i], y[i])) {
				return false
			}
		}
		return true
	case *pyDict:
		y, ok := b.(*pyDict)
		if !ok || x.Len() != y.Len() {
			return false
		}
		for i, k := range x.keys {
			v, found := y.get(k)
			if !found || !(identical(x.vals[i], v) || pyEq(x.vals[i], v)) {
				return false
			}
		}
		return true
	case *missingType:
		return a == b
	}
	return false
}

// identical stands in for Python's `is` shortcut inside container equality (a NaN equals itself there).
func identical(a, b any) bool {
	switch x := a.(type) {
	case *pyDict:
		y, ok := b.(*pyDict)
		return ok && x == y
	case *pyDate:
		y, ok := b.(*pyDate)
		return ok && x == y
	case *pyDateTime:
		y, ok := b.(*pyDateTime)
		return ok && x == y
	}
	return false
}

type pyNum struct {
	i   *big.Int
	f   float64
	isF bool
}

func numeric(v any) (pyNum, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return pyNum{i: big.NewInt(1)}, true
		}
		return pyNum{i: big.NewInt(0)}, true
	case *big.Int:
		return pyNum{i: x}, true
	case float64:
		return pyNum{f: x, isF: true}, true
	}
	return pyNum{}, false
}

func numCmp(a, b pyNum) (int, bool) {
	if !a.isF && !b.isF {
		return a.i.Cmp(b.i), true
	}
	if a.isF && math.IsNaN(a.f) || b.isF && math.IsNaN(b.f) {
		return 0, false
	}
	fa, fb := toBigFloat(a), toBigFloat(b)
	if fa == nil || fb == nil {
		ia, ib := infSign(a), infSign(b)
		switch {
		case ia < ib:
			return -1, true
		case ia > ib:
			return 1, true
		}
		return 0, true
	}
	return fa.Cmp(fb), true
}

func infSign(n pyNum) int {
	if n.isF && math.IsInf(n.f, 1) {
		return 1
	}
	if n.isF && math.IsInf(n.f, -1) {
		return -1
	}
	return 0
}

func toBigFloat(n pyNum) *big.Float {
	if n.isF {
		if math.IsInf(n.f, 0) {
			return nil
		}
		return new(big.Float).SetPrec(2048).SetFloat64(n.f)
	}
	return new(big.Float).SetPrec(2048).SetInt(n.i)
}

func numEq(a, b pyNum) bool {
	c, ok := numCmp(a, b)
	return ok && c == 0
}

// pyTypeName is type(x).__name__, for same()'s `type(x) is type(y)`.
func pyTypeName(v any) string {
	switch asKit(v).(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case *big.Int:
		return "int"
	case float64:
		return "float"
	case string:
		return "str"
	case pyBytes:
		return "bytes"
	case *pyDate:
		return "date"
	case *pyDateTime:
		return "datetime"
	case *pyDict:
		return "dict"
	case []any:
		return "list"
	case pyTuple:
		return "tuple"
	case *missingType:
		return "_NoneType"
	}
	return fmt.Sprintf("%T", v)
}

// truthy is Python bool(x).
func truthy(v any) bool {
	switch x := asKit(v).(type) {
	case nil:
		return false
	case bool:
		return x
	case *big.Int:
		return x.Sign() != 0
	case float64:
		return x != 0
	case string:
		return x != ""
	case pyBytes:
		return x != ""
	case *pyDict:
		return x.Len() > 0
	case []any:
		return len(x) > 0
	case pyTuple:
		return len(x) > 0
	}
	return true
}

// pyStr is Python str(x).
func pyStr(v any) string {
	switch x := asKit(v).(type) {
	case string:
		return x
	case *pyDate:
		return fmt.Sprintf("%04d-%02d-%02d", x.y, x.m, x.d)
	case *pyDateTime:
		s := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", x.y, x.mo, x.d, x.h, x.mi, x.s)
		if x.us != 0 {
			s += fmt.Sprintf(".%06d", x.us)
		}
		if x.aware {
			s += utcOffsetText(x.offset)
		}
		return s
	}
	return pyRepr(v)
}

func utcOffsetText(off int) string {
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	return fmt.Sprintf("%s%02d:%02d", sign, off/3600, off%3600/60)
}

// pyRepr is Python repr(x).
func pyRepr(v any) string {
	switch x := asKit(v).(type) {
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case *big.Int:
		return pyIntStr(x)
	case float64:
		return pyFloatRepr(x)
	case string:
		return pyStrRepr(x)
	case pyBytes:
		return pyBytesRepr(string(x))
	case *pyDate:
		return fmt.Sprintf("datetime.date(%d, %d, %d)", x.y, x.m, x.d)
	case *pyDateTime:
		parts := []int{x.y, x.mo, x.d, x.h, x.mi, x.s, x.us}
		if parts[6] == 0 {
			parts = parts[:6]
			if parts[5] == 0 {
				parts = parts[:5]
			}
		}
		ss := make([]string, len(parts))
		for i, p := range parts {
			ss[i] = strconv.Itoa(p)
		}
		s := "datetime.datetime(" + strings.Join(ss, ", ")
		if x.aware {
			s += ", tzinfo=" + tzRepr(x.offset)
		}
		return s + ")"
	case *pyDict:
		parts := make([]string, x.Len())
		for i, k := range x.keys {
			parts[i] = pyRepr(k) + ": " + pyRepr(x.vals[i])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = pyRepr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case pyTuple:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = pyRepr(e)
		}
		if len(parts) == 1 {
			return "(" + parts[0] + ",)"
		}
		return "(" + strings.Join(parts, ", ") + ")"
	case *missingType:
		return "<none>"
	}
	return fmt.Sprintf("%v", v)
}

// maxStrDigits is CPython's default int_max_str_digits: int <-> decimal str conversions above it raise ValueError.
const maxStrDigits = 4300

// pyIntStr is str(int); past maxStrDigits it raises ValueError (a panic the kit-function frames label).
func pyIntStr(x *big.Int) string {
	s := x.String()
	if len(strings.TrimPrefix(s, "-")) > maxStrDigits {
		panic(&crashError{msg: "ValueError: Exceeds the limit (4300 digits) for integer string conversion"})
	}
	return s
}

// kitFrame, deferred in a ported kit function, names that function on a crash raised below it (the innermost kit
// frame of the kit's traceback) and lets the crash go on up.
func kitFrame(name string) {
	if p := recover(); p != nil {
		if ce, ok := p.(*crashError); ok && ce.fn == "" {
			ce.fn = name
		}
		panic(p)
	}
}

func tzRepr(off int) string {
	if off == 0 {
		return "datetime.timezone.utc"
	}
	days, secs := off/86400, off%86400
	if secs < 0 {
		secs += 86400
		days--
	}
	var td string
	switch {
	case days != 0 && secs != 0:
		td = fmt.Sprintf("days=%d, seconds=%d", days, secs)
	case days != 0:
		td = fmt.Sprintf("days=%d", days)
	default:
		td = fmt.Sprintf("seconds=%d", secs)
	}
	return "datetime.timezone(datetime.timedelta(" + td + "))"
}

// pyFloatRepr is Python repr(float): the shortest round-trip digits, positional for exponents -4..15.
func pyFloatRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64)
	sign := ""
	if e[0] == '-' {
		sign, e = "-", e[1:]
	}
	mant, expText, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expText)
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

func pyStrRepr(s string) string {
	quote := byte('\'')
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r < 0x7f:
			b.WriteRune(r)
		case pyPrintable(r):
			b.WriteRune(r)
		case r <= 0xff:
			fmt.Fprintf(&b, `\x%02x`, r)
		case r <= 0xffff:
			fmt.Fprintf(&b, `\u%04x`, r)
		default:
			fmt.Fprintf(&b, `\U%08x`, r)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// pyPrintable is str.isprintable() for one non-ASCII code point (no Cc/Cf/Cs/Co/Cn/Zl/Zp/Zs).
func pyPrintable(r rune) bool {
	if r == utf8.RuneError {
		return true
	}
	return unicode.IsPrint(r)
}

func pyBytesRepr(s string) string {
	quote := byte('\'')
	if strings.IndexByte(s, '\'') >= 0 && strings.IndexByte(s, '"') < 0 {
		quote = '"'
	}
	var b strings.Builder
	b.WriteString("b")
	b.WriteByte(quote)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == quote || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\t':
			b.WriteString(`\t`)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c < 0x20 || c >= 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte(quote)
	return b.String()
}

// pyLess is Python `a < b` for sort keys; ok is false where Python raises TypeError.
func pyLess(a, b any) (bool, bool) {
	a, b = asKit(a), asKit(b)
	if na, ok := numeric(a); ok {
		nb, ok := numeric(b)
		if !ok {
			return false, false
		}
		c, ok := numCmp(na, nb)
		return ok && c < 0, true
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x < y, ok
	case pyBytes:
		y, ok := b.(pyBytes)
		return ok && x < y, ok
	case *pyDate:
		y, ok := b.(*pyDate)
		if !ok {
			return false, false
		}
		return lessInts([]int{x.y, x.m, x.d}, []int{y.y, y.m, y.d}), true
	case *pyDateTime:
		y, ok := b.(*pyDateTime)
		if !ok || x.aware != y.aware {
			return false, false
		}
		if x.aware {
			return x.utcMicros() < y.utcMicros(), true
		}
		return lessInts([]int{x.y, x.mo, x.d, x.h, x.mi, x.s, x.us}, []int{y.y, y.mo, y.d, y.h, y.mi, y.s, y.us}), true
	}
	return false, false
}

func lessInts(a, b []int) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// sortedByStr is Python sorted(xs, key=str): a stable sort on str(x).
func sortedByStr(xs []any) []any {
	out := append([]any(nil), xs...)
	keys := make([]string, len(out))
	for i, x := range out {
		keys[i] = pyStr(x)
	}
	idx := make([]int, len(out))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool { return keys[idx[i]] < keys[idx[j]] })
	res := make([]any, len(out))
	for i, k := range idx {
		res[i] = out[k]
	}
	return res
}

func strListRepr(xs []string) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = pyStrRepr(x)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
