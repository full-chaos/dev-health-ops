package docstools

import (
	"fmt"
	"math"
	"math/big"
	"strings"
)

// The Python values yaml.safe_load builds, with the two things the validator did to them: truthiness (`x or default`)
// and str(x).

type pyList struct{ items []any }

type pyDict struct {
	keys []any
	vals []any
	idx  map[string]int
}

type pySet struct{ items []any }

type pyTuple struct{ a, b any }

type pyBytes []byte

type pyDate struct{ y, m, d int }

type pyDatetime struct {
	y, mo, d, h, mi, s, us int
	hasTZ                  bool
	offsetSeconds          int
}

func newPyDict() *pyDict { return &pyDict{idx: map[string]int{}} }

// pyKeyID is the identity of a dict key as Python's hash and == see it: bool, int and float compare by numeric value, so
// 1, 1.0 and True are one key; every `.nan` is the one shared float object.
func pyKeyID(k any) string {
	switch v := k.(type) {
	case nil:
		return "N"
	case bool:
		if v {
			return "n1"
		}
		return "n0"
	case *big.Int:
		return "n" + v.String()
	case float64:
		switch {
		case math.IsNaN(v):
			return "fnan"
		case math.IsInf(v, 1):
			return "finf"
		case math.IsInf(v, -1):
			return "f-inf"
		case v == math.Trunc(v):
			bf := new(big.Float).SetFloat64(v)
			bi, _ := bf.Int(nil)
			return "n" + bi.String()
		}
		return "f" + dcPyFloatRepr(v)
	case string:
		return "s" + v
	case pyBytes:
		return "b" + string(v)
	case pyDate:
		return fmt.Sprintf("d%d-%d-%d", v.y, v.m, v.d)
	case pyDatetime:
		if !v.hasTZ {
			return fmt.Sprintf("t%d-%d-%d %d:%d:%d.%d", v.y, v.mo, v.d, v.h, v.mi, v.s, v.us)
		}
		return fmt.Sprintf("T%d", pyDatetimeUTCKey(v))
	}
	return fmt.Sprintf("?%p", k)
}

// pyDatetimeUTCKey orders aware datetimes by instant (microseconds from year 1 day 1 UTC).
func pyDatetimeUTCKey(v pyDatetime) int64 {
	days := int64(pyDaysFromCivil(v.y, v.mo, v.d))
	return ((days*24+int64(v.h))*60+int64(v.mi))*60*1000000 + int64(v.s)*1000000 + int64(v.us) - int64(v.offsetSeconds)*1000000
}

func pyDaysFromCivil(y, m, d int) int {
	if m <= 2 {
		y--
	}
	era := y / 400
	if y < 0 {
		era = (y - 399) / 400
	}
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

func (d *pyDict) set(k, v any) {
	id := pyKeyID(k)
	if i, ok := d.idx[id]; ok {
		d.vals[i] = v
		return
	}
	d.idx[id] = len(d.keys)
	d.keys = append(d.keys, k)
	d.vals = append(d.vals, v)
}

func (d *pyDict) get(k any) (any, bool) {
	if i, ok := d.idx[pyKeyID(k)]; ok {
		return d.vals[i], true
	}
	return nil, false
}

func (d *pyDict) getStr(k string) (any, bool) { return d.get(k) }

func pyTruthy(v any) bool {
	switch x := v.(type) {
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
		return len(x) > 0
	case *pyList:
		return len(x.items) > 0
	case *pyDict:
		return len(x.keys) > 0
	case *pySet:
		return len(x.items) > 0
	}
	return true
}

func pyTimedeltaRepr(seconds int) string {
	days := seconds / 86400
	rem := seconds % 86400
	if rem < 0 {
		rem += 86400
		days--
	}
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

func (d pyDatetime) isoformat() string {
	s := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", d.y, d.mo, d.d, d.h, d.mi, d.s)
	if d.us != 0 {
		s += fmt.Sprintf(".%06d", d.us)
	}
	if d.hasTZ {
		off := d.offsetSeconds
		sign := "+"
		if off < 0 {
			sign, off = "-", -off
		}
		s += fmt.Sprintf("%s%02d:%02d", sign, off/3600, off%3600/60)
		if off%60 != 0 {
			s += fmt.Sprintf(":%02d", off%60)
		}
	}
	return s
}

func pyBytesRepr(b pyBytes) string {
	q := byte('\'')
	if strings.IndexByte(string(b), '\'') >= 0 && strings.IndexByte(string(b), '"') < 0 {
		q = '"'
	}
	var sb strings.Builder
	sb.WriteString("b")
	sb.WriteByte(q)
	for _, c := range b {
		switch {
		case c == q || c == '\\':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case c == '\t':
			sb.WriteString(`\t`)
		case c == '\n':
			sb.WriteString(`\n`)
		case c == '\r':
			sb.WriteString(`\r`)
		case c < 0x20 || c >= 0x7f:
			fmt.Fprintf(&sb, `\x%02x`, c)
		default:
			sb.WriteByte(c)
		}
	}
	sb.WriteByte(q)
	return sb.String()
}

// pyRepr is repr(v). A set with two or more members is refused: Python prints it in hash order, which for strings
// changes from run to run (STRICTER).
func pyRepr(v any, seen []any) (string, error) {
	for _, s := range seen {
		if s == v {
			switch v.(type) {
			case *pyList:
				return "[...]", nil
			case *pyDict:
				return "{...}", nil
			}
		}
	}
	if len(seen) > 600 {
		return "", &dcYErr{"RecursionError", "maximum recursion depth exceeded"}
	}
	switch x := v.(type) {
	case nil:
		return "None", nil
	case bool:
		if x {
			return "True", nil
		}
		return "False", nil
	case *big.Int:
		return pyIntStr(x)
	case float64:
		return dcPyFloatRepr(x), nil
	case string:
		return a1Repr(x), nil
	case pyBytes:
		return pyBytesRepr(x), nil
	case pyDate:
		return fmt.Sprintf("datetime.date(%d, %d, %d)", x.y, x.m, x.d), nil
	case pyDatetime:
		args := []string{fmt.Sprint(x.y), fmt.Sprint(x.mo), fmt.Sprint(x.d), fmt.Sprint(x.h), fmt.Sprint(x.mi)}
		if x.s != 0 || x.us != 0 {
			args = append(args, fmt.Sprint(x.s))
		}
		if x.us != 0 {
			args = append(args, fmt.Sprint(x.us))
		}
		if x.hasTZ {
			if x.offsetSeconds == 0 {
				args = append(args, "tzinfo=datetime.timezone.utc")
			} else {
				args = append(args, "tzinfo=datetime.timezone("+pyTimedeltaRepr(x.offsetSeconds)+")")
			}
		}
		return "datetime.datetime(" + strings.Join(args, ", ") + ")", nil
	case *pyList:
		parts := make([]string, len(x.items))
		for i, it := range x.items {
			s, err := pyRepr(it, append(seen, v))
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	case pyTuple:
		a, err := pyRepr(x.a, append(seen, v))
		if err != nil {
			return "", err
		}
		b, err := pyRepr(x.b, append(seen, v))
		if err != nil {
			return "", err
		}
		return "(" + a + ", " + b + ")", nil
	case *pyDict:
		parts := make([]string, len(x.keys))
		for i := range x.keys {
			k, err := pyRepr(x.keys[i], append(seen, v))
			if err != nil {
				return "", err
			}
			val, err := pyRepr(x.vals[i], append(seen, v))
			if err != nil {
				return "", err
			}
			parts[i] = k + ": " + val
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	case *pySet:
		switch len(x.items) {
		case 0:
			return "set()", nil
		case 1:
			s, err := pyRepr(x.items[0], append(seen, v))
			if err != nil {
				return "", err
			}
			return "{" + s + "}", nil
		}
		return "", &dcYErr{"ValueError", "the text of a set with two or more members is refused (STRICTER: Python prints it in hash order)"}
	}
	return "", fmt.Errorf("pyRepr: unknown value %T", v)
}

func pyIntStr(x *big.Int) (string, error) {
	s := x.String()
	digits := len(s)
	if x.Sign() < 0 {
		digits--
	}
	if digits > 4300 {
		return "", &dcYErr{"ValueError", "Exceeds the limit (4300 digits) for integer string conversion; use sys.set_int_max_str_digits() to increase the limit"}
	}
	return s, nil
}

// pyStr is str(v).
func pyStr(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case pyDate:
		return fmt.Sprintf("%04d-%02d-%02d", x.y, x.m, x.d), nil
	case pyDatetime:
		return x.isoformat(), nil
	}
	return pyRepr(v, nil)
}
