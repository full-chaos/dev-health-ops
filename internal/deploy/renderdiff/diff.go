package renderdiff

import (
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"
)

// The kit's redaction regexes (full-render-diff.py:48-50). Python's `$` also matches before a final newline, and
// Python's `\s` on str is every Unicode whitespace code point, so both are spelled out for RE2.
var (
	secretyRe  = pyIgnoreCase{regexp.MustCompile(`(password|passwd|secret|token|private|dsn|credential|_uri\n?$|key\n?$)`)}
	cmKeyRe    = pyIgnoreCase{regexp.MustCompile(`(password|secret|token|key|dsn|credential)`)}
	userinfoRe = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\t\n\v\f\r\x1c-\x1f \x{85}\x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}/]*@`)
)

// pyIgnoreCase is Python's re.IGNORECASE on a str for a pattern of ASCII lower-case letters: a code point matches a
// letter when its simple lower-case mapping is that letter (U+0130 and U+212A too), or when it is one of the two
// extra equivalences of sre_compile (U+0131 for i, U+017F for s). RE2's (?i) uses case folding, which misses U+0130.
type pyIgnoreCase struct{ re *regexp.Regexp }

func (p pyIgnoreCase) MatchString(s string) bool {
	return p.re.MatchString(strings.Map(func(r rune) rune {
		switch r {
		case 0x131:
			return 'i'
		case 0x17f:
			return 's'
		}
		return unicode.ToLower(r)
	}, s))
}

const (
	redacted    = "<redacted>"
	postgresSTS = "StatefulSet/dev-health-ops-postgresql"
)

// redact is the kit's redact(o): a fresh copy with every secret-like value replaced, applied on load.
func redact(o any) any { return redactAt(o, 1) }

// redactDepth bounds redact's recursion as the kit's Python stack does (one frame per level): a structure that holds
// itself (a mapping alias inside its own anchor) dies there in a RecursionError, never in an unbounded Go recursion.
const redactDepth = 990

func redactAt(o any, depth int) any {
	defer kitFrame("redact")
	if depth >= redactDepth {
		panic(&crashError{msg: "RecursionError: maximum recursion depth exceeded"})
	}
	switch x := o.(type) {
	case *pyDict:
		d := x.copy()
		if name, ok := d.getS("name"); ok {
			if s, isStr := name.(string); isStr && d.has("value") && secretyRe.MatchString(s) {
				raw, _ := d.getS("value")
				_ = d.set("value", &redactedValue{raw: raw})
			}
		}
		if kind, ok := d.getS("kind"); ok && pyEq(kind, "ConfigMap") {
			if _, isStr := kind.(string); isStr {
				for _, sec := range []string{"data", "binaryData"} {
					if m, ok := d.getS(sec); ok {
						if md, isDict := m.(*pyDict); isDict {
							nd := newDict()
							for i, k := range md.keys {
								v := md.vals[i]
								if cmKeyRe.MatchString(pyStr(k)) {
									v = &redactedValue{raw: v}
								}
								_ = nd.set(k, v)
							}
							_ = d.set(sec, nd)
						}
					}
				}
			}
		}
		for _, argk := range []string{"args", "command"} {
			if l, ok := d.getS(argk); ok {
				if list, isList := l.([]any); isList {
					nl := make([]any, len(list))
					for i, e := range list {
						if s, isStr := e.(string); isStr && strings.Contains(s, "://") && strings.Contains(s, "@") {
							nl[i] = &redactedValue{raw: e}
						} else {
							nl[i] = e
						}
					}
					_ = d.set(argk, nl)
				}
			}
		}
		out := newDict()
		for i, k := range d.keys {
			_ = out.set(k, redactAt(d.vals[i], depth+1))
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = redactAt(e, depth+1)
		}
		return out
	case string:
		if userinfoRe.MatchString(x) {
			return &redactedValue{raw: x}
		}
	}
	return o
}

func (d *pyDict) getS(k string) (any, bool) { return d.get(k) }

// redactedValue is a value redact() hides. Everywhere the kit reads it, it is the str "<redacted>" (it prints,
// dumps, sorts, hashes and compares as that str). The raw value stays in memory for one boolean only: whether a
// redacted value changed between A and B (a CHANGED row with no value and no hash of it). raw is never formatted.
type redactedValue struct{ raw any }

// asKit is the value the kit holds: a redactedValue is the str "<redacted>".
func asKit(v any) any {
	if _, ok := v.(*redactedValue); ok {
		return redacted
	}
	return v
}

// hiddenChange is true when both sides hold a redacted value and the raw values differ (same() on the raws).
func hiddenChange(a, b any) bool {
	ra, okA := a.(*redactedValue)
	rb, okB := b.(*redactedValue)
	return okA && okB && !same(ra.raw, rb.raw)
}

// docKind is d.get("kind") for a doc that docs_of() keeps: a mapping with a truthy kind.
func docKind(d any) (any, bool) {
	m, ok := d.(*pyDict)
	if !ok {
		return nil, false
	}
	k, ok := m.getS("kind")
	if !ok || !truthy(k) {
		return nil, false
	}
	return k, true
}

func docsOf(all []any) []*pyDict {
	var out []*pyDict
	for _, d := range all {
		if _, ok := docKind(d); ok {
			out = append(out, d.(*pyDict))
		}
	}
	return out
}

// docName is (d.get("metadata") or {}).get("name").
func docName(d *pyDict) (any, error) {
	md, ok := d.getS("metadata")
	if !ok || !truthy(md) {
		return nil, nil
	}
	m, isDict := md.(*pyDict)
	if !isDict {
		return nil, &crashError{msg: fmt.Sprintf("AttributeError: '%s' object has no attribute 'get'", pyTypeName(md))}
	}
	v, _ := m.getS("name")
	return v, nil
}

// key is the kit's key(d): "<kind>/<name>".
func key(d *pyDict) (string, error) {
	defer kitFrame("key")
	kind, _ := d.getS("kind")
	name, err := docName(d)
	if err != nil {
		return "", err
	}
	return pyStr(kind) + "/" + pyStr(name), nil
}

type leaf struct {
	path string
	val  any
}

// flat is the kit's flat(o, pre): the leaves, an empty container counting as one.
func flat(o any, pre string, out *[]leaf) {
	defer kitFrame("flat")
	switch x := o.(type) {
	case *pyDict:
		if x.Len() > 0 {
			for _, k := range sortedByStr(x.keys) {
				v, _ := x.get(k)
				flat(v, pre+"."+pyStr(k), out)
			}
			return
		}
	case []any:
		if len(x) > 0 {
			for i, v := range x {
				flat(v, fmt.Sprintf("%s[%d]", pre, i), out)
			}
			return
		}
	}
	*out = append(*out, leaf{pre, o})
}

// same is the kit's same(x, y): equal WITH the type.
func same(x, y any) bool {
	return pyTypeName(x) == pyTypeName(y) && pyEq(x, y)
}

// form is the kit's form(v): "missing", "null", "list", "map" or "".
func form(v any) string {
	switch x := v.(type) {
	case *missingType:
		return "missing"
	case nil:
		return "null"
	case []any:
		if len(x) == 0 {
			return "list"
		}
	case *pyDict:
		if x.Len() == 0 {
			return "map"
		}
	}
	return ""
}

func isInt(v any) bool {
	switch v.(type) {
	case bool, *big.Int:
		return true
	}
	return false
}

func pctD(v any) string {
	switch x := v.(type) {
	case bool:
		if x {
			return "1"
		}
		return "0"
	case *big.Int:
		return pyIntStr(x)
	}
	return pyStr(v)
}

// listKeys is the kit's list_keys(lst): ("name"|"port", keys) or ok=false.
func listKeys(lst []any) (string, []string, bool) {
	defer kitFrame("list_keys")
	if len(lst) == 0 {
		return "", nil, false
	}
	byName := true
	for _, e := range lst {
		d, isDict := e.(*pyDict)
		if !isDict {
			byName = false
			break
		}
		n, _ := d.getS("name")
		if s, isStr := asKit(n).(string); !isStr || s == "" {
			byName = false
			break
		}
	}
	if byName {
		keys := make([]string, len(lst))
		for i, e := range lst {
			n, _ := e.(*pyDict).getS("name")
			keys[i] = asKit(n).(string)
		}
		return "name", keys, true
	}
	for _, e := range lst {
		d, isDict := e.(*pyDict)
		if !isDict || !isInt(portOf(d)) {
			return "", nil, false
		}
	}
	keys := make([]string, len(lst))
	for i, e := range lst {
		d := e.(*pyDict)
		proto, _ := d.getS("protocol")
		ps := "TCP"
		if truthy(proto) {
			ps = pyStr(proto)
		}
		keys[i] = pctD(portOf(d)) + "/" + ps
	}
	return "port", keys, true
}

// portOf is e.get("containerPort", e.get("port")).
func portOf(d *pyDict) any {
	if v, ok := d.getS("containerPort"); ok {
		return v
	}
	v, _ := d.getS("port")
	return v
}

type row struct {
	doc, path, verb string
	old, new        []string
	before, after   any
	hidden          bool // a changed redacted value: printed as "CHANGED <redacted>"
}

type differ struct {
	doc  string
	rows []row
}

func (df *differ) row(path, verb string, old, new []string, before, after any) {
	df.rows = append(df.rows, row{doc: df.doc, path: path, verb: verb, old: old, new: new, before: before, after: after})
}

func dictGet(d *pyDict, k any) any {
	if v, ok := d.get(k); ok {
		return v
	}
	return missing
}

func unionKeys(a, b *pyDict) []any {
	seen := map[string]bool{}
	var all []any
	for _, d := range []*pyDict{a, b} {
		for _, k := range d.keys {
			h, _ := hashKey(k)
			if h == "" || !seen[h] {
				seen[h] = true
				all = append(all, k)
			}
		}
	}
	return sortedByStr(all)
}

func (df *differ) walk(a, b any, path string) {
	defer kitFrame("walk")
	fa, fb := form(a), form(b)
	if fa != "" && fb != "" {
		if fa != fb {
			df.row(path, "CHANGED", nil, nil, a, b)
		}
		return
	}
	da, aDict := a.(*pyDict)
	db, bDict := b.(*pyDict)
	la, aList := a.([]any)
	lb, bList := b.([]any)
	switch {
	case aDict && bDict:
		for _, k := range unionKeys(da, db) {
			p := path + "." + pyStr(k)
			va, vb := dictGet(da, k), dictGet(db, k)
			fa, fb := form(va), form(vb)
			if fa != "" && fb != "" {
				if fa != fb {
					df.row(p, "CHANGED", nil, nil, va, vb)
				}
				continue
			}
			lvb, vbList := vb.([]any)
			lva, vaList := va.([]any)
			switch {
			case (fa == "missing" || fa == "null" || fa == "list") && vbList && hasKeys(lvb):
				df.walk([]any{}, vb, p)
			case (fb == "missing" || fb == "null" || fb == "list") && vaList && hasKeys(lva):
				df.walk(va, []any{}, p)
			case !db.has(k):
				df.row(p, "ELEMENT-REMOVED", nil, nil, va, missing)
			case !da.has(k):
				df.leaves(va, vb, p)
			default:
				df.walk(va, vb, p)
			}
		}
	case aList && bList:
		kaKind, kaKeys, kaOK := listKeys(la)
		kbKind, kbKeys, kbOK := listKeys(lb)
		if len(la) == 0 && kbOK {
			kaKind, kaKeys, kaOK = kbKind, []string{}, true
		}
		if len(lb) == 0 && kaOK {
			kbKind, kbKeys, kbOK = kaKind, []string{}, true
		}
		if kaOK && kbOK && kaKind == kbKind {
			df.keyed(la, lb, kaKind, kaKeys, kbKeys, path)
			return
		}
		n := len(la)
		if len(lb) > n {
			n = len(lb)
		}
		for i := 0; i < n; i++ {
			p := fmt.Sprintf("%s[%d]", path, i)
			if i >= len(la) || i >= len(lb) {
				var ea, eb any = missing, missing
				if i < len(la) {
					ea = la[i]
				}
				if i < len(lb) {
					eb = lb[i]
				}
				df.leaves(ea, eb, p)
			} else {
				df.walk(la[i], lb[i], p)
			}
		}
	default:
		df.leaves(a, b, path)
	}
}

func hasKeys(l []any) bool {
	_, _, ok := listKeys(l)
	return ok
}

func leafMap(v any, path string) (map[string]any, bool) {
	if v == any(missing) {
		return map[string]any{}, true
	}
	switch v.(type) {
	case *pyDict, []any:
		var ls []leaf
		flat(v, path, &ls)
		m := map[string]any{}
		for _, l := range ls {
			m[l.path] = l.val
		}
		return m, true
	}
	return map[string]any{path: v}, true
}

func (df *differ) leaves(a, b any, path string) {
	defer kitFrame("leaves")
	fa, _ := leafMap(a, path)
	fb, _ := leafMap(b, path)
	var paths []string
	for p := range fa {
		paths = append(paths, p)
	}
	for p := range fb {
		if _, ok := fa[p]; !ok {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		va, inA := fa[p]
		vb, inB := fb[p]
		if !inA {
			va = missing
		}
		if !inB {
			vb = missing
		}
		switch {
		case !same(va, vb) || inA != inB:
			df.row(p, "CHANGED", nil, nil, va, vb)
		case hiddenChange(va, vb):
			df.rows = append(df.rows, row{doc: df.doc, path: p, verb: "CHANGED", before: va, after: vb, hidden: true})
		}
	}
}

func (df *differ) keyed(a, b []any, kind string, namesA, namesB []string, path string) {
	defer kitFrame("keyed")
	ia := map[string]any{}
	for i, n := range namesA {
		ia[n] = a[i]
	}
	ib := map[string]any{}
	for i, n := range namesB {
		ib[n] = b[i]
	}
	var commonA, commonB []string
	for _, n := range namesA {
		if _, ok := ib[n]; ok {
			commonA = append(commonA, n)
		}
	}
	for _, n := range namesB {
		if _, ok := ia[n]; ok {
			commonB = append(commonB, n)
		}
	}
	if !slices.Equal(commonA, commonB) {
		df.row(path, "REORDERED", append([]string{}, namesA...), append([]string{}, namesB...), nil, nil)
	}
	set := map[string]bool{}
	var all []string
	for _, n := range append(append([]string{}, namesA...), namesB...) {
		if !set[n] {
			set[n] = true
			all = append(all, n)
		}
	}
	sort.Strings(all)
	for _, n := range all {
		p := fmt.Sprintf("%s[%s=%s]", path, kind, n)
		_, inA := ia[n]
		_, inB := ib[n]
		switch {
		case !inA:
			df.row(p, "ELEMENT-ADDED", nil, nil, nil, nil)
		case !inB:
			df.row(p, "ELEMENT-REMOVED", nil, nil, nil, nil)
		default:
			df.walk(ia[n], ib[n], p)
		}
	}
}

func findDups(o any, doc, path, label string, out *[]string) {
	defer kitFrame("find_dups")
	switch x := o.(type) {
	case *pyDict:
		for _, k := range sortedByStr(x.keys) {
			v, _ := x.get(k)
			findDups(v, doc, path+"."+pyStr(k), label, out)
		}
	case []any:
		if _, keys, ok := listKeys(x); ok {
			seen := map[string]bool{}
			for _, n := range keys {
				if seen[n] {
					*out = append(*out, fmt.Sprintf("DUPLICATE name %s in %s %s (render %s)", n, doc, path, label))
				}
				seen[n] = true
			}
		}
		for i, e := range x {
			findDups(e, doc, fmt.Sprintf("%s[%d]", path, i), label, out)
		}
	}
}
