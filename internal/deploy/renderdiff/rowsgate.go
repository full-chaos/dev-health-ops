package renderdiff

import (
	"fmt"
	"math/big"
	"sort"
)

// pyIter is iter(x) for the kit's sorted()/tuple() calls on rows-file values.
func pyIter(v any) []any {
	switch x := v.(type) {
	case string:
		out := []any{}
		for _, r := range x {
			out = append(out, string(r))
		}
		return out
	case []any:
		return x
	case pyTuple:
		return x
	case *pyDict:
		return x.keys
	case pyBytes:
		out := []any{}
		for i := 0; i < len(x); i++ {
			out = append(out, big.NewInt(int64(x[i])))
		}
		return out
	}
	panic(&crashError{msg: fmt.Sprintf("TypeError: '%s' object is not iterable", pyTypeName(v))})
}

// pySorted is sorted(x) with Python's TypeError for values that do not compare.
func pySorted(v any) []any {
	xs := append([]any(nil), pyIter(v)...)
	for i := 1; i < len(xs); i++ {
		_, ok1 := pyLess(xs[0], xs[i])
		_, ok2 := pyLess(xs[i], xs[0])
		if !ok1 || !ok2 {
			panic(&crashError{msg: fmt.Sprintf("TypeError: '<' not supported between instances of '%s' and '%s'", pyTypeName(xs[i]), pyTypeName(xs[0]))})
		}
	}
	sort.SliceStable(xs, func(i, j int) bool {
		lt, _ := pyLess(xs[i], xs[j])
		return lt
	})
	return xs
}

func strsToAny(xs []string) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}

func sig(doc, path, verb, old, new any) pyTuple {
	t := func(v any) any {
		if v == nil {
			return nil
		}
		return pyTuple(pyIter(v))
	}
	return pyTuple{doc, path, verb, t(old), t(new)}
}

func strsOrNil(xs []string) any {
	if xs == nil {
		return nil
	}
	return strsToAny(xs)
}

// tupleSet is a dict keyed by signature tuples under Python equality.
type tupleSet struct {
	keys []pyTuple
	vals []any
	idx  map[string]int
}

func newTupleSet() *tupleSet { return &tupleSet{idx: map[string]int{}} }

func (s *tupleSet) set(k pyTuple, v any) {
	h, ok := hashKey(k)
	if !ok {
		panic(&crashError{msg: "TypeError: unhashable type in a row signature"})
	}
	if i, found := s.idx[h]; found {
		s.vals[i] = v
		return
	}
	s.idx[h] = len(s.keys)
	s.keys = append(s.keys, k)
	s.vals = append(s.vals, v)
}

func (s *tupleSet) get(k pyTuple) (any, bool) {
	h, _ := hashKey(k)
	i, ok := s.idx[h]
	if !ok {
		return nil, false
	}
	return s.vals[i], true
}

// minus is set(s) - set(o), sorted by str (the order the kit prints).
func (s *tupleSet) minus(o *tupleSet, keep bool) []pyTuple {
	var out []any
	for _, k := range s.keys {
		_, in := o.get(k)
		if in == keep {
			out = append(out, k)
		}
	}
	sorted := sortedByStr(out)
	res := make([]pyTuple, len(sorted))
	for i, k := range sorted {
		res[i] = k.(pyTuple)
	}
	return res
}

func dictField(e any, name string) (any, bool) {
	d, ok := e.(*pyDict)
	if !ok {
		panic(&crashError{msg: fmt.Sprintf("TypeError: '%s' object is not subscriptable by a str", pyTypeName(e))})
	}
	return d.get(name)
}

func mustField(e any, name string) any {
	v, ok := dictField(e, name)
	if !ok {
		panic(&crashError{msg: "KeyError: " + pyStrRepr(name)})
	}
	return v
}

func optField(e any, name string) any {
	v, _ := dictField(e, name)
	return v
}

func (r *run) rowsGate(path, final string, added, removed []string, rows []row, namesOK bool) (rc int) {
	defer func() {
		if p := recover(); p != nil {
			if ce, ok := p.(*crashError); ok {
				r.crash(ce)
			}
			panic(p)
		}
	}()
	raw, err := readFile(path)
	if err != nil {
		r.usage(fmt.Sprintf("cannot read the rows file %s: %s", path, pyOSErrorStr(err, path)))
	}
	text, err := pythonText(raw)
	if err != nil {
		r.usage(fmt.Sprintf("cannot read the rows file %s: %s", path, err.Error()))
	}
	want, err := pyJSONLoads(text)
	if err != nil {
		if _, ok := err.(*pyJSONError); ok {
			r.usage(fmt.Sprintf("cannot read the rows file %s: %s", path, err.Error()))
		}
		r.crash(err)
	}
	wd, isDict := want.(*pyDict)
	if !isDict {
		panic(&crashError{msg: fmt.Sprintf("AttributeError: '%s' object has no attribute 'get'", pyTypeName(want))})
	}
	getOr := func(k string) any {
		if v, ok := wd.get(k); ok {
			return v
		}
		return []any{}
	}
	ok := namesOK
	fileAdded, fileRemoved := pySorted(getOr("added")), pySorted(getOr("removed"))
	sortedAdded := append([]string{}, added...)
	sort.Strings(sortedAdded)
	sortedRemoved := append([]string{}, removed...)
	sort.Strings(sortedRemoved)
	if !pyEq(fileAdded, strsToAny(sortedAdded)) || !pyEq(fileRemoved, strsToAny(sortedRemoved)) {
		r.print("docs added/removed differ from the rows file: added %s removed %s; file added %s removed %s",
			strListRepr(added), strListRepr(removed), pyRepr(fileAdded), pyRepr(fileRemoved))
		ok = false
	}
	observed := newTupleSet()
	for _, rw := range rows {
		observed.set(sig(rw.doc, rw.path, rw.verb, strsOrNil(rw.old), strsOrNil(rw.new)), rw)
	}
	expected := newTupleSet()
	for _, e := range pyIter(getOr("rows")) {
		expected.set(sig(mustField(e, "doc"), mustField(e, "path"), mustField(e, "verb"), optField(e, "old"), optField(e, "new")), e)
	}
	for _, s := range observed.minus(expected, false) {
		r.print("unexpected row %s %s %s", pyStr(s[0]), pyStr(s[1]), pyStr(s[2]))
		ok = false
	}
	for _, s := range expected.minus(observed, false) {
		r.print("missing row %s %s %s", pyStr(s[0]), pyStr(s[1]), pyStr(s[2]))
		ok = false
	}
	for _, s := range expected.minus(observed, true) {
		ev, _ := expected.get(s)
		ov, _ := observed.get(s)
		e, rw := ev.(*pyDict), ov.(row)
		before, after := rw.before, rw.after
		for _, f := range []struct {
			name string
			got  any
		}{{"before", before}, {"after", after}} {
			if want, in := e.get(f.name); in {
				if !(f.got != any(missing) && pyStr(f.got) == pyStr(want)) {
					r.print("value-check FAIL %s %s (%s)", pyStr(s[0]), pyStr(s[1]), f.name)
					ok = false
				}
			}
		}
		if (e.has("before") || e.has("after")) && ok {
			r.print("value-check ok %s %s", pyStr(s[0]), pyStr(s[1]))
		}
		if show, _ := e.get("show"); truthy(show) {
			r.print("value %s %s %s -> %s", pyStr(s[0]), pyStr(s[1]), pyStr(before), pyStr(after))
		}
	}
	word := "HOLD"
	if ok {
		word = "PASS"
	}
	r.print("%s: %s", final, word)
	if ok {
		return 0
	}
	return 1
}
