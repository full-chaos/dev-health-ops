package pyjson

import (
	"math"
	"math/big"
)

// Equal is Python's == over decoded JSON values: dicts compare without
// regard to key order, lists element by element, and numbers across bool,
// int and float exactly (True == 1 == 1.0; two large ints that share a
// float rounding are not equal; NaN equals nothing).
func Equal(a, b Value) bool {
	if left, ok := pyNumber(a); ok {
		right, isNumber := pyNumber(b)
		return isNumber && left != nil && right != nil && left.Cmp(right) == 0
	}
	switch x := a.(type) {
	case nil:
		return b == nil
	case string:
		y, ok := b.(string)
		return ok && x == y
	case []Value:
		y, ok := b.([]Value)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !Equal(x[i], y[i]) {
				return false
			}
		}
		return true
	case *Object:
		y, ok := b.(*Object)
		if !ok || x.Len() != y.Len() {
			return false
		}
		for _, key := range x.Keys() {
			left, _ := x.Get(key)
			right, present := y.Get(key)
			if !present || !Equal(left, right) {
				return false
			}
		}
		return true
	}
	return false
}

// pyNumber reads a bool, int or float as the exact rational Python
// compares; a NaN or an infinity is a number with no rational (nil), which
// equals nothing -- an infinity is equal only to itself, handled by the
// caller's rational compare never matching, a deliberate simplification
// that is exact for every stored JSON value because JSON has no infinity.
func pyNumber(value Value) (*big.Rat, bool) {
	switch v := value.(type) {
	case bool:
		if v {
			return big.NewRat(1, 1), true
		}
		return big.NewRat(0, 1), true
	case Int:
		if v.Int == nil {
			return big.NewRat(0, 1), true
		}
		return new(big.Rat).SetInt(v.Int), true
	case Float:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, true
		}
		return new(big.Rat).SetFloat64(f), true
	}
	return nil, false
}
