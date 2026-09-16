package pythonparity

// OrZero ports Python's `value or 0.0` idiom.
//
// Python's `bool(float)` is `value != 0.0`, which is False (falsy) for BOTH
// +0.0 and -0.0 -- not only for a missing/None value. `x or 0.0` therefore
// silently normalizes a genuine -0.0 to +0.0, the same as it does for a
// nil-turned-zero, while NaN and +-Inf are all truthy (`bool(nan)` is True;
// NaN's own `!=` is defined True by IEEE754, unlike its `<`/`>`/`==`) and pass
// through unchanged. A naive Go `if ptr != nil { v = *ptr }` only replaces a
// missing (nil) value -- it leaves a genuine -0.0 reading unchanged, which is
// a divergence from Python's silent sign-normalization wherever the source
// value can carry a negative zero.
func OrZero(value float64) float64 {
	if value == 0 {
		return 0.0
	}
	return value
}
