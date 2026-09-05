package icfinalize

// FloatValue mirrors compute_ic.py's _float_value, which coerces one loader
// cell to a float.
//
// The Python is:
//
//	if isinstance(value, bool):        return 0.0
//	if isinstance(value, int | float): return float(value)
//	if isinstance(value, str):
//	    try:    return float(value)
//	    except ValueError: return 0.0
//	return 0.0
//
// The bool branch is FIRST and returns 0.0, and it has to be: bool subclasses
// int in Python, so without it `True` would fall into the int arm and become
// 1.0. Go has no such subtyping, so a port that simply switches on the
// dynamic type would silently diverge the moment a loader yields a bool --
// there is no compiler error and no test failure unless a bool is in the
// corpus.
//
// STRING BRANCH: deliberately NOT implemented yet. Python `float()` is not
// Go's strconv.ParseFloat -- the brief records them as differing in BOTH
// directions -- and no Python-float() primitive exists in
// internal/pythonparity (checked against its whole exported surface: Sum,
// Round, Repr, FormatFixed, ParseUUID, DecodeClickHouseString, the JSON
// marshalers, IsSpace/Strip/LStrip/RStrip/SplitWhitespace/CollapseWhitespace/
// RuneLen/TruncateRunes/SplitLines).
//
// Writing one is only justified if the branch is REACHABLE from the
// ClickHouse loader, which is a measurement, not a reading: the loader's
// SELECT returns typed columns, so a string may never arrive here at all.
// Until that is measured on bigboy with a positive control, a string input
// returns 0.0 and records itself, so the corpus can prove whether the branch
// is ever entered instead of the port guessing. See StringInputSeen.
func FloatValue(value any) float64 {
	switch typed := value.(type) {
	case bool:
		// Not `if typed { return 1 }` -- Python returns 0.0 for BOTH.
		_ = typed
		return 0
	case int:
		return float64(typed)
	case int8:
		return float64(typed)
	case int16:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint:
		return float64(typed)
	case uint8:
		return float64(typed)
	case uint16:
		return float64(typed)
	case uint32:
		return float64(typed)
	case uint64:
		return float64(typed)
	case float32:
		return float64(typed)
	case float64:
		return typed
	case string:
		stringInputs.observe(typed)
		return 0
	default:
		return 0
	}
}
