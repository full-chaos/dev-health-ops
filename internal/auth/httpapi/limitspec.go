package httpapi

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// limitExpr is one rate limit in the `limits` library's string notation
// (limits.util.SINGLE_EXPR, compiled VERBOSE|IGNORECASE): "3/hour",
// "10/15minutes", "5 per day". Go's \s is ASCII whitespace where Python's is
// Unicode; a value that differs only there is refused here, never read
// differently.
var limitExpr = regexp.MustCompile(`(?i)^\s*([0-9]+)\s*(?:/|\s*per\s*)\s*([0-9]+)?\s*([a-z]+)\s*$`)

var limitGranularities = map[string]time.Duration{
	"second": time.Second,
	"minute": time.Minute,
	"hour":   time.Hour,
	"day":    24 * time.Hour,
	"month":  30 * 24 * time.Hour,
	"year":   12 * 30 * 24 * time.Hour,
}

// ParseLimit reads text as limits.parse_many does for a single limit and
// returns it under id. It refuses what Python refuses, and also refuses
// what this api cannot honour the same way: more than one limit ("1/second;
// 5/minute"), a count of zero, and a value that overflows. A limit set from
// the environment is read at startup, so a refused value stops the process
// rather than serving with a different limit.
func ParseLimit(id, text string) (Limit, error) {
	if strings.ContainsAny(text, ",;|") {
		return Limit{}, fmt.Errorf("rate limit %q: only a single limit is supported", text)
	}
	match := limitExpr.FindStringSubmatch(text)
	if match == nil {
		return Limit{}, fmt.Errorf("couldn't parse rate limit string %q", text)
	}
	count, err := strconv.Atoi(match[1])
	if err != nil || count <= 0 {
		return Limit{}, fmt.Errorf("rate limit %q: the count must be a positive integer", text)
	}
	// `multiples and int(multiples) or None`: absent or zero means one unit.
	multiples := int64(1)
	if match[2] != "" {
		parsed, err := strconv.ParseInt(match[2], 10, 32)
		if err != nil {
			return Limit{}, fmt.Errorf("rate limit %q: the window multiple is out of range", text)
		}
		if parsed > 0 {
			multiples = parsed
		}
	}
	// check_granularity_string: the lowered name, or its plural.
	unit, ok := limitGranularities[strings.TrimSuffix(strings.ToLower(match[3]), "s")]
	if !ok {
		return Limit{}, fmt.Errorf("no granularity matched for %s", match[3])
	}
	window := time.Duration(multiples) * unit
	if window/unit != time.Duration(multiples) {
		return Limit{}, fmt.Errorf("rate limit %q: the window is out of range", text)
	}
	return Limit{ID: id, Count: count, Window: window}, nil
}
