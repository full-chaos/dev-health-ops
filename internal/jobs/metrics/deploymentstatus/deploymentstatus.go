// Package deploymentstatus holds the one meaning of a deployment's own status
// for every delivery metric (CHAOS-9621): change failure rate, deployment
// failure rate, deployment frequency and the deploy metrics of a day.
//
// Providers spell the same outcome differently (GitHub "failure", GitLab
// "failed"), and a push-ingested row may carry any string. Classify maps every
// stored value to one Class, so no metric keeps a private status list.
package deploymentstatus

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// Class is what a deployment's own status says about its outcome.
type Class int

const (
	// Other is any status that is not a final outcome: in progress, queued,
	// pending, inactive (GitHub: superseded by a newer deployment), blocked
	// (GitLab: waits for approval), an unknown string, NULL or empty. Such a
	// deployment is a deployment of the day, and it is not failed.
	Other Class = iota
	// Succeeded is a deployment that finished well.
	Succeeded
	// Failed is a deployment that finished badly.
	Failed
	// Canceled is a deployment that never reached its environment. It is
	// neither failed nor a change: metrics do not count it at all.
	Canceled
)

// String names the class for logs.
func (c Class) String() string {
	switch c {
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Canceled:
		return "canceled"
	default:
		return "other"
	}
}

// Terminal reports whether the class is a final outcome that counts as
// evidence for a rate: succeeded or failed. Canceled is not an outcome of a
// change, and Other has no outcome yet.
func (c Class) Terminal() bool {
	return c == Succeeded || c == Failed
}

// CanceledStatuses are the normalized spellings Classify reads as Canceled.
// CanceledExclusionSQL binds this same list, so SQL and Go share one set.
var CanceledStatuses = []string{"canceled", "cancelled"}

// Classify maps one stored deployment status to its Class. Case and
// surrounding space do not matter.
func Classify(status string) Class {
	normalized := strings.ToLower(strings.TrimSpace(status))
	if slices.Contains(CanceledStatuses, normalized) {
		return Canceled
	}
	switch normalized {
	case "success":
		return Succeeded
	case "failure", "failed", "error":
		return Failed
	default:
		return Other
	}
}

// CanceledExclusionSQL is the ClickHouse predicate that keeps a row of column
// exactly when Classify would not call it Canceled. The query binds
// CanceledStatuses as {canceled_statuses:Array(String)}.
//
// ClickHouse trimBoth trims only ' ', and lower only ASCII, so the trim is a
// regex over the whitespace strings.TrimSpace removes. Each whitespace rune
// is a literal alternative, not a class member: a class would split a
// multi-byte rune into bytes if the regex engine runs in byte mode.
func CanceledExclusionSQL(column string) string {
	return "lowerUTF8(replaceRegexpAll(coalesce(" + column + ", ''), '" + trimSpacePattern +
		"', '')) NOT IN {canceled_statuses:Array(String)}"
}

var trimSpacePattern = func() string {
	var alternatives []string
	add := func(r rune) {
		if r < 0x80 {
			// Doubled backslash: the SQL string literal unescapes it once.
			alternatives = append(alternatives, fmt.Sprintf(`\\x%02x`, r))
			return
		}
		alternatives = append(alternatives, string(r))
	}
	for _, span := range unicode.White_Space.R16 {
		for r := rune(span.Lo); r <= rune(span.Hi); r += rune(span.Stride) {
			add(r)
		}
	}
	for _, span := range unicode.White_Space.R32 {
		for r := rune(span.Lo); r <= rune(span.Hi); r += rune(span.Stride) {
			add(r)
		}
	}
	space := "(?:" + strings.Join(alternatives, "|") + ")+"
	return "^" + space + "|" + space + "$"
}()
