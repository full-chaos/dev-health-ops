package remaining

import "strings"

// workItemAttributionMaxArrayBytes caps the RENDERED size of one array literal
// in a work-item attribution statement.
//
// WHY A CAP EXISTS: clickhouse-go does not send an array argument as a bound
// parameter; bindPositional renders it INTO the statement text, so a
// `has(?, <ids>)` statement grows with the number of ids. ClickHouse refuses
// text longer than max_query_size (default 262144 bytes) with code 62 "Max
// query size exceeded" before the statement starts. The org-wide attribution
// partition crossed that size on 2026-09-27 and failed every day after (prod
// read 2026-10-02: code 62, nul=0, org-wide partition only).
//
// 64 KiB per array keeps the worst statement (three arrays, in
// loadAffectedSubjects, all small in practice; at most two here: donor ids +
// keys) at about 128 KiB plus the fixed query text, half of the server limit.
// It is a variable only so the integration equivalence test can force many
// chunks with a tiny cap; nothing in production assigns it.
var workItemAttributionMaxArrayBytes = 64 * 1024

var clickhouseStringQuote = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// renderedStringLen is the length of one element as clickhouse-go renders it
// (clickhouse-go v2.47.0 bind.go format(): quotes around the value, `\` and `'`
// each doubled by a backslash).
func renderedStringLen(s string) int {
	return len(clickhouseStringQuote.Replace(s)) + 2
}

// chunkStringsByRenderedBytes splits items into consecutive chunks whose
// rendered array literal (`[` + elements joined by ", " + `]`) is at most
// maxBytes. Order is preserved and every item lands in exactly one chunk, so a
// statement run once per chunk and the results unioned reads the same rows as
// one statement over all items would (the sites below are all "row matches any
// id in the list" reads: each row is matched by exactly one id).
//
// An item that alone exceeds maxBytes gets a chunk of its own: dropping it
// would silently lose a row, and no real id is that long.
// Empty input returns nil.
func chunkStringsByRenderedBytes(items []string, maxBytes int) [][]string {
	if len(items) == 0 {
		return nil
	}
	var chunks [][]string
	start, size := 0, 2 // "[]"
	for i, item := range items {
		add := renderedStringLen(item)
		if i > start {
			add += 2 // ", "
		}
		if i > start && size+add > maxBytes {
			chunks = append(chunks, items[start:i])
			start, size = i, 2
			add = renderedStringLen(item)
		}
		size += add
	}
	return append(chunks, items[start:])
}
