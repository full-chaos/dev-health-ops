// Package querybound keeps a ClickHouse statement that carries a list of ids
// under the server's statement size limit.
//
// WHY A BOUND EXISTS: clickhouse-go does not send an array argument as a bound
// parameter; bindPositional renders it INTO the statement text, so a
// `has(?, <ids>)` or `IN ?` statement grows with the number of ids. ClickHouse
// refuses text longer than max_query_size (default 262144 bytes) with code 62
// "Max query size exceeded" before the statement starts. The org-wide
// work-item attribution partition crossed that size on 2026-09-27 and failed
// every day after (prod read 2026-10-02: code 62, org-wide partition only).
//
// The rule lives here, and not in each reader, so that every reader that
// binds a list of ids splits it the same way: internal/jobs/metrics/remaining
// (work-item attribution) and internal/jobs/metrics/workitemblockers
// (CHAOS-8493).
package querybound

import "strings"

// MaxArrayBytes caps the RENDERED size of one array literal in a statement.
// 64 KiB per array keeps a statement with two arrays at about 128 KiB plus the
// fixed query text, half of the server limit.
const MaxArrayBytes = 64 * 1024

var clickhouseStringQuote = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// RenderedStringLen is the length of one element as clickhouse-go renders it
// (clickhouse-go v2.48.0 bind.go format(): quotes around the value, `\` and `'`
// each doubled by a backslash).
func RenderedStringLen(s string) int {
	return len(clickhouseStringQuote.Replace(s)) + 2
}

// ChunkStringsByRenderedBytes splits items into consecutive chunks whose
// rendered array literal (`[` + elements joined by ", " + `]`) is at most
// maxBytes. Order is preserved and every item lands in exactly one chunk, so a
// statement run once per chunk and the results unioned reads the same rows as
// one statement over all items would, for a "row matches any id in the list"
// read.
//
// An item that alone exceeds maxBytes gets a chunk of its own: dropping it
// would silently lose a row, and no real id is that long.
// Empty input returns nil.
func ChunkStringsByRenderedBytes(items []string, maxBytes int) [][]string {
	if len(items) == 0 {
		return nil
	}
	var chunks [][]string
	start, size := 0, 2 // "[]"
	for i, item := range items {
		add := RenderedStringLen(item)
		if i > start {
			add += 2 // ", "
		}
		if i > start && size+add > maxBytes {
			chunks = append(chunks, items[start:i])
			start, size = i, 2
			add = RenderedStringLen(item)
		}
		size += add
	}
	return append(chunks, items[start:])
}
