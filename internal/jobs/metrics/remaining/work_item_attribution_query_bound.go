package remaining

import "github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"

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
var workItemAttributionMaxArrayBytes = querybound.MaxArrayBytes

// renderedStringLen and chunkStringsByRenderedBytes are the shared rule of
// internal/jobs/metrics/querybound: one implementation for every reader that
// binds a list of ids (CHAOS-8493 added a second reader).
func renderedStringLen(s string) int { return querybound.RenderedStringLen(s) }

func chunkStringsByRenderedBytes(items []string, maxBytes int) [][]string {
	return querybound.ChunkStringsByRenderedBytes(items, maxBytes)
}
