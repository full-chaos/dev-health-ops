// Package chtime binds a Go time to a ClickHouse DateTime64 comparison without losing the boundary second.
//
// A positional `?` bound with a time.Time reaches ClickHouse as toDateTime('<seconds>'), a DateTime. A strict `<` or `>` of
// that constant against a DateTime64 column in a primary key, partition key or skip index prunes the granules of the
// boundary second wrongly. Bind the instant as Int64 microseconds and rebuild it in SQL:
//
//	occurred_at < fromUnixTimestamp64Micro(?, 'UTC')    // argument: chtime.Micros(end)
package chtime

import "time"

// Micros is the Unix time of t in microseconds, the argument of fromUnixTimestamp64Micro.
func Micros(t time.Time) int64 {
	return t.UnixMicro()
}
