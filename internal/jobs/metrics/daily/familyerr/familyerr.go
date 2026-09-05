// Package familyerr holds error sentinels shared between the daily-metrics
// handler and the per-family packages it drives.
//
// It exists ONLY to break an import cycle, and it is deliberately tiny.
// `internal/jobs/metrics/daily` imports every family subpackage (the native
// executors live in `daily` and call into `daily/benchmarking`,
// `daily/compoundingrisk`, and so on), so a family package cannot import
// `daily` back. But a family's WRITER is exactly where a partial write is
// detected, and it needs to say so in the error it returns.
//
// So the sentinel lives here, at a leaf both sides can import, and `daily`
// re-exports it as `daily.ErrPartialWrite` -- the name the handler checks and
// the name other lanes are already building against.
package familyerr

import "errors"

// ErrPartialWrite marks a native family executor that FAILED AFTER already
// writing at least one row.
//
// The native-family contract is fail-open by design: a runtime failure is not a
// partition failure, and the compatibility bridge computes that family instead.
// That is correct precisely when NOTHING was written.
//
// It is wrong when something was. A family writing several tables sequentially
// can succeed on the first and fail on the third. The outputs are plain
// MergeTrees with no version column, so a subsequent bridge write does not
// replace the rows already there -- it adds a second copy. Fail-open then turns
// a partial failure into silent duplication that no reader can detect:
// argMax-style reads still return a sane latest value and only the row count
// grows.
//
// Wrap this ONLY when a write actually landed. Wrapping it for a failure BEFORE
// the first write suppresses the bridge's legitimate fallback and loses the
// family for that partition -- the opposite mistake, and just as silent.
var ErrPartialWrite = errors.New("native family wrote rows before failing")
