package daily

import "errors"

// ErrPartialWrite marks a native family executor that FAILED AFTER already
// writing at least one row.
//
// Why this is not just another error (CHAOS-4288, codex r1 on #2235):
//
// The native-family contract is FAIL-OPEN by design -- a family's runtime
// failure is not a partition failure, and the compatibility bridge computes
// that family instead, exactly as it would have before any native executor
// existed. That is correct precisely when NOTHING was written.
//
// It is wrong when something was. A family that writes several tables
// sequentially can succeed on the first and fail on the third. The output
// tables are plain MergeTrees with no version column, so a subsequent bridge
// write does NOT replace the rows already there -- it adds a second copy.
// Fail-open then converts a partial failure into silent duplication, which no
// reader can detect: argMax-style reads still return a sane latest value and
// only the row count grows.
//
// So an executor that has written anything wraps this sentinel. The handler
// adds the family to the skip list -- deliberately the same treatment a SUCCESS
// gets, because in both cases the bridge must not write it -- and records the
// partial_write outcome with the TRUE rows-written count so the run is
// re-driven rather than double-written.
//
// Executors must wrap it only when a write actually landed. Wrapping it for a
// failure BEFORE the first write would suppress the bridge's legitimate
// fallback and lose the family for that partition, which is the opposite
// mistake and just as silent.
var ErrPartialWrite = errors.New("native family wrote rows before failing")
