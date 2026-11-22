package workitemmetrics

// StoredTransitionColumnsSQL is the select list of a work_item_transitions read that drops stale board rows: the
// event's columns, its tie_ordinal, and same_raw_slots, the (from_status, to_status) of each board slot row
// (tie_ordinal >= 2^31) with a known to_status stored at the same instant of the item with the same raw value. A slot
// row whose value no status map knows (a custom board column, to_status 'unknown') is no evidence: it gives no status
// for that raw value, so a tie_ordinal 0 row with the same raw value beside it is kept.
const StoredTransitionColumnsSQL = `org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw,
		to_status_raw, actor, last_synced, tie_ordinal,
		groupArrayIf((from_status, to_status), tie_ordinal >= 2147483648 AND to_status != 'unknown')
			OVER (PARTITION BY org_id, work_item_id, occurred_at, to_status_raw) AS same_raw_slots`

// StaleBoardTieZeroSQL is true for a Projects v2 board Status event stored before migration 170 (tie_ordinal 0) whose
// board now writes the same event (same instant, same raw value) at its slot with a different normalized status, as
// after a status-map change: the board never re-writes the 0 row, so it would be a stale second transition. The 0 row
// must have the board event's shape (from "unknown", no from raw, no actor). A row the positive evidence does not
// cover is kept: a GitHub label transition has the same shape, and only the same raw value marks the 0 row as the
// board's own event.
const StaleBoardTieZeroSQL = `(tie_ordinal = 0 AND from_status = 'unknown' AND from_status_raw = '' AND actor = ''
		AND arrayExists(slot -> slot != (from_status, to_status), same_raw_slots))`
