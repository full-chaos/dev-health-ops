package workitemmetrics

// StoredTransitionColumnsSQL is the select list of a work_item_transitions read that drops stale board rows: the
// event's columns, its tie_ordinal and instant_tie, the largest tie_ordinal stored at the same instant of the item.
const StoredTransitionColumnsSQL = `org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw,
		to_status_raw, actor, last_synced, tie_ordinal,
		max(tie_ordinal) OVER (PARTITION BY org_id, work_item_id, occurred_at) AS instant_tie`

// StaleBoardTieZeroSQL is true for a Projects v2 board Status event stored before migration 170 (tie_ordinal 0) at an
// instant that also holds a board slot row (tie_ordinal >= 2^31). The board re-writes that event at its slot on every
// run and never re-writes the 0 row, so after a status-map change or a raw rename the 0 row is a stale second
// transition. The 0 row must have the board event's shape (from "unknown", no from raw, no actor); a GitHub timeline
// transition of that shape keeps its row by its raw value (closed, reopened, merged, unlabeled:*).
const StaleBoardTieZeroSQL = `(tie_ordinal = 0 AND instant_tie >= 2147483648
		AND from_status = 'unknown' AND from_status_raw = '' AND actor = ''
		AND to_status_raw NOT IN ('closed', 'reopened', 'merged') AND NOT startsWith(to_status_raw, 'unlabeled:'))`
