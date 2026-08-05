---
page_id: op-ingestion
summary: Run bounded ingestion and backfills while protecting provider budgets, idempotency, and product freshness.
content_type: task-guide
owner: platform-operations
applicability: current
lifecycle: active
---

# Ingestion and backfill operations

1. Define provider, organization, repositories or projects, record families, and time range.
2. Estimate units, provider budget, queue capacity, and completion window.
3. Start a bounded slice first.
4. Monitor dispatch, running, retrying, failed, and completed units.
5. Verify idempotent writes and watermarks.
6. Expand only after the bounded slice advances product coverage correctly.
7. Record any residual gap or replay requirement.

Avoid repeatedly starting overlapping backfills for the same scope and period.

## Incremental windows

An incremental run plans one window per source and dataset. The window starts at
that dataset's stored watermark, less the configured watermark overlap. A
dataset with no watermark yet cold-starts at the configured initial sync depth,
capped by the organization's plan backfill limit. The window ends at the
requested upper bound, or at the current time when none was requested. A
successful unit stamps the watermark at its own window end — never at the
current time — so a partial window advances coverage only as far as it actually
read.

### Heavy-dataset window ratchet

Unit cost grows with window span. For heavy record families — commit statistics,
file records, blame, and test results — a single window covering a wide initial
sync depth can cost more than the provider budget allows for one unit. Such a
unit is deferred, stamps no watermark, and every later attempt recomputes the
same unaffordable span, so the dataset never starts.

Incremental windows for heavy record families are therefore capped in span. The
window still starts at the watermark or cold-start depth, but ends no later than
the start plus the cap. `SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS` sets the cap;
the default is seven days, matching the proven backfill chunk size.

The cap must exceed the configured watermark overlap. The window starts at the
watermark less the overlap, so a cap at or below the overlap would end the window
at or before the watermark it started from; because watermark writes only ever
move forward, that write is discarded and the family re-reads the same slice on
every run. When `SYNC_WATERMARK_OVERLAP` is greater than or equal to the cap, the
planner widens the cap to exceed the overlap and records a warning naming both
values. Treat that warning as a configuration defect to correct, not as a healthy
steady state — the widened window is more expensive than the one requested.

Consequences to expect when reading run state:

- A capped run is a healthy partial run, not a failure. Its window end is behind
  the current time by design.
- Each successful tick stamps the watermark at its window end, so the next
  scheduled tick resumes exactly there. Coverage ratchets forward one capped
  window per tick with no gap and no overlap beyond the configured watermark
  overlap.
- A ninety-day cold start at the default cap reaches the current time after
  roughly thirteen ticks. On an hourly schedule that is about half a day of
  catch-up.
- A capped run finalizes as an ordinary successful run. Today there is no
  distinct run state, badge, or freshness signal that says "caught up to here
  only", so success alone does not mean the family has reached the current time.
  The advancing watermark is the observable evidence of progress: read the
  dataset's latest successful source timestamp across consecutive runs rather
  than inferring coverage from run status. Surfacing this lag directly is
  tracked separately as CHAOS-3430.
- Catch-up is paced by the sync schedule. A heavy dataset that is far behind
  does not accelerate; to close a wide historical span faster, run a bounded
  backfill for that period instead.
- Light and medium record families are unaffected and keep a single
  full-depth incremental window.
- A run requested with an upper bound already covered by the dataset's watermark
  plans no unit for that dataset. If that leaves the whole run with no units, the
  run finalizes as failed with "No sync units planned". Read that as "the
  requested window was already covered", not as an ingestion fault. Scheduled
  runs do not request an upper bound and so never reach this state; only a
  manually requested past upper bound can.

A stored watermark ahead of the current time is treated as corrupt. It can be
produced by a provider record carrying a skewed timestamp, or by a version of the
planner that predates these rules. Because watermark writes otherwise only move
forward, such a value cannot be corrected by an ordinary run and would stop the
dataset permanently: no unit would be planned, and the run would report "No sync
units planned" on every attempt. The planner therefore plans a short recovery
window instead, records a warning naming the stored value and the recovery
window, and the next successful run replaces the corrupt watermark with a valid
one. Records between the true last synchronized point and the recovery window are
not re-fetched — run a bounded backfill if that span matters.

These window rules — the end never in the future, and no unit for an already
covered range — apply to full resynchronization as well as incremental
synchronization. Both stamp the watermark at the window end on success, so both
carry the same consequences. Bounded backfills are separate: they never stamp a
watermark and legitimately target a historical range, so they keep their own
chunking and bounds.

Full resynchronization of a heavy record family is deliberately **not** capped.
The cap exists so repeated incremental runs can make progress; a one-shot full
resynchronization has no next run to continue from, so a capped window would
cover only the cap's span and then report success — claiming a completed resync
that did not happen. A full resync of a heavy family over a wide depth is
therefore expected to be expensive, and where it exceeds the provider budget it
terminalizes visibly and names the remedy rather than silently under-reading. To
rebuild deep history for a heavy family, use bounded backfills for the period
instead of a full resynchronization.
