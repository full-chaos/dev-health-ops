# CHAOS-4381 — Canonical parity rules for the proof-gate comparator

Status: ACCEPTED (chris 2026-08-27 19:44 PT, CHAOS-4381 Done). Author:
lane-4366 (CHAOS-4366 Wave 0).
Parent: `go-api-epic.md` §5 (five-stage proof gate), §7 open decision 3.

This document is the CONTENT of decision 3 (process was already accepted:
parity rules are a process gate). It fixes what "match" is allowed to mean
before any operation reaches stage 2 (local dual-run proof). The comparator
(CHAOS-4366 deliverable 5) is implemented against sections 1-6 below with
the planted-defect controls from §6; no `pending sign-off` flag is carried
in its PR.

## 1. Error ordering

GraphQL does not guarantee resolver execution order across sibling root
fields (they may run concurrently), so the `errors` array's *positional*
order is not part of the observable contract.

**Rule:** compare `errors` as a **set keyed by `(path, extensions.code)`**,
not by array position.
- Sort both sides' `errors` by `(path joined with ".", extensions.code,
  message)` before diffing.
- `path` must match exactly (including list indices).
- `extensions.code` must match exactly.
- `message` text is compared but a message-only mismatch with identical
  `path`+`code` is reported as a distinct, lower-severity finding
  (`error_message_drift`), not a `mismatch` terminal state — message
  strings are not a versioned contract, codes and paths are.
- A missing error on one side, or an extra error on either side, IS a
  `mismatch`.

## 2. Null vs. omission

GraphQL distinguishes `"field": null` (key present, value null) from the
key being absent. These are NOT interchangeable, and a nullable field that
errors resolves to `null` in `data` per the GraphQL spec's error-handling
section, while an unselected field is simply absent.

**Rule:** compare JSON structurally with **exact key-presence semantics**.
- A field present-and-null on one side but absent on the other is a
  `mismatch`.
- Do not normalize `{}` vs `{"x": null}` before comparing.
- Exception: top-level transport envelope keys that one framework omits
  when empty (e.g. an empty `extensions` object) may be allowlisted per
  key, in the comparator's exclusion list, with a written reason — same
  discipline as `oraclecompare` exclusions (ops/AGENTS.md "Compute-port
  parity").

## 3. Floating-point comparison

Aggregation order can differ between ClickHouse's own execution plan
(unchanged) and Go vs Python client-side rounding/formatting, so bit-exact
float equality is the wrong bar for computed metrics, but the wrong bar in
the other direction hides real bugs (a dropped term, a unit error).

**Rule:** two-tier comparison by field provenance, declared per-field in the
operation's registry entry (default = tier A):
- **Tier A — pass-through values** (raw stored counts/ids/timestamps that
  neither side computes, e.g. `count(*)`, an ID, a boolean): **exact**
  equality. No tolerance.
- **Tier B — computed/aggregated floats** (ratios, percentages, scores,
  anything involving division, weighted sums, or LLM-adjacent scoring):
  relative tolerance `1e-9` OR absolute tolerance `1e-9`, whichever is
  looser, i.e. `abs(a-b) <= max(1e-9, 1e-9 * max(abs(a), abs(b)))`.
- A field not explicitly declared Tier B is Tier A by default — an operation
  owner must opt a field into tolerance, tolerance is never the silent
  default, mirroring "an inaccurate coverage claim is worse than an admitted
  gap" (root AGENTS.md).
- `NaN`/`Infinity` on either side is always a `mismatch`, never
  tolerance-compared.

## 4. Concurrent ClickHouse watermark handling

ClickHouse is eventually consistent under concurrent merges/inserts; Python
and Go issuing the "same" query microseconds apart can legitimately observe
different row counts if data landed between the two reads. That is not a
Go-vs-Python defect and must not be reported as one.

**Rule:** every comparable operation must expose a **watermark** — the
`max(updated_at)` (or the table's canonical high-water column) actually
observed by each side's query, captured alongside the response.
- Stage 2 (dual-run) and stage 4 (shadow) run against a scratch/seeded
  store or the same real-traffic read, and both capture their own
  watermark.
- If watermarks differ, the comparator emits terminal state `unsupported`
  (per plan §5's state list) with the watermark delta recorded — **never**
  `mismatch`. This is why `PROOF_RUN.data_watermark` exists in the ledger
  (plan §8.3): a `match` verdict is only meaningful when both sides are
  pinned to the same watermark, or the delta is provably a subset (Go's
  watermark ⊇ Python's watermark and the extra rows are excluded from the
  diffed set).
- An operation that cannot be pinned to a stable watermark within N retries
  (comparator config, default 3) surfaces `unsupported` and is excluded
  from that stage's pass/fail count — it does not silently count as a pass.

## 5. List tie-ordering

Where a query's `ORDER BY` has no fully unique tiebreaker, Python and Go/CH
can return a different, both-valid order for rows tied on the sort key.
Comparator-side "solving" nondeterminism (e.g. by sorting both sides before
comparing) would hide operations that need a real fix and would also mask
genuine pagination/cursor-stability bugs.

**Rule:**
- **Default: list order is part of the contract and IS compared
  position-by-position.** A mismatch in tied-row order is a real finding.
- An operation may be registered with `tie_ordering: relaxed` in its
  registry entry ONLY when the operation owner attaches evidence that the
  `ORDER BY`/sort key lacks a unique tiebreaker AND a ticket to add one
  (e.g. append `id ASC`) — `relaxed` is a documented, time-boxed exception,
  not a default. Under `relaxed`, the comparator groups rows into tie-blocks
  by the declared sort key and compares each tie-block as a set (by primary
  id), never the whole list as a set.
- `reviewEdges` (plan §6, wave-1 continuation item 2, "after making
  tie-ordering deterministic") is the known first case; it should ship with
  a real tiebreaker, not a `relaxed` registration, before its own canary.

## 6. Comparator falsification (planted-defect controls)

Per plan §5 stage 2, the comparator itself is validated with table-driven
planted-defect tests before it is trusted for any operation:
1. **Removed row** — candidate drops one row from a list result → must be
   `mismatch`.
2. **Changed nullability** — candidate returns `null` where baseline returns
   a value (or omits vs. includes per §2 above) → `mismatch`.
3. **Changed error path** — candidate's error `path` differs by one segment
   → `mismatch` (never silently absorbed into "same code, different path").
4. **Reordered results** — under default (non-`relaxed`) ordering, a
   candidate that returns the same rows in a different order → `mismatch`.
   Under a `relaxed` tie-ordering fixture, an in-tie-block reorder must
   still `match`, and a cross-tie-block reorder must still `mismatch` — both
   directions are asserted so `relaxed` cannot degrade into "list order
   doesn't matter."
5. Each control also asserts the comparator emits the *specific* terminal
   state named above, not just a boolean pass/fail — an
   `unsupported`-labeled watermark drift and a real `mismatch` must never be
   collapsible into the same signal an operator reads.

## Sign-off

Accepted by chris, 2026-08-27 19:44 PT (CHAOS-4381 Done, ruling comment on
the ticket). The comparator implementation (`src/dev_health_ops/api/graphql/
go_api_comparator.py`) applies sections 1-6 above verbatim.
