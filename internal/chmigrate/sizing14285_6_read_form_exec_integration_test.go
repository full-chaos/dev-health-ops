//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of the prod read form SIZING-14285-6 (CHAOS-14285): not for merge. sizing14285s6Form is the form
// file with its org prefix (8 hex) replaced by the synthetic "f0f0f0f0" in all 14 places; its sha256 is pinned.
const sizing14285s6FormSHA256 = "aa05332cb4cb89bf93597a610ab02005a03f44c99bd9e174f59ab041e9120278"

const sizing14285s6Form = `-- SIZING-14285-6 = SIZING-14285-5 (13cebc9f7b6b) cut to stmt0 + stmt6 + stmt7, with a per-statement derived cap. Lane
--   l14285s (lead fcdev), 2026-11-24. Why: the -5 prod run (pm-guard/s14285/SIZING-14285-5.out) STOPPED after stmt5 on a
--   read_rows WANT miss (50647 > 46915); stmt6 and stmt7 were NOT RUN. acp ruling for -6 (binding): only stmt0 (ORG-1) +
--   stmt6 + stmt7; text unchanged except the caps. Prod: NOT EXECUTED. md: SIZING-14285-6.md. CI exec: EXEC-PROOF-14285-6.md.
-- The -5 comment block below is kept byte for byte as history. Where it says S0, stmt1..stmt5, M1 (budget gate), M3
--   (read_rows WANT) or the 4 GiB bytes arm, D1-D4 here replace it.
--   D1 Cap derived INSIDE each statement from live system.parts of the tables that statement reads (no S0, no pm math):
--      cap = ceil(1.25 x (sum(marks x k) x 8192 + P)) = intDiv(5 x (sum(marks x k) x 8192 + P) + 3, 4).
--      marks = granules (+ 1 final mark per part: an upper bound). 8192 = index_granularity of each of the 7 tables
--      (baseline/head.json :251 :346 :351 :361 :366 :531 :696 "SETTINGS index_granularity = 8192"): a granule holds at
--      most 8192 rows, so marks x 8192 >= rows of a part. k = scans of that table in the statement (as -5). P = part count.
--      1.25 = 25 % headroom.
--   D2 read_rows_cap = f x cap + (1 + s) x (2 x P + 1). f = 2 if the statement reads FINAL (a FINAL scan can read a part
--      twice), else 1. s = scalar subqueries that use cost_gate (stmt0 7, stmt6 3, stmt7 0): each scope can run the gate
--      once (P + 1 metadata rows each, <= 2 x P + 1). stmt7 (s 0): 2 x cap + 2 x P + 1.
--   D3 Gate = scalar subquery cost_gate: throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE'). So the statement runs only
--      if read_rows_cap < 20M strictly (the same 20M hard bound). It reads system.parts and system.one only.
--   D4 The 4 GiB bytes_on_disk arm of -5 is dropped: the gate is the derived rows cap only (a CI fixture can fire it; the
--      prod tables read here are < 40 MB on disk, -5 S0). Each statement outputs read_rows_cap: pm WANT read_rows <= it.
-- Run rule: stmt0, stmt6, stmt7 once each, in order, one statement per call. A SQL error (STOP-COST-GATE = code 395
--   included) = STOP. A timeout (30 s) = STOP, no re-run. read_rows > read_rows_cap of that statement = STOP, acp reads.
-- SIZING-14285-5 = SIZING-14285-4 (0e668c288a69) with a later old-rule cut. FORM-TEMPLATE-3 (pm-guard, 6b9e73a3c45a) shape
--   (a) + pass-path budget. ClickHouse, SELECT only. Lane l13902-cutfix (lead fcsup), 2026-11-23. Item 3 (CHAOS-14285) of
--   CARD-13902-recompute-3 (30f972dfa124) P6. Chain: SIZING-14285-2 406b0f9dd2d7 -> -3 0b545d0c495b -> -4 0e668c288a69
--   (CI exec, acp READ-FORM-14285-4 429a7a26eed0: FORM NOT OK, P2-1 cut) -> this file. -3 and -4 are SUPERSEDED: do not run.
-- Prod: NOT EXECUTED. CI exec: EXEC-PROOF-14285-5.md. md: SIZING-14285-5.md.
-- Edits vs -4 (acp READ-FORM-14285-4):
--   C1 (P2-1) Cut '2026-11-23 03:08:08' -> '2026-11-23 05:10:00' UTC in stmt1, stmt2, stmt3. Why: the STEP 323 drain-0 read
--      at 03:08:08Z (G3-read-s323-25-1.sh ef39b1593a28:11) skipped Terminating pods. The first new pod started 02:58:22Z
--      (RECORD-323 8ff7c60b985f); an old-image go-heavy pod (grace 7260 s) can write until 02:58:22Z + 7260 s = 04:59:22Z.
--      05:10:00 = that + margin: no old-image pod writes at or after it. A cut too early hid old-pod rows with c >= cut and
--      ls >= c in no count; a later cut cannot make a wrong count (a new-rule row before the cut matches, never doubled).
--   C2 (P2-1) stmt2 and stmt3 new INFO after_raw_changed (c >= cut, ls >= c) and after_doubled_raw_changed (same AND
--      sl > rl; > 0 = acp reads): the after side has no hidden class now.
--   C3 (P3-3) Cite pin wording: the live workers pin is 340c3e05 (STEP 323). Cites are read at a7febdf50002; acp checked
--      git diff a7febdf50002 340c3e05 over every cited path = only testops files: the same lines hold at 340c3e05.
--   No other SQL change: gates, caps, k weights, scans and every other column are the -4 text.
-- Edits of -4 vs -3 (FORM-TEMPLATE-3, kept):
--   M1 Budget gate: each statement's rows cap is the PASS-PATH BUDGET 20M rows read. The gate sums the system.parts rows
--      of each table it reads times k = the scans of that table in the statement (stmt5 git_pull_requests 2, stmt6
--      git_commit_stats 2 + git_commits 2, stmt7 git_commit_stats 2, else 1): sum(rows x k) over 20M or sum(bytes on disk
--      x k) over 4 GiB = STOP-COST-GATE. All orgs (an upper bound of the org reads). The gates read ONLY system.parts and
--      system.one. No CTE: the only WITH is the scalar gate; it reads metadata only, at each scope that uses it.
--   M2 stmt6: the 3 git_commits FINAL scalar subqueries (count, min day, max day) are one derived table cf, read once:
--      git_commits scans 4 -> 2. Same outputs, same names.
--   M3 read_rows WANT: read_rows <= 1.1 x G + (1 + s) x (2 x P + 1), G = sum of rows x k (S0), P = S0 parts of the gate
--      tables, s = scalar subqueries that read a table (stmt0 7, stmt6 3, else 0). CI asserts the exact scan count:
--      G + 2 x P + 1 <= query_log read_rows <= G + (1 + s) x (2 x P + 1).
-- Edits of -3 vs -2 (kept):
--   L1 Engine cost gate in EVERY statement (now the M1 budget gate), cost_gate = 0 in the outer WHERE and in every scalar
--      subquery that reads a table.
--   L2 New S0, gated on system.one (part count of its 7 tables over 100000 = STOP-COST-GATE): one row per table read here.
--   L3 No placeholder: the old-rule cut placeholder of -2 (ROLL_END_14285_UTC) is a literal, now '2026-11-23 05:10:00' UTC
--      (C1). ops 340c3e05 (STEP 323) holds ceee575af360 (merge-base --is-ancestor rc 0); the COMBINED-8 build 5aa4b332 does
--      not (rc 1). A row computed before the cut can be old rule (raw read) or new rule (an earlier fixed build matches: it
--      lands in match, never in doubled). A row at or after it is new rule (C1). Check: an after-cut row that does not match
--      the recompute = after_mismatch > 0: STOP, acp reads; an after-cut row with raw synced after it = after_raw_changed
--      (INFO), after_doubled_raw_changed (acp reads).
--   L4 Cites read with "git show a7febdf50002:<path>"; the live STEP 323 workers pin is 340c3e05 and the cited lines are the
--      same there (C3). "git diff 84d6c394e23f a7febdf50002" is empty for repouser/, repo_user_commit_native_executor.go,
--      icfinalize/executor.go, teamkeytables/teamkeytables.go, providersync/github_commit_stats_route.go, baseline/head.json.
--   L5 stmt3: new INFO doubled_commit_first_day / doubled_commit_last_day (range input of the decision rule).
-- Run rule: each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP. A timeout (30 s) = STOP, no re-run.
-- read_rows WANT: M3. Work math per statement: md.
-- -2 notes (kept; paths at a7febdf50002, same lines at 340c3e05: C3):
--   F1 stmt0: prs_orgs / reviews_orgs 0 = INFO (stmt5 arm empty: no_raw can only grow), not a STOP. > 1 = STOP.
--   F2/F3 every derived table has an alias; every outer name is alias.<col>.
--   F4 stmt3 groups by the full commit_metrics key (repo_id, commit_hash, day, author_email): head.json:251 ORDER BY
--      (org_id, repo_id, day, author_email, commit_hash). INFO commit_rhd_keys < stored_commits = one commit stored under
--      2+ identity keys.
--   F5 doubled_raw_changed (stmt2, stmt3) > 0 = acp reads before a "no rerun" ruling.
--   F6 stmt7 OLD RULE vs NEW RULE on the same raw rows NOW. Old rule = the raw join of
--      ceee575af360^:internal/jobs/metrics/daily/repouser/clickhouse.go:87-89 ("FROM git_commits AS c LEFT JOIN
--      git_commit_stats AS s"). oldrule_under_commits = 0 is a hard control (raw rows hold every FINAL row, LOC terms >= 0).
--   F8 minIf/maxIf of a Date with no matching row = 1970-01-01 (= none). trimBoth trims ' ' only; Go strings.TrimSpace
--      (repouser/identity.go:35-43) trims all Unicode space: stmt4 INFO only. last_synced = the writer's normalizedAt
--      (providersync/github_commit_stats_route.go:185), set before the INSERT: a row with ls < c inserted after the
--      compute read is possible in a short gap (mostly under). computed_at = executor.nowUTC() after the reads
--      (repo_user_commit_native_executor.go:99-104 vs :164-165): same gap. Retire rows: marker column "retracted"
--      (teamkeytables/teamkeytables.go:380 repo, :295 user), version = snapshot (executor :99, :248-249).
--   F9 keys_checked / commits_checked / after_checked = 0 = NOT EXERCISED (not a "no rerun"): acp reads.
-- Run window: STEP 323 workers (340c3e05, holds 9d88395188e6 + ceee575af360) live, and OUTSIDE any recompute of these
--   tables (card daily-start run, partition-recompute). The normal nightly run is allowed: its keys are after-cut keys or
--   are left out by ls >= c. Schema: head.json 'create' lines :251 :346 :351 :361 :366 :531 :696; migrations 145-170 touch
--   only repo_metrics_daily here (161:34-36 pooled_*, not read; 164:16 retracted, read).
-- Defect: before ceee575af360 (#6672) commitStatsQuery read git_commits / git_commit_stats raw (both ReplacingMergeTree(
--   last_synced), head.json:346, :351). A commit with 2 unmerged versions joined every stat row twice; a stat row with 2
--   versions counted twice. The job STORED that LOC. Now: "git_commits AS c FINAL" / "git_commit_stats AS s FINAL"
--   (repouser/clickhouse.go:77-92).
-- Why a compare: the old unmerged state is not stored and merges removed most duplicates. So: newest stored row vs the same
--   value recomputed NOW by the code rule. stored > recomputed = the doubled case (this defect). stored < recomputed = raw
--   arrived after the compute (INFO, not this defect).
-- Code rule (recompute), repouser/ at a7febdf50002 (same lines at 340c3e05: C3):
--   R1 day = UTC day of git_commits.committer_when, [00:00, +24h) (clickhouse.go:90; compute.go:23-26; executor
--      repo_user_commit_native_executor.go:94-95). committer_when is DateTime64(3, 'UTC') (head.json:351).
--   R2 one aggregate per (repo_id, hash) (compute.go:213-228). LOC of a commit = sum over its stat rows of max(0, additions)
--      + max(0, deletions) (compute.go:229-230, :139). No stat row = 0 (LEFT JOIN clickhouse.go:88; derefInt32 :131-132).
--   R3 repo_metrics_daily.total_loc_touched = sum of the LOC of the day's commits (compute.go:249-254, :515-521; writer
--      clickhouse.go:538-541). commits_count = distinct commits of the day (compute.go:252, :520): NOT doubled = control
--      cell count_mismatch.
--   R4 commit_metrics.total_loc = LOC of the commit (compute.go:654-663; writer clickhouse.go:736-737). Size bucket
--      literals 'small' (<= 50) / 'medium' (<= 300) / 'large' = CommitSizeBucket compute.go:12-20.
--   R5 user_metrics_daily.loc_added + loc_deleted = LOC of the identity's commits of the day (compute.go:249-254, :464-467);
--      identity = lower(trim(author_email)), else trim(author_name), else the literal 'unknown' (identity.go:35-43;
--      executor :166).
-- Not checked (derived): avg_commit_size_loc, large_commit_ratio, large_commits_count, size_bucket (INFO bucket_move),
--   rework_churn_ratio_30d, single_owner_file_ratio_30d, bus_factor, code_ownership_gini (30-day window: executor :96,
--   :104). A commit doubled at the day's compute makes total_loc_touched of THAT day mismatch; a 30-day value made wrong by
--   a duplicate that existed only at a LATER day's compute is not found (CARD ADDENDUM-2 R2).
-- Exclusion: a key with a raw commit or stat row synced at/after the stored compute (ls >= c) is left out of every check
--   and counted as INFO raw_changed_after (before the cut) or after_raw_changed (at/after the cut, C2).
-- argMax: every stored value read here is NOT Nullable (UInt32 / UInt8 / LowCardinality(String)): plain argMax(v,
--   computed_at) = the newest row's value. computed_at is DateTime('UTC'), second precision (clickhouse.go:413-419).
-- LEFT JOIN miss: every test goes through ifNull first: no dependence on join_use_nulls (no SETTINGS).

-- stmt0 ORG-1 over every table read. WANT 1 in each column; > 1 = STOP. F1: prs_orgs / reviews_orgs 0 = INFO; 0 in any
--   other column = STOP. Gate tables: all 7.
-- CAP (D1-D3): k 1 each, f 1, s 7: read_rows_cap = cap + 8 x (2 x P + 1). Output read_rows_cap (pm WANT read_rows <= it).
WITH (SELECT 1 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 8 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'user_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT uniqExact(org_id) FROM git_commits WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS commits_orgs,
  (SELECT uniqExact(org_id) FROM git_commit_stats WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS stats_orgs,
  (SELECT uniqExact(org_id) FROM git_pull_requests WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS prs_orgs,
  (SELECT uniqExact(org_id) FROM git_pull_request_reviews WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS reviews_orgs,
  (SELECT uniqExact(org_id) FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS repo_orgs,
  (SELECT uniqExact(org_id) FROM user_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS user_orgs,
  (SELECT uniqExact(org_id) FROM commit_metrics WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS commit_orgs,
  read_rows_cap;

-- stmt6 RAW SPAN and unmerged versions NOW. Gate tables: commits + stats, each read twice (raw + FINAL; M2).
-- WANT commit_rows_final > 0; stat_rows_final > 0 (the compare sees data).
-- WANT raw_first_day <= stmt1 first_day (else old stored days lost their raw rows: see stmt2 no_commit_now, stmt5 no_raw).
-- INFO commit_versions_unmerged, stat_versions_unmerged: > 0 = the duplicate state still occurs today.
-- CAP (D1-D3): k 2 each, f 2 (FINAL), s 3: read_rows_cap = 2 x cap + 4 x (2 x P + 1). Output read_rows_cap.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'git_commit_stats', 2, table = 'git_commits', 2, 1)) * 8192 + count()) + 3, 4) + 4 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT count() FROM git_commits WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS commit_rows_raw,
  cf.f_rows AS commit_rows_final,
  (SELECT count() FROM git_commit_stats WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS stat_rows_raw,
  (SELECT count() FROM git_commit_stats FINAL WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS stat_rows_final,
  commit_rows_raw - commit_rows_final AS commit_versions_unmerged,
  stat_rows_raw - stat_rows_final AS stat_versions_unmerged,
  cf.f_first AS raw_first_day, cf.f_last AS raw_last_day,
  read_rows_cap
FROM (SELECT count() AS f_rows, min(toDate(committer_when)) AS f_first, max(toDate(committer_when)) AS f_last
      FROM git_commits FINAL WHERE startsWith(org_id, 'f0f0f0f0')) AS cf
WHERE cost_gate = 0;

-- stmt7 OLD RULE vs NEW RULE NOW (F6), per (repo_id, hash) of every raw commit of the org, no day filter.
--   old = the pre-fix join: raw git_commits x raw git_commit_stats, LOC as compute.go:229-230. new = git_commit_stats FINAL
--   (one version per (org, repo, hash, file_path), head.json:346). Gate tables: commits + stats (stats read twice).
-- WANT commits_now > 0; oldrule_same_commits > 0; oldrule_under_commits = 0 (old >= new always: > 0 = the SQL is wrong,
--   STOP, acp reads). INFO oldrule_over_commits (> 0 = the doubling is live today), oldrule_ratio2_commits.
-- CAP (D1-D3): k git_commit_stats 2, git_commits 1, f 2 (FINAL), s 0: read_rows_cap = 2 x cap + 2 x P + 1. Output it.
WITH (SELECT 2 * intDiv(5 * (sum(marks * multiIf(table = 'git_commit_stats', 2, 1)) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT count() AS commits_now,
  countIf(o.o_old = o.o_new) AS oldrule_same_commits,
  countIf(o.o_old > o.o_new) AS oldrule_over_commits,
  countIf(o.o_new > 0 AND o.o_old = 2 * o.o_new) AS oldrule_ratio2_commits,
  countIf(o.o_old < o.o_new) AS oldrule_under_commits,
  read_rows_cap
FROM (
  SELECT oc.k_repo AS o_repo, oc.k_hash AS o_hash, oc.old_loc AS o_old, ifNull(nc.new_loc, 0) AS o_new
  FROM (
    SELECT c.c_repo AS k_repo, c.c_hash AS k_hash,
      sum(greatest(0, ifNull(s.s_add, 0))) + sum(greatest(0, ifNull(s.s_del, 0))) AS old_loc
    FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash FROM git_commits
          WHERE startsWith(org_id, 'f0f0f0f0')) AS c
    LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash, additions AS s_add, deletions AS s_del
               FROM git_commit_stats WHERE startsWith(org_id, 'f0f0f0f0')) AS s
      ON s.s_org = c.c_org AND s.s_repo = c.c_repo AND s.s_hash = c.c_hash
    GROUP BY c.c_repo, c.c_hash
  ) AS oc
  LEFT JOIN (
    SELECT repo_id AS n_repo, commit_hash AS n_hash, sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS new_loc
    FROM git_commit_stats FINAL WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, commit_hash
  ) AS nc ON nc.n_repo = oc.k_repo AND nc.n_hash = oc.k_hash
) AS o
WHERE cost_gate = 0;
`

const sizing14285s6Fixture = `
SYSTEM STOP MERGES git_commits;
SYSTEM STOP MERGES git_commit_stats;
SYSTEM STOP MERGES git_pull_requests;
SYSTEM STOP MERGES git_pull_request_reviews;
SYSTEM STOP MERGES repo_metrics_daily;
SYSTEM STOP MERGES user_metrics_daily;
SYSTEM STOP MERGES commit_metrics;
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'Dev One', 'Dev1@Example.com ', '2026-11-10 10:00:00', '2026-11-10 12:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', 'Dev Two', 'dev2@example.com', '2026-11-11 09:00:00', '2026-11-11 10:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-03', ' Dev Three ', NULL, '2026-11-11 15:00:00', '2026-11-11 16:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', 'Dev One', 'dev1@example.com', '2026-11-12 10:00:00', '2026-11-12 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', 'Dev Two', 'dev2@example.com', '2026-11-13 10:00:00', '2026-11-13 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', 'Dev Three', 'dev3@example.com', '2026-11-14 10:00:00', '2026-11-14 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', 'Dev Four', 'dev4@example.com', '2026-11-22 10:00:00', '2026-11-22 11:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-09', 'Dev Other', 'other@example.com', '2026-11-16 10:00:00', '2026-11-16 11:00:00');
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'Dev One', 'Dev1@Example.com ', '2026-11-10 10:00:00', '2026-11-10 13:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', 'Dev Four', 'dev4@example.com', '2026-11-22 10:00:00', '2026-11-22 12:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'f1.go', 10, 5, '2026-11-10 12:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'f2.go', 20, 0, '2026-11-10 12:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', 'f1.go', 3, 2, '2026-11-11 10:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', 'f1.go', 100, 0, '2026-11-12 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', 'f1.go', 6, 4, '2026-11-13 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', 'f2.go', -3, 0, '2026-11-13 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', 'f1.go', 200, 0, '2026-11-14 11:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', 'f1.go', 8, 2, '2026-11-22 11:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'f1.go', 500, 500, '2026-11-10 12:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', 'f1.go', 100, 0, '2026-11-14 00:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', 'f1.go', 200, 0, '2026-11-14 12:00:00');
INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, merged_at, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 1, '2026-11-15 09:00:00', NULL, '2026-11-15 10:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 2, '2026-11-01 09:00:00', NULL, '2026-11-01 10:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 3, '2026-11-16 09:00:00', NULL, '2026-11-16 10:00:00');
INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, merged_at, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 2, '2026-11-01 09:00:00', '2026-11-18 09:00:00', '2026-11-18 10:00:00');
INSERT INTO git_pull_request_reviews (org_id, repo_id, number, review_id, reviewer, state, submitted_at, last_synced) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 1, 'review-1', 'dev1', 'APPROVED', '2026-11-17 14:00:00', '2026-11-17 15:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 3, 'review-2', 'other', 'APPROVED', '2026-11-16 14:00:00', '2026-11-16 15:00:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 70, '2026-11-11 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 2, 999, '2026-11-11 20:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 200, '2026-11-13 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 4, '2026-11-14 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 1, 400, '2026-11-15 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 1, 7, '2026-11-16 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-16', 0, 0, '2026-11-17 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-17', 0, 0, '2026-11-18 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-18', 0, 0, '2026-11-19 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 1, 3, '2026-11-19 23:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, 10, '2026-11-23 06:00:00', 0),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 999, '2026-11-11 01:00:00', 0);
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 2, 5, '2026-11-12 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 0, 0, '2026-11-20 01:00:00', 1);
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, size_bucket, computed_at) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', '2026-11-10', 'dev1@example.com', 70, 'medium', '2026-11-11 01:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', '2026-11-10', 'dev1-old@example.com', 70, 'medium', '2026-11-11 00:30:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', '2026-11-11', 'dev2@example.com', 50, 'small', '2026-11-11 20:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-03', '2026-11-11', 'Dev Three', 0, 'small', '2026-11-12 01:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', '2026-11-12', 'dev1@example.com', 200, 'medium', '2026-11-13 01:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', '2026-11-13', 'dev2@example.com', 4, 'small', '2026-11-14 01:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', '2026-11-14', 'dev3@example.com', 400, 'large', '2026-11-15 01:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-07', '2026-11-15', 'dev3@example.com', 7, 'small', '2026-11-16 01:00:00'),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', '2026-11-22', 'dev4@example.com', 10, 'small', '2026-11-23 06:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', '2026-11-10', 'dev1@example.com', 999, 'large', '2026-11-11 01:00:00');
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, size_bucket, computed_at) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', '2026-11-11', 'dev2@example.com', 5, 'small', '2026-11-12 01:00:00');
INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, commits_count, loc_added, loc_deleted, computed_at, retracted) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 'dev1@example.com', 1, 60, 10, '2026-11-11 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 'dev2@example.com', 1, 3, 2, '2026-11-12 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 'Dev Three', 1, 0, 0, '2026-11-12 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 'dev1@example.com', 1, 200, 0, '2026-11-13 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 'dev2@example.com', 2, 4, 0, '2026-11-14 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 'dev3@example.com', 1, 400, 0, '2026-11-15 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 'dev3@example.com', 1, 7, 0, '2026-11-16 01:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 'dev4@example.com', 1, 3, 0, '2026-11-19 23:00:00', 0),
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 'dev4@example.com', 1, 8, 2, '2026-11-23 06:00:00', 0),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 'dev1@example.com', 1, 999, 0, '2026-11-11 01:00:00', 0);
INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, commits_count, loc_added, loc_deleted, computed_at, retracted) VALUES
('f0f0f0f0-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 'dev4@example.com', 0, 0, 0, '2026-11-20 01:00:00', 1);
`

// Fire fixtures: rows of another org, so the form's org filter skips them; they only grow the gate's live marks.
const sizing14285s6FireRows1 = `INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced)
SELECT 'e0e0e0e0-0000-4000-8000-000000000000', toUUID('00000000-0000-4000-8000-000000000009'), concat('fire-', toString(number)),
  'f.go', 1, 0, toDateTime64('2026-11-01 00:00:00', 3, 'UTC') FROM numbers(4200000)`

const sizing14285s6FireRows2 = `INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced)
SELECT 'e0e0e0e0-0000-4000-8000-000000000000', toUUID('00000000-0000-4000-8000-000000000009'), concat('fire-', toString(number)),
  'f.go', 1, 0, toDateTime64('2026-11-01 00:00:00', 3, 'UTC') FROM numbers(4200000, 12400000)`

type sizing14285s6Statement struct {
	label string
	text  string
}

// sizing14285s6Split cuts the form at lines whose code part ends with ";", keeping each statement's comment lines.
func sizing14285s6Split(body string) []sizing14285s6Statement {
	var out []sizing14285s6Statement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		code := strings.TrimSpace(chmigrate.StripLineComments(line))
		if !strings.HasSuffix(code, ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		for _, l := range chunk {
			if t := strings.TrimSpace(l); strings.HasPrefix(t, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(t, "-- "))[0]
				break
			}
		}
		out = append(out, sizing14285s6Statement{label: label, text: text})
		chunk = nil
	}
	return out
}

func sizing14285s6Cell(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

type sizing14285s6Parts struct{ rows, marks, parts uint64 }

func sizing14285s6LiveParts(t *testing.T, ctx context.Context, conn driver.Conn) map[string]sizing14285s6Parts {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT table, sum(rows), sum(marks), count() FROM system.parts
		WHERE active AND database = currentDatabase() GROUP BY table`)
	if err != nil {
		t.Fatalf("system.parts: %v", err)
	}
	defer rows.Close()
	out := map[string]sizing14285s6Parts{}
	for rows.Next() {
		var table string
		var p sizing14285s6Parts
		if err := rows.Scan(&table, &p.rows, &p.marks, &p.parts); err != nil {
			t.Fatalf("system.parts scan: %v", err)
		}
		out[table] = p
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("system.parts: %v", err)
	}
	return out
}

func sizing14285s6QueryLog(t *testing.T, ctx context.Context, conn driver.Conn, comment string) ([]string, uint64) {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
		WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comment)
	if err != nil {
		t.Fatalf("query log %s: %v", comment, err)
	}
	defer rows.Close()
	var types []string
	var read uint64
	for rows.Next() {
		var typ string
		var r uint64
		if err := rows.Scan(&typ, &r); err != nil {
			t.Fatalf("query log scan %s: %v", comment, err)
		}
		types = append(types, typ)
		read += r
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("query log %s: %v", comment, err)
	}
	return types, read
}

func TestSizing14285s6ReadFormPassPathAndEveryGateFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(sizing14285s6Form))
	if got := hex.EncodeToString(sum[:]); got != sizing14285s6FormSHA256 {
		t.Fatalf("SIZING-14285-6 (prefix swapped) sha256 = %s, want %s", got, sizing14285s6FormSHA256)
	}
	code := chmigrate.StripLineComments(sizing14285s6Form)
	if n := strings.Count(code, "startsWith(org_id, 'f0f0f0f0')"); n != 14 {
		t.Fatalf("%d org filters in the SQL, want 14", n)
	}
	if strings.Contains(code, "4294967296") || strings.Contains(code, "bytes_on_disk") {
		t.Fatal("the -5 bytes arm is still in the SQL (D4 drops it)")
	}
	statements := sizing14285s6Split(sizing14285s6Form)
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
	}
	if strings.Join(labels, ",") != "stmt0,stmt6,stmt7" {
		t.Fatalf("SIZING-14285-6 statements = %v", labels)
	}

	// Per statement: table -> k (scans of that table), f (2 = reads FINAL), s (scalar subqueries that use cost_gate).
	reads := map[string]map[string]uint64{
		"stmt0": {"commit_metrics": 1, "git_commit_stats": 1, "git_commits": 1, "git_pull_request_reviews": 1,
			"git_pull_requests": 1, "repo_metrics_daily": 1, "user_metrics_daily": 1},
		"stmt6": {"git_commit_stats": 2, "git_commits": 2},
		"stmt7": {"git_commit_stats": 2, "git_commits": 1},
	}
	finalFactor := map[string]uint64{"stmt0": 1, "stmt6": 2, "stmt7": 2}
	scalarScopes := map[string]uint64{"stmt0": 7, "stmt6": 3, "stmt7": 0}
	tablesOf := func(label string) []string {
		names := make([]string, 0, len(reads[label]))
		for name := range reads[label] {
			names = append(names, name)
		}
		sort.Strings(names)
		return names
	}
	gateText := func(label string) string {
		weight := ""
		var arms []string
		for _, name := range tablesOf(label) {
			if k := reads[label][name]; k > 1 {
				arms = append(arms, fmt.Sprintf("table = '%s', %d", name, k))
			}
		}
		if len(arms) > 0 {
			weight = " * multiIf(" + strings.Join(arms, ", ") + ", 1)"
		}
		return fmt.Sprintf("WITH (SELECT %d * intDiv(5 * (sum(marks%s) * 8192 + count()) + 3, 4) + %d * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()\n"+
			"      AND table IN ('%s')) AS read_rows_cap,\n"+
			"  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate\n",
			finalFactor[label], weight, 1+scalarScopes[label], strings.Join(tablesOf(label), "', '"))
	}
	// The cap the gate derives, recomputed here from the same live system.parts: the Go side of every WANT.
	derive := func(label string, live map[string]sizing14285s6Parts) (readRowsCap, kRows, p uint64) {
		var w uint64
		for table, k := range reads[label] {
			w += k * live[table].marks
			kRows += k * live[table].rows
			p += live[table].parts
		}
		capRows := (5*(w*8192+p) + 3) / 4
		return finalFactor[label]*capRows + (1+scalarScopes[label])*(2*p+1), kRows, p
	}
	for _, s := range statements {
		body := chmigrate.StripLineComments(s.text)
		if n := strings.Count(s.text, gateText(s.label)); n != 1 {
			t.Fatalf("%s: gate text found %d times, want 1:\n%s", s.label, n, gateText(s.label))
		}
		for _, one := range []string{"'STOP-COST-GATE'", ">= 20000000", "system.parts", "WITH ", "table IN ("} {
			if n := strings.Count(body, one); n != 1 {
				t.Fatalf("%s: %q found %d times in the SQL, want 1", s.label, one, n)
			}
		}
		if n := strings.Count(body, "read_rows_cap"); n != 3 {
			t.Fatalf("%s: read_rows_cap found %d times, want 3 (alias, gate, output)", s.label, n)
		}
		if n := uint64(strings.Count(body, "AND cost_gate = 0) AS ")); n != scalarScopes[s.label] {
			t.Fatalf("%s: %d scalar subqueries use cost_gate, want %d", s.label, n, scalarScopes[s.label])
		}
		if hasFinal := strings.Contains(body, " FINAL "); hasFinal != (finalFactor[s.label] == 2) {
			t.Fatalf("%s: FINAL in the SQL = %v, but f = %d", s.label, hasFinal, finalFactor[s.label])
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	t.Logf("schema head %s", result.Head)
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("clickhouse version %s", version)
	var granularity uint64
	for table := range reads["stmt0"] {
		var engineFull string
		if err := conn.QueryRow(ctx, "SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = ?", table).Scan(&engineFull); err != nil {
			t.Fatalf("%s engine: %v", table, err)
		}
		if !strings.Contains(engineFull, "index_granularity = 8192") {
			t.Fatalf("%s: engine %q, want index_granularity = 8192 (the form's literal)", table, engineFull)
		}
		granularity++
	}
	t.Logf("index_granularity = 8192 on %d tables", granularity)

	for _, statement := range chmigrate.SplitStatements(sizing14285s6Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	want := map[string]map[string]string{
		"stmt0": {"commits_orgs": "1", "stats_orgs": "1", "prs_orgs": "1", "reviews_orgs": "1", "repo_orgs": "1",
			"user_orgs": "1", "commit_orgs": "1"},
		"stmt6": {"commit_rows_raw": "9", "commit_rows_final": "7", "stat_rows_raw": "10", "stat_rows_final": "8",
			"commit_versions_unmerged": "2", "stat_versions_unmerged": "2", "raw_first_day": "2026-11-10",
			"raw_last_day": "2026-11-22"},
		"stmt7": {"commits_now": "7", "oldrule_same_commits": "3", "oldrule_over_commits": "4", "oldrule_ratio2_commits": "4",
			"oldrule_under_commits": "0"},
	}
	byLabel := map[string]sizing14285s6Statement{}
	for _, s := range statements {
		byLabel[s.label] = s
	}

	// run executes one statement as pm does. It returns the log comment and the error (nil = pass).
	run := func(phase string, s sizing14285s6Statement, wantRow map[string]string, wantCap uint64) (string, error) {
		comment := fmt.Sprintf("sizing14285s6-%s-%s-%d", phase, s.label, time.Now().UnixNano())
		rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), s.text)
		if err != nil {
			return comment, err
		}
		columns := rows.Columns()
		types := rows.ColumnTypes()
		var got []map[string]string
		for rows.Next() {
			cells := make([]any, len(types))
			for i, ct := range types {
				cells[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(cells...); err != nil {
				t.Fatalf("%s %s: scan: %v", phase, s.label, err)
			}
			row := map[string]string{}
			parts := make([]string, 0, len(columns))
			for i, name := range columns {
				row[name] = sizing14285s6Cell(cells[i])
				parts = append(parts, name+"="+row[name])
			}
			t.Logf("%s %s ROW %s", phase, s.label, strings.Join(parts, " "))
			got = append(got, row)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return comment, err
		}
		if len(got) != 1 {
			t.Errorf("%s %s: %d rows, want 1", phase, s.label, len(got))
			return comment, nil
		}
		for name, value := range wantRow {
			if got[0][name] != value {
				t.Errorf("%s %s: %s = %s, want %s", phase, s.label, name, got[0][name], value)
			}
		}
		if got[0]["read_rows_cap"] != fmt.Sprint(wantCap) {
			t.Errorf("%s %s: read_rows_cap = %s, want %d (Go formula on live system.parts)", phase, s.label, got[0]["read_rows_cap"], wantCap)
		}
		for _, name := range columns {
			if _, ok := wantRow[name]; !ok && name != "read_rows_cap" {
				t.Errorf("%s %s: column %s has no WANT in the cell", phase, s.label, name)
			}
		}
		return comment, nil
	}
	type logged struct {
		phase, label, comment string
		readRowsCap, kRows, p uint64
		fire                  bool
	}
	var checks []logged
	logParts := func(phase string, live map[string]sizing14285s6Parts) {
		for _, table := range tablesOf("stmt0") {
			t.Logf("%s parts %s rows=%d marks=%d parts=%d", phase, table, live[table].rows, live[table].marks, live[table].parts)
		}
	}
	// step runs each named statement once; fire = the gate must STOP it (code 395, before any data row).
	step := func(phase string, fire map[string]bool, order ...string) {
		live := sizing14285s6LiveParts(t, ctx, conn)
		logParts(phase, live)
		for _, label := range order {
			readRowsCap, kRows, p := derive(label, live)
			t.Logf("%s %s derived read_rows_cap=%d (k-rows %d, P %d, gate fires iff >= 20000000)", phase, label, readRowsCap, kRows, p)
			if (readRowsCap >= 20000000) != fire[label] {
				t.Fatalf("%s %s: derived read_rows_cap %d, fixture must make fire = %v", phase, label, readRowsCap, fire[label])
			}
			comment, err := run(phase, byLabel[label], want[label], readRowsCap)
			if fire[label] {
				if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
					t.Errorf("%s %s: err = %v, want code 395 STOP-COST-GATE", phase, label, err)
					continue
				}
				t.Logf("%s %s FIRED: %s", phase, label, strings.SplitN(err.Error(), "\n", 2)[0])
			} else if err != nil {
				t.Fatalf("%s %s: SQL error: %v", phase, label, err)
			}
			checks = append(checks, logged{phase, label, comment, readRowsCap, kRows, p, fire[label]})
		}
	}

	step("PASS", nil, "stmt0", "stmt6", "stmt7")

	// Org and rule mutants (acp standing FORM rule, every statement): a cell-side copy of the swapped text with ONE change.
	// Each must CHANGE the output (exact WANT below, differing from the PASS WANT); none fires (the gate text is untouched).
	// ORG: the prefix swapped to another synthetic org ('b0000000' = the fixture's other org; stmt0 uses 'a0a0a0a0', an
	// org with no rows, since every stmt0 table also holds one 'b0000000' org: its row would stay all 1).
	// RULE: stmt0 the prefix match admits every org; stmt6 the FINAL of stat_rows_final; stmt7 the FINAL of the new rule.
	mutants := []struct {
		label, kind, from, to string
		places                int
		want                  map[string]string
	}{
		{"stmt0", "ORG", "'f0f0f0f0'", "'a0a0a0a0'", 7, map[string]string{"commits_orgs": "0", "stats_orgs": "0",
			"prs_orgs": "0", "reviews_orgs": "0", "repo_orgs": "0", "user_orgs": "0", "commit_orgs": "0"}},
		{"stmt0", "RULE", "startsWith(org_id, 'f0f0f0f0')", "startsWith(org_id, '')", 7, map[string]string{
			"commits_orgs": "2", "stats_orgs": "2", "prs_orgs": "2", "reviews_orgs": "2", "repo_orgs": "2", "user_orgs": "2",
			"commit_orgs": "2"}},
		{"stmt6", "ORG", "'f0f0f0f0'", "'b0000000'", 4, map[string]string{"commit_rows_raw": "1", "commit_rows_final": "1",
			"stat_rows_raw": "1", "stat_rows_final": "1", "commit_versions_unmerged": "0", "stat_versions_unmerged": "0",
			"raw_first_day": "2026-11-16", "raw_last_day": "2026-11-16"}},
		{"stmt6", "RULE", "(SELECT count() FROM git_commit_stats FINAL WHERE", "(SELECT count() FROM git_commit_stats WHERE", 1,
			map[string]string{"commit_rows_raw": "9", "commit_rows_final": "7", "stat_rows_raw": "10", "stat_rows_final": "10",
				"commit_versions_unmerged": "2", "stat_versions_unmerged": "0", "raw_first_day": "2026-11-10",
				"raw_last_day": "2026-11-22"}},
		{"stmt7", "ORG", "'f0f0f0f0'", "'b0000000'", 3, map[string]string{"commits_now": "1", "oldrule_same_commits": "1",
			"oldrule_over_commits": "0", "oldrule_ratio2_commits": "0", "oldrule_under_commits": "0"}},
		{"stmt7", "RULE", "FROM git_commit_stats FINAL WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, commit_hash",
			"FROM git_commit_stats WHERE startsWith(org_id, 'f0f0f0f0') GROUP BY repo_id, commit_hash", 1,
			map[string]string{"commits_now": "7", "oldrule_same_commits": "5", "oldrule_over_commits": "2",
				"oldrule_ratio2_commits": "2", "oldrule_under_commits": "0"}},
	}
	mutantLive := sizing14285s6LiveParts(t, ctx, conn)
	for _, m := range mutants {
		base := byLabel[m.label]
		if n := strings.Count(base.text, m.from); n != m.places {
			t.Fatalf("MUTANT-%s %s: %q found %d times, want %d", m.kind, m.label, m.from, n, m.places)
		}
		var changed []string
		for name, value := range m.want {
			if want[m.label][name] != value {
				changed = append(changed, name+" "+want[m.label][name]+"->"+value)
			}
		}
		sort.Strings(changed)
		if len(changed) == 0 || len(m.want) != len(want[m.label]) {
			t.Fatalf("MUTANT-%s %s: WANT must name every column and change at least one", m.kind, m.label)
		}
		readRowsCap, kRows, p := derive(m.label, mutantLive)
		phase := "MUTANT-" + m.kind
		comment, err := run(phase, sizing14285s6Statement{label: m.label, text: strings.ReplaceAll(base.text, m.from, m.to)}, m.want, readRowsCap)
		if err != nil {
			t.Fatalf("%s %s: SQL error (a mutant must change the output, not fire): %v", phase, m.label, err)
		}
		t.Logf("%s %s CHANGED (%d places %q -> %q): %s", phase, m.label, m.places, m.from, m.to, strings.Join(changed, ", "))
		checks = append(checks, logged{phase, m.label, comment, readRowsCap, kRows, p, false})
	}
	if err := conn.Exec(ctx, sizing14285s6FireRows1); err != nil {
		t.Fatalf("fire rows 1: %v", err)
	}
	step("FIRE1", map[string]bool{"stmt6": true, "stmt7": true}, "stmt6", "stmt7", "stmt0")
	if err := conn.Exec(ctx, sizing14285s6FireRows2); err != nil {
		t.Fatalf("fire rows 2: %v", err)
	}
	step("FIRE2", map[string]bool{"stmt0": true}, "stmt0")

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	fires := 0
	for _, c := range checks {
		types, read := sizing14285s6QueryLog(t, ctx, conn, c.comment)
		if c.fire {
			fires++
			t.Logf("%s %s query_log types=%v read_rows=%d bound 2P+1=%d", c.phase, c.label, types, read, 2*c.p+1)
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
				t.Errorf("%s %s: query_log types %v, want [ExceptionBeforeStart]", c.phase, c.label, types)
			}
			if read > 2*c.p+1 {
				t.Errorf("%s %s: query_log read_rows %d over 2P + 1 = %d", c.phase, c.label, read, 2*c.p+1)
			}
			continue
		}
		t.Logf("%s %s query_log types=%v read_rows=%d k-rows=%d read_rows_cap=%d", c.phase, c.label, types, read, c.kRows, c.readRowsCap)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("%s %s: query_log types %v, want [QueryFinish]", c.phase, c.label, types)
		}
		if read > c.readRowsCap {
			t.Errorf("%s %s: query_log read_rows %d over read_rows_cap %d", c.phase, c.label, read, c.readRowsCap)
		}
		if c.phase == "PASS" && read < c.kRows {
			t.Errorf("%s %s: query_log read_rows %d under the k-weighted rows %d (a scan is missing)", c.phase, c.label, read, c.kRows)
		}
	}
	if fires != 3 {
		t.Errorf("gate fired %d times, want 3 (stmt6, stmt7 at FIRE1; stmt0 at FIRE2)", fires)
	}
}
