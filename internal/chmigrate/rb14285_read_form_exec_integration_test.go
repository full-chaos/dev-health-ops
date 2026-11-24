//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of the prod read form FORM-RB14285-13902-1 (CHAOS-13902 card step 3, item 3): not for merge. rbForm is
// the form file with its org prefix (8 hex) replaced by the synthetic "f0f0f0f0"; its sha256 is pinned.
const rbSHA256 = "2bf39ea0b4ba7b8702f478bfd0f5dc364e1395d75c0d870d458e950fb0f4b197"

const rbForm = `-- FORM-RB14285-13902-1: step 3 item-3 read-back of CARD-13902-recompute-11/-12 (CHAOS-14285 joined, acp rule 3), run by pm
--   ONCE after pass 1 (and pass 2), with the card's T0. ClickHouse, SELECT only, 3 statements, one per call. Lane l13902 (lead
--   fcdev), 2026-11-24. Prod: NOT EXECUTED. CI exec: EXEC-PROOF-RB14285-1.md. acp FORM read: OPEN. md: FORM-RB14285-13902-1.md.
-- Shape: SIZING-14285-5 stmt2 (REPO) and stmt3 (COMMIT) (13cebc9f7b6b, acp FORM OK) with the old-rule cut replaced by the card's
--   T0, the stored side limited to the item-3 range 2026-10-22..2026-11-20, and the cost gate of SIZING-14285-6 D1-D3 (acp OK).
-- ONE placeholder (T0_UTC in double braces, 14 places, SQL code only) = T0_UTC.txt of the card's step 0 (YYYY-MM-DD
--   HH:MM:SS UTC), filled by the guarded fill line of the md (fail closed); every T0 test is a toDateTime(<T0>, 'UTC').
-- Rule (card s6, cicd/fence.go-style for repo_user_commit: repo_user_commit_native_executor.go:223-250): pass 1 rewrites every
--   produced key of a day at or after T0 and retracts a live repo/user key it does not produce; so after the run a live key of
--   the range with computed_at < T0 was NOT rewritten (stale), and a key computed at or after T0 must equal the new rule
--   recomputed from raw now (repouser/clickhouse.go:87-92 FINAL reads; compute.go R1-R4 as SIZING-14285-5 header).

-- stmt0 ORG-1 over every table read. WANT 1 in each column (> 1 or 0 = STOP).
-- CAP: k 1 each, f 1, s 4: read_rows_cap = cap + 5 x (2 x P + 1). pm WANT read_rows <= read_rows_cap.
WITH (SELECT 1 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 5 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits', 'repo_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT uniqExact(org_id) FROM git_commits WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS commits_orgs,
  (SELECT uniqExact(org_id) FROM git_commit_stats WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS stats_orgs,
  (SELECT uniqExact(org_id) FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS repo_orgs,
  (SELECT uniqExact(org_id) FROM commit_metrics WHERE startsWith(org_id, 'f0f0f0f0') AND cost_gate = 0) AS commit_orgs,
  read_rows_cap;

-- stmt1 REPO. Stored newest live repo_metrics_daily key in 2026-10-22..2026-11-20 vs the new rule recomputed from raw now.
--   checked = raw commit on the day, computed at/after T0, no raw sync at/after the compute.
-- WANT live_repo_days > 0; keys_checked > 0; match_keys > 0; after_mismatch = 0; after_doubled = 0 (> 0 = STOP, acp reads);
--   stale_keys = 0 (a live key of the range not rewritten by the run: acp reads); stale_doubled = 0 (STOP).
-- INFO with_commits, no_commit_now, count_mismatch (in after_mismatch too), raw_changed_after.
-- CAP: k 1 each, f 2 (FINAL), s 0: read_rows_cap = 2 x cap + 2 x P + 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits', 'repo_metrics_daily')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_repo_days,
  countIf(ifNull(raw.n, 0) > 0) AS with_commits,
  countIf(ifNull(raw.n, 0) = 0 AND s.sn > 0) AS no_commit_now,
  countIf(s.c < toDateTime('{{T0_UTC}}', 'UTC')) AS stale_keys,
  countIf(s.c < toDateTime('{{T0_UTC}}', 'UTC') AND ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND s.sl > raw.rl) AS stale_doubled,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c) AS keys_checked,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND s.sl = raw.rl AND s.sn = raw.n) AS match_keys,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND (s.sl != raw.rl OR s.sn != raw.n)) AS after_mismatch,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS after_doubled,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND s.sn != raw.n) AS count_mismatch,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls >= s.c) AS raw_changed_after,
  read_rows_cap
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        toInt64(argMax(total_loc_touched, computed_at)) AS sl, toInt64(argMax(commits_count, computed_at)) AS sn
      FROM repo_metrics_daily WHERE startsWith(org_id, 'f0f0f0f0')
        AND day >= toDate('2026-10-22') AND day <= toDate('2026-11-20') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT cm.c_repo AS k_repo, cm.c_day AS k_day, count() AS n, sum(ifNull(st.loc, 0)) AS rl,
    max(greatest(cm.c_ls, ifNull(st.s_ls, cm.c_ls))) AS ls
  FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash, toDate(committer_when) AS c_day, last_synced AS c_ls
        FROM git_commits FINAL WHERE startsWith(org_id, 'f0f0f0f0')) AS cm
  LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash,
               sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS loc, max(last_synced) AS s_ls
             FROM git_commit_stats FINAL WHERE startsWith(org_id, 'f0f0f0f0')
             GROUP BY org_id, repo_id, commit_hash) AS st
    ON st.s_org = cm.c_org AND st.s_repo = cm.c_repo AND st.s_hash = cm.c_hash
  GROUP BY cm.c_repo, cm.c_day
) AS raw ON raw.k_repo = s.repo_id AND raw.k_day = s.day
WHERE s.r = 0 AND cost_gate = 0;

-- stmt2 COMMIT. Stored newest commit_metrics row per (repo_id, commit_hash, day, author_email) in the range vs the new rule.
--   commit_metrics has no retracted column (head.json:251): every stored key counts.
-- WANT stored_commits > 0; keys_checked > 0; match_commits > 0; after_mismatch = 0; after_doubled = 0 (> 0 = STOP, acp reads).
--   stale_commits (computed before T0) and commit_no_raw: INFO, acp reads if > 0 (a commit key is never retracted by the run).
-- CAP: k 1 each, f 2 (FINAL), s 0: read_rows_cap = 2 x cap + 2 x P + 1.
WITH (SELECT 2 * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + 1 * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits')) AS read_rows_cap,
  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate
SELECT count() AS stored_commits,
  countIf(ifNull(raw.present, 0) = 1) AS with_raw,
  countIf(ifNull(raw.present, 0) = 0) AS commit_no_raw,
  countIf(s.c < toDateTime('{{T0_UTC}}', 'UTC')) AS stale_commits,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c) AS keys_checked,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND s.sl = raw.rl) AS match_commits,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND s.sl != raw.rl) AS after_mismatch,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS after_doubled,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('{{T0_UTC}}', 'UTC') AND raw.ls >= s.c) AS raw_changed_after,
  read_rows_cap
FROM (SELECT repo_id, commit_hash, day, max(computed_at) AS c, toInt64(argMax(total_loc, computed_at)) AS sl
      FROM commit_metrics WHERE startsWith(org_id, 'f0f0f0f0')
        AND day >= toDate('2026-10-22') AND day <= toDate('2026-11-20') GROUP BY repo_id, commit_hash, day, author_email) AS s
LEFT JOIN (
  SELECT cm.c_repo AS k_repo, cm.c_hash AS k_hash, cm.c_day AS k_day, toUInt8(1) AS present,
    toInt64(ifNull(st.loc, 0)) AS rl, greatest(cm.c_ls, ifNull(st.s_ls, cm.c_ls)) AS ls
  FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash, toDate(committer_when) AS c_day, last_synced AS c_ls
        FROM git_commits FINAL WHERE startsWith(org_id, 'f0f0f0f0')) AS cm
  LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash,
               sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS loc, max(last_synced) AS s_ls
             FROM git_commit_stats FINAL WHERE startsWith(org_id, 'f0f0f0f0')
             GROUP BY org_id, repo_id, commit_hash) AS st
    ON st.s_org = cm.c_org AND st.s_repo = cm.c_repo AND st.s_hash = cm.c_hash
) AS raw ON raw.k_repo = s.repo_id AND raw.k_hash = s.commit_hash AND raw.k_day = s.day
WHERE cost_gate = 0;
`

const rbT0 = "2026-11-25 06:00:00"

// PASS fixture: org f0 repo r1 in the range 2026-10-22..2026-11-20, org b0 (must not show). Keys computed at 07:00 after T0
// (rewritten), 10-27 computed before T0 (stale), 10-21 outside the range, 10-29 retracted.
const rbFixture = `
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h1', 'a', 'a@x', '2026-10-25 10:00:00', '2026-10-25 11:00:00'),
('` + fOrg + `', '` + r1 + `', 'h2', 'a', 'a@x', '2026-10-26 10:00:00', '2026-10-26 11:00:00'),
('` + bOrg + `', '` + r1 + `', 'hb', 'b', 'b@x', '2026-10-25 10:00:00', '2026-10-25 11:00:00');
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h1', 'a', 'a@x', '2026-10-25 10:00:00', '2026-10-25 12:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h1', 'f1.go', 10, 5, '2026-10-25 11:00:00'),
('` + fOrg + `', '` + r1 + `', 'h2', 'f1.go', 3, 0, '2026-10-26 11:00:00'),
('` + bOrg + `', '` + r1 + `', 'hb', 'f1.go', 100, 0, '2026-10-25 11:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h1', 'f1.go', 10, 5, '2026-10-25 12:00:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-25', 1, 15, '2026-11-25 07:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2026-10-26', 1, 3, '2026-11-25 07:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2026-10-27', 0, 0, '2026-11-20 01:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2026-10-21', 0, 0, '2026-11-25 07:00:00', 0),
('` + fOrg + `', '` + r1 + `', '2026-10-29', 1, 9, '2026-11-20 01:00:00', 0),
('` + bOrg + `', '` + r1 + `', '2026-10-25', 1, 100, '2026-11-25 07:00:00', 0);
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-29', 0, 0, '2026-11-25 07:00:00', 1);
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, computed_at) VALUES
('` + fOrg + `', '` + r1 + `', 'h1', '2026-10-25', 'a@x', 15, '2026-11-25 07:00:00'),
('` + fOrg + `', '` + r1 + `', 'h2', '2026-10-26', 'a@x', 3, '2026-11-25 07:00:00'),
('` + fOrg + `', '` + r1 + `', 'h0', '2026-10-21', 'a@x', 4, '2026-11-25 07:00:00'),
('` + bOrg + `', '` + r1 + `', 'hb', '2026-10-25', 'b@x', 100, '2026-11-25 07:00:00');
`

// DOUBLED variant (the form must fail on the defect): 10-28 stored at 07:00 with 2 x the raw LOC.
const rbDoubled = `
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h3', 'a', 'a@x', '2026-10-28 10:00:00', '2026-10-28 11:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('` + fOrg + `', '` + r1 + `', 'h3', 'f1.go', 5, 5, '2026-10-28 11:00:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('` + fOrg + `', '` + r1 + `', '2026-10-28', 1, 20, '2026-11-25 07:00:00', 0);
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, computed_at) VALUES
('` + fOrg + `', '` + r1 + `', 'h3', '2026-10-28', 'a@x', 20, '2026-11-25 07:00:00');
`

func TestRB14285ReadFormExecutesMutantsAndEveryGateFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(rbForm))
	if got := hex.EncodeToString(sum[:]); got != rbSHA256 {
		t.Fatalf("FORM-RB14285-13902-1 (prefix swapped) sha256 = %s, want %s", got, rbSHA256)
	}
	code := chmigrate.StripLineComments(rbForm)
	if n := strings.Count(code, "'f0f0f0f0')"); n != 10 {
		t.Fatalf("%d org filters, want 10", n)
	}
	if n := strings.Count(code, "toDateTime('{{T0_UTC}}', 'UTC')"); n != 14 || strings.Count(rbForm, "{{") != 14 {
		t.Fatalf("%d T0 placeholders in the SQL (want 14, none in comments)", n)
	}
	statements := floor2Split(rbForm)
	if len(statements) != 3 || statements[0].label != "stmt0" || statements[1].label != "stmt1" || statements[2].label != "stmt2" {
		t.Fatalf("statements = %d", len(statements))
	}
	type spec struct {
		tables []string
		f, s   uint64
	}
	specs := map[string]spec{
		"stmt0": {[]string{"commit_metrics", "git_commit_stats", "git_commits", "repo_metrics_daily"}, 1, 4},
		"stmt1": {[]string{"git_commit_stats", "git_commits", "repo_metrics_daily"}, 2, 0},
		"stmt2": {[]string{"commit_metrics", "git_commit_stats", "git_commits"}, 2, 0},
	}
	forms := map[string]string{}
	for _, st := range statements {
		sp := specs[st.label]
		gate := fmt.Sprintf("WITH (SELECT %d * intDiv(5 * (sum(marks) * 8192 + count()) + 3, 4) + %d * (2 * count() + 1) FROM system.parts WHERE active AND database = currentDatabase()\n"+
			"      AND table IN ('%s')) AS read_rows_cap,\n"+
			"  (SELECT throwIf(read_rows_cap >= 20000000, 'STOP-COST-GATE')) AS cost_gate\n", sp.f, 1+sp.s, strings.Join(sp.tables, "', '"))
		if strings.Count(st.text, gate) != 1 {
			t.Fatalf("%s: gate text not found:\n%s", st.label, gate)
		}
		body := chmigrate.StripLineComments(st.text)
		for _, one := range []string{"'STOP-COST-GATE'", ">= 20000000", "system.parts"} {
			if strings.Count(body, one) != 1 {
				t.Fatalf("%s: %q not exactly once", st.label, one)
			}
		}
		if n := uint64(strings.Count(body, "AND cost_gate = 0) AS ")); n != sp.s {
			t.Fatalf("%s: %d scalar scopes use cost_gate, want %d", st.label, n, sp.s)
		}
		forms[st.label] = strings.ReplaceAll(st.text, "{{T0_UTC}}", rbT0)
	}
	derive := func(label string, live map[string]floor2Parts) (uint64, uint64) {
		sp := specs[label]
		var w, p uint64
		for _, table := range sp.tables {
			w += live[table].marks
			p += live[table].parts
		}
		return sp.f*((5*(w*8192+p)+3)/4) + (1+sp.s)*(2*p+1), p
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
	for _, table := range specs["stmt0"].tables {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges %s: %v", table, err)
		}
	}
	for _, statement := range chmigrate.SplitStatements(rbFixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	type logged struct {
		phase, label, comment string
		capRows, p            uint64
		fire                  bool
	}
	var checks []logged
	cols := map[string][]string{
		"stmt0": {"commits_orgs", "stats_orgs", "repo_orgs", "commit_orgs"},
		"stmt1": {"live_repo_days", "with_commits", "no_commit_now", "stale_keys", "stale_doubled", "keys_checked", "match_keys",
			"after_mismatch", "after_doubled", "count_mismatch", "raw_changed_after"},
		"stmt2": {"stored_commits", "with_raw", "commit_no_raw", "stale_commits", "keys_checked", "match_commits", "after_mismatch",
			"after_doubled", "raw_changed_after"},
	}
	run := func(phase, label, text string) (string, error) {
		comment := fmt.Sprintf("rb14285-%s-%s-%d", phase, label, time.Now().UnixNano())
		capRows, p := derive(label, floor2LiveParts(t, ctx, conn))
		rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment})), text)
		var line string
		if err == nil {
			names := rows.Columns()
			types := rows.ColumnTypes()
			n := 0
			for rows.Next() {
				n++
				cells := make([]any, len(types))
				for i, ct := range types {
					cells[i] = reflect.New(ct.ScanType()).Interface()
				}
				if err := rows.Scan(cells...); err != nil {
					t.Fatalf("%s %s: scan: %v", phase, label, err)
				}
				got := map[string]string{}
				parts := []string{}
				for i, name := range names {
					got[name] = floor2Cell(cells[i])
					parts = append(parts, name+"="+got[name])
				}
				t.Logf("%s %s ROW %s", phase, label, strings.Join(parts, " "))
				if got["read_rows_cap"] != fmt.Sprint(capRows) {
					t.Errorf("%s %s: read_rows_cap %s, want %d", phase, label, got["read_rows_cap"], capRows)
				}
				var vals []string
				for _, c := range cols[label] {
					vals = append(vals, got[c])
				}
				line = strings.Join(vals, " ")
				if len(names) != len(cols[label])+1 {
					t.Errorf("%s %s: %d columns, want %d + read_rows_cap", phase, label, len(names), len(cols[label]))
				}
			}
			err = rows.Err()
			_ = rows.Close()
			if err == nil && n != 1 {
				t.Errorf("%s %s: %d rows, want 1", phase, label, n)
			}
		}
		checks = append(checks, logged{phase, label, comment, capRows, p, err != nil})
		return line, err
	}
	expect := func(phase, label, text, want string) {
		got, err := run(phase, label, text)
		if err != nil {
			t.Fatalf("%s %s: SQL error: %v", phase, label, err)
		}
		if got != want {
			t.Errorf("%s %s = %s\n                  want %s (%s)", phase, label, got, want, strings.Join(cols[label], " "))
		}
	}
	mut := func(text, from, to string, places int) string {
		if n := strings.Count(text, from); n != places {
			t.Fatalf("mutant pattern %q found %d times, want %d", from, n, places)
		}
		return strings.ReplaceAll(text, from, to)
	}
	pass := map[string]string{"stmt0": "1 1 1 1", "stmt1": "3 2 0 1 0 2 2 0 0 0 0", "stmt2": "2 2 0 0 2 2 0 0 0"}
	for _, label := range []string{"stmt0", "stmt1", "stmt2"} {
		expect("PASS", label, forms[label], pass[label])
	}
	noFinal := "FROM git_commit_stats FINAL WHERE"
	mutants := []struct {
		label, kind, from, to string
		places                int
		want                  string
	}{
		{"stmt0", "ORG", "'f0f0f0f0'", "'a0a0a0a0'", 4, "0 0 0 0"},
		{"stmt0", "RULE", "startsWith(org_id, 'f0f0f0f0')", "startsWith(org_id, '')", 4, "2 2 2 2"},
		{"stmt1", "ORG", "'f0f0f0f0'", "'b0000000'", 3, "1 1 0 0 0 1 1 0 0 0 0"},
		{"stmt1", "RULE", noFinal, "FROM git_commit_stats WHERE", 1, "3 2 0 1 0 2 1 1 0 0 0"},
		{"stmt2", "ORG", "'f0f0f0f0'", "'b0000000'", 3, "1 1 0 0 1 1 0 0 0"},
		{"stmt2", "RULE", noFinal, "FROM git_commit_stats WHERE", 1, "2 2 0 0 2 1 1 0 0"},
	}
	for _, m := range mutants {
		if m.want == pass[m.label] {
			t.Fatalf("mutant %s %s WANT equals the PASS WANT", m.kind, m.label)
		}
		expect("MUTANT-"+m.kind, m.label, mut(forms[m.label], m.from, m.to, m.places), m.want)
		t.Logf("MUTANT-%s %s CHANGED (%d places %q -> %q): %s -> %s", m.kind, m.label, m.places, m.from, m.to, pass[m.label], m.want)
	}
	for _, statement := range chmigrate.SplitStatements(rbDoubled) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("doubled fixture: %v", err)
		}
	}
	expect("DOUBLED", "stmt1", forms["stmt1"], "4 3 0 1 0 3 2 1 1 0 0")
	expect("DOUBLED", "stmt2", forms["stmt2"], "3 3 0 0 3 2 1 1 0")
	t.Logf("DOUBLED: a key stored at 2 x the raw LOC after T0 -> after_mismatch 1, after_doubled 1 in stmt1 and stmt2 (STOP reading)")

	gate := func(phase string, fires map[string]bool) {
		live := floor2LiveParts(t, ctx, conn)
		for _, label := range []string{"stmt0", "stmt1", "stmt2"} {
			capRows, _ := derive(label, live)
			if (capRows >= 20000000) != fires[label] {
				t.Fatalf("%s %s: derived read_rows_cap %d, the fixture must make fire = %v", phase, label, capRows, fires[label])
			}
			_, err := run(phase, label, forms[label])
			if fires[label] {
				if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
					t.Errorf("%s %s: err = %v, want code 395 STOP-COST-GATE", phase, label, err)
				} else {
					t.Logf("%s %s FIRED at derived read_rows_cap %d", phase, label, capRows)
				}
			} else if err != nil {
				t.Errorf("%s %s: SQL error %v (derived cap %d < 20M: must pass)", phase, label, err, capRows)
			} else {
				t.Logf("%s %s passes at derived read_rows_cap %d", phase, label, capRows)
			}
		}
	}
	fire := func(from, count int) {
		start := time.Now()
		sql := fmt.Sprintf(`INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced)
SELECT '`+eOrg+`', toUUID('00000000-0000-4000-8000-000000000009'), concat('fire-', toString(number)), 'f.go', 1, 0,
  toDateTime64('2026-11-01 00:00:00', 3, 'UTC') FROM numbers(%d, %d)`, from, count)
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("fire rows: %v", err)
		}
		t.Logf("fire rows %d..%d inserted in %s", from, from+count, time.Since(start).Round(time.Millisecond))
	}
	fire(0, 8200000)
	gate("FIRE-A", map[string]bool{"stmt1": true, "stmt2": true})
	fire(8200000, 8200000)
	gate("FIRE-B", map[string]bool{"stmt0": true, "stmt1": true, "stmt2": true})

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	fired := map[string]bool{}
	for _, c := range checks {
		types, read := floor2QueryLog(t, ctx, conn, c.comment)
		if c.fire {
			fired[c.label] = true
			t.Logf("%s %s query_log types=%v read_rows=%d bound 2P+1=%d", c.phase, c.label, types, read, 2*c.p+1)
			if len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > 2*c.p+1 {
				t.Errorf("%s %s: query_log %v read_rows %d, want [ExceptionBeforeStart] and <= %d", c.phase, c.label, types, read, 2*c.p+1)
			}
			continue
		}
		t.Logf("%s %s query_log types=%v read_rows=%d read_rows_cap=%d", c.phase, c.label, types, read, c.capRows)
		if len(types) != 1 || types[0] != "QueryFinish" || read > c.capRows {
			t.Errorf("%s %s: query_log %v read_rows %d, want [QueryFinish] and <= %d", c.phase, c.label, types, read, c.capRows)
		}
	}
	for _, label := range []string{"stmt0", "stmt1", "stmt2"} {
		if !fired[label] {
			t.Errorf("%s: the cost gate never fired", label)
		}
	}
}
