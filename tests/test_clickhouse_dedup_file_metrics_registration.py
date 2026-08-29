"""CHAOS-4459 (codex review, P1): file_metrics_daily must be registered for
read-time dedup.

``file_metrics_daily`` is a plain (append-only) MergeTree, same class as
``dora_metrics_daily`` (see ``test_clickhouse_dedup_dora_registration.py``) --
a redrive/recompute (CHAOS-4459's ``metrics partition-recompute`` verb, or an
ordinary post-sync re-drive) writes a SECOND ``(org_id, repo_id, day, path)``
row with a fresher ``computed_at``, never replacing the first.

Before this fix, ``file_metrics_daily`` was in neither
``RERUN_DEDUPED_DAILY_TABLES`` nor ``_APPEND_ONLY_DAILY_KEYS`` --
``api/queries/heatmap.py``'s ``fetch_hotspot_risk``/``fetch_hotspot_evidence``
summed the raw table directly, with no dedup at all (codex round-1 review on
PR #1990: a redriven day's hotspot/churn scores silently doubled).
"""

from __future__ import annotations

from dev_health_ops.clickhouse_dedup import dedup_from


def test_file_metrics_daily_source_is_deduplicated_on_read() -> None:
    source = dedup_from("file_metrics_daily")
    assert source != "file_metrics_daily", (
        "dedup_from('file_metrics_daily') returned the RAW table name -- "
        "file_metrics_daily is not registered in either RERUN_DEDUPED_DAILY_TABLES "
        "or _APPEND_ONLY_DAILY_KEYS, so a hotspot/churn read double-counts any "
        "re-drive or partition-recompute."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    # The natural key must include path: file_metrics_daily's own sorting
    # key is (org_id, repo_id, day, path) (migration 027). Collapsing
    # without path would silently pick one file's row for every file in the
    # same repo/day.
    for column in ("org_id", "repo_id", "day", "path"):
        assert column in source, f"file_metrics_daily dedup key is missing {column}"


def test_file_metrics_daily_alias_is_preserved() -> None:
    # heatmap.py's readers reference `file_metrics_daily.repo_id` /
    # `file_metrics_daily.org_id` after the FROM clause -- dedup_from must
    # keep that alias so those references stay valid.
    source = dedup_from("file_metrics_daily")
    assert source.rstrip().endswith("AS file_metrics_daily")


def test_file_hotspot_daily_source_is_deduplicated_on_read() -> None:
    # CHAOS-4459 (codex review round 3): file_hotspot_daily is the
    # file_risk_hotspots family's own output table, missing from the
    # GENERIC report-registry dedup path (reports/charts.py's
    # dedup_from(definition.source_table)) even though its hand-rolled
    # readers already dedup correctly.
    source = dedup_from("file_hotspot_daily")
    assert source != "file_hotspot_daily", (
        "dedup_from('file_hotspot_daily') returned the RAW table name -- "
        "the generic report-registry chart path (reports/charts.py) reads "
        "it unguarded."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    for column in ("org_id", "repo_id", "day", "file_path"):
        assert column in source, f"file_hotspot_daily dedup key is missing {column}"


def test_review_edges_daily_source_is_deduplicated_on_read() -> None:
    # CHAOS-4459 (codex review round 4): review_edges_daily is another
    # generic report-registry source_table (review-load charts,
    # sum(reviews_count)) that partition-recompute's own doc comment admits
    # gets re-executed on every recompute (every family in the partition,
    # not just repo_user_commit) -- missing from the dedup registry, the
    # exact same gap class as file_metrics_daily/file_hotspot_daily above.
    source = dedup_from("review_edges_daily")
    assert source != "review_edges_daily", (
        "dedup_from('review_edges_daily') returned the RAW table name -- "
        "the generic report-registry chart path (reports/charts.py) reads "
        "it unguarded, doubling reviews_count for a redriven/recomputed day."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    # Natural key matches migration 027's canonical sorting key (org_id,
    # repo_id, reviewer, author, day) -- migration 024 added org_id to this
    # table after 004 originally created it without one.
    for column in ("org_id", "repo_id", "reviewer", "author", "day"):
        assert column in source, f"review_edges_daily dedup key is missing {column}"


def test_review_edges_daily_dedup_key_includes_org_id() -> None:
    # CHAOS-4459 (codex review round 5, P1): the first version of this key
    # omitted org_id, wrongly claiming the table has no such column. Two
    # orgs syncing the same repo slug share a deterministic repo_id; an
    # org_id-less key lets LIMIT 1 BY pick ONE org's row before any org_id
    # filter downstream ever runs, silently discarding the other org's
    # review data for that repo/day. Dedicated regression test, separate
    # from the general dedup-is-present assertion above.
    source = dedup_from("review_edges_daily")
    assert "org_id" in source, (
        "review_edges_daily's dedup key is missing org_id -- two "
        "organizations syncing the same repo would collapse into one "
        "organization's row before any tenant filter applies."
    )


def test_commit_metrics_source_is_deduplicated_on_read() -> None:
    # CHAOS-4459 (self-audit against families.json's full write-table list,
    # requested by team-lead after codex round 4): commit_metrics is THIS
    # TICKET'S OWN target table, also a generic report-registry source_table,
    # also missing a dedup key -- a repeat partition-recompute of the same
    # day (this PR's own integration test does exactly that) would double
    # the commit charts' sums without this fix.
    source = dedup_from("commit_metrics")
    assert source != "commit_metrics", (
        "dedup_from('commit_metrics') returned the RAW table name -- the "
        "generic report-registry chart path (reports/charts.py) reads it "
        "unguarded, doubling commit charts for a repeated recompute."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    # Natural key matches migration 027's canonical sorting key (org_id,
    # repo_id, day, author_email, commit_hash) -- migration 024 added
    # org_id to this table after 001 originally created it without one.
    for column in ("org_id", "repo_id", "day", "author_email", "commit_hash"):
        assert column in source, f"commit_metrics dedup key is missing {column}"


def test_commit_metrics_dedup_key_includes_org_id() -> None:
    # CHAOS-4459 (codex review round 5, P1): same org_id omission as
    # review_edges_daily above, same migration-024 root cause. Two orgs
    # syncing the same repo slug share a deterministic repo_id; an
    # org_id-less key lets LIMIT 1 BY pick ONE org's commits before any
    # org_id filter downstream ever runs, silently discarding the other
    # org's commit history for that repo/day -- directly undermining this
    # ticket's own repair goal for a shared-repo org.
    source = dedup_from("commit_metrics")
    assert "org_id" in source, (
        "commit_metrics's dedup key is missing org_id -- two organizations "
        "syncing the same repo would collapse into one organization's "
        "commits before any tenant filter applies."
    )
