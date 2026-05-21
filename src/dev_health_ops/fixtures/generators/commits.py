"""Commits / files / blame / complexity / PR-commits fixture generators."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.metrics.schemas import FileMetricsRecord
from dev_health_ops.models.git import (
    GitCommit,
    GitCommitStat,
    GitFile,
    GitPullRequest,
)


class CommitsGeneratorMixin(BaseGeneratorMixin):
    """Generates Git commits, commit stats, files, blame, complexity, and PR↔commit links."""

    def generate_commits(
        self, days: int = 30, commits_per_day: int = 5
    ) -> list[GitCommit]:
        commits = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(1, commits_per_day * 2)
            for _ in range(daily_count):
                author_name, author_email = random.choice(self.repo_authors)
                commit_time = current_date + timedelta(seconds=random.randint(0, 86400))
                if commit_time > end_date:
                    continue

                commit_hash = f"{random.getrandbits(128):032x}"
                base_messages = [
                    "fix typo",
                    "add feature",
                    "update docs",
                    "refactor code",
                    "optimize performance",
                    "fix security vulnerability",
                    "bump dependencies",
                    "revert change",
                    "add tests",
                    "improve logging",
                ]
                message = f"Synthetic commit: {random.choice(base_messages)}"
                if random.random() < 0.4:
                    project_key = self.repo_name.split("/")[-1].upper()[:3]
                    issue_num = random.randint(1, 200)
                    prefix = random.choice(["", "Fixes ", "Closes ", "Refs "])
                    message = f"{prefix}{project_key}-{issue_num}: {message}"
                commits.append(
                    GitCommit(
                        repo_id=self.repo_id,
                        hash=commit_hash,
                        message=message,
                        author_name=author_name,
                        author_email=author_email,
                        author_when=commit_time,
                        committer_name=author_name,
                        committer_email=author_email,
                        committer_when=commit_time,
                        parents=1,
                    )
                )
            current_date += timedelta(days=1)

        return commits

    def generate_commit_stats(self, commits: list[GitCommit]) -> list[GitCommitStat]:
        stats = []
        for commit in commits:
            # Each commit touches 1-5 files
            files_to_touch = random.sample(
                self.files, random.randint(1, min(5, len(self.files)))
            )
            for file_path in files_to_touch:
                # 80% small changes, 15% medium, 5% large
                change_type = random.random()
                if change_type < 0.8:
                    additions = random.randint(1, 50)
                elif change_type < 0.95:
                    additions = random.randint(50, 200)
                else:
                    additions = random.randint(200, 1000)

                deletions = random.randint(0, additions)
                stats.append(
                    GitCommitStat(
                        repo_id=self.repo_id,
                        commit_hash=commit.hash,
                        file_path=file_path,
                        additions=additions,
                        deletions=deletions,
                    )
                )
        return stats

    def generate_complexity_metrics(
        self, days: int = 30, *, org_id: str = ""
    ) -> dict[str, list[Any]]:
        from dev_health_ops.metrics.schemas import (
            FileComplexitySnapshot,
            RepoComplexityDaily,
        )

        snapshots = []
        dailies = []
        end_date = datetime.now(timezone.utc)

        for i in range(days):
            day = (end_date - timedelta(days=i)).date()
            computed_at = datetime.now(timezone.utc)

            total_loc = 0
            total_cc = 0
            total_high = 0
            total_very_high = 0

            for file_path in self.files:
                # Synthetic complexity values
                loc = random.randint(50, 500)
                funcs = random.randint(5, 50)
                cc_total = random.randint(funcs, funcs * 5)
                cc_avg = cc_total / funcs

                high = 0
                very_high = 0
                if cc_avg > 10:
                    high = random.randint(1, funcs // 3)
                if cc_avg > 20:
                    very_high = random.randint(0, high // 2)

                snapshots.append(
                    FileComplexitySnapshot(
                        repo_id=self.repo_id,
                        as_of_day=day,
                        ref="HEAD",
                        file_path=file_path,
                        language="python",
                        loc=loc,
                        functions_count=funcs,
                        cyclomatic_total=cc_total,
                        cyclomatic_avg=cc_avg,
                        high_complexity_functions=high,
                        very_high_complexity_functions=very_high,
                        computed_at=computed_at,
                        org_id=org_id,
                    )
                )

                total_loc += loc
                total_cc += cc_total
                total_high += high
                total_very_high += very_high

            cc_per_kloc = (total_cc / (total_loc / 1000.0)) if total_loc > 0 else 0.0

            dailies.append(
                RepoComplexityDaily(
                    repo_id=self.repo_id,
                    day=day,
                    loc_total=total_loc,
                    cyclomatic_total=total_cc,
                    cyclomatic_per_kloc=cc_per_kloc,
                    high_complexity_functions=total_high,
                    very_high_complexity_functions=total_very_high,
                    computed_at=computed_at,
                    org_id=org_id,
                )
            )

        return {"snapshots": snapshots, "dailies": dailies}

    def generate_files(self) -> list[GitFile]:
        return [
            GitFile(repo_id=self.repo_id, path=f, executable=False) for f in self.files
        ]

    def _generate_synthetic_python_lines(self, file_path: str) -> list[str]:
        target_lines = random.randint(30, 140)
        safe_name = (
            file_path.replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
            .replace("-", "_")
        )
        safe_name = "".join(
            ch if (ch.isalnum() or ch == "_") else "_" for ch in safe_name
        )
        safe_name = safe_name.strip("_") or "synthetic_module"

        lines: list[str] = [
            f'"""Synthetic fixture module: {safe_name}."""',
            "",
            "from __future__ import annotations",
            "",
            "from typing import Iterable",
            "",
        ]

        max_functions = 6
        for func_idx in range(max_functions):
            func_name = f"{safe_name}_fn_{func_idx}"
            threshold = random.randint(3, 12)
            multiplier = random.randint(2, 7)

            block = [
                f"def {func_name}(values: Iterable[int]) -> int:",
                "    total = 0",
                "    for idx, value in enumerate(values):",
                f"        if value % {threshold} == 0:",
                f"            total += value * {multiplier}",
                "        elif value % 2 == 0:",
                "            total += value",
                "        elif value < 0:",
                "            total -= value",
                "        else:",
                "            total -= value // 2",
                "        if idx % 5 == 0 and total > 0:",
                "            total //= 2",
                "    return total",
                "",
            ]

            # Ensure we never truncate mid-block (keeps generated code parseable).
            if func_idx >= 2 and (len(lines) + len(block)) > target_lines:
                break
            lines.extend(block)

        while len(lines) < target_lines:
            lines.append(f"# filler {len(lines) + 1} for {file_path}")
        return lines

    def generate_blame(
        self, commits: list[GitCommit]
    ) -> list[
        Any
    ]:  # using Any to avoid circular import issues if GitBlame isn't imported, but it is
        # We need to import GitBlame inside the method or file level
        from dev_health_ops.models.git import GitBlame

        blame_records: list[GitBlame] = []
        if not commits:
            return blame_records

        for file_path in self.files:
            if file_path.endswith(".py"):
                lines = self._generate_synthetic_python_lines(file_path)
            else:
                num_lines = random.randint(10, 200)
                lines = [
                    f"Line {i} content for {file_path}" for i in range(1, num_lines + 1)
                ]

            for i, line in enumerate(lines, start=1):
                # Pick a random commit that "modified" this line
                commit = random.choice(commits)

                blame_records.append(
                    GitBlame(
                        repo_id=self.repo_id,
                        path=file_path,
                        line_no=i,
                        author_email=commit.author_email,
                        author_name=commit.author_name,
                        author_when=commit.author_when,
                        commit_hash=commit.hash,
                        line=line,
                    )
                )
        return blame_records

    def generate_pr_commits(
        self,
        prs: list[GitPullRequest],
        commits: list[GitCommit],
        *,
        org_id: str = "",
    ) -> list[dict[str, Any]]:
        """
        Link PRs to commits.
        Assumes commits and PRs are already generated.
        Returns a list of dicts suitable for insertion into work_graph_pr_commit.
        """
        links = []
        synced_at = datetime.now(timezone.utc)

        # Sort commits by date
        commits_sorted = sorted(commits, key=lambda c: c.committer_when)

        # For each PR, pick a range of commits that happened before PR merge/close
        # and after PR creation (loosely).

        # Shuffle PRs to distribute commits
        shuffled_prs = list(prs)
        random.shuffle(shuffled_prs)

        # Naive distribution: each PR gets 1-5 commits
        # If we have more commits than PRs * 5, some commits might be orphaned (which is fine, direct pushes)
        # If we have fewer, we reuse commits? No, commits belong to one PR usually.

        available_commits = list(commits_sorted)

        for pr in shuffled_prs:
            if not commits_sorted:
                break

            upper = min(5, len(available_commits)) if available_commits else 0
            if upper >= 2:
                num_commits = random.randint(2, upper)
            else:
                num_commits = 2

            # Pick commits close to PR creation
            # This is O(N^2) effectively if we iterate, but lists are small for fixtures.
            # Let's just pop from available for simplicity in synthetic gen.

            pr_commits = []
            for _ in range(num_commits):
                if not available_commits:
                    break
                # Pop from end? or start? Start is oldest.
                # PRs are somewhat random in time.
                # Let's just pick random commits for now, but valid logic would be better.
                # Given strict requirements, let's just assign.
                c = available_commits.pop(0)
                pr_commits.append(c)

            if len(pr_commits) < num_commits:
                supplement = [c for c in commits_sorted if c not in pr_commits]
                need = min(num_commits - len(pr_commits), len(supplement))
                if need > 0:
                    pr_commits.extend(random.sample(supplement, k=need))

            for c in pr_commits:
                links.append(
                    {
                        "repo_id": str(pr.repo_id),
                        "pr_number": pr.number,
                        "commit_hash": c.hash,
                        "confidence": 1.0,
                        "provenance": "native",
                        "evidence": "generated_fixture",
                        "last_synced": synced_at,
                        "org_id": org_id,
                    }
                )

        return links

    def generate_file_hotspot_daily(self, days: int = 30) -> list[Any]:
        """Generate file hotspot daily records using synthetic complexity and churn data."""
        from dev_health_ops.metrics.schemas import FileHotspotDaily

        records = []
        end_date = datetime.now(timezone.utc)
        computed_at = datetime.now(timezone.utc)

        for i in range(days):
            day = (end_date - timedelta(days=i)).date()
            for file_path in self.files:
                churn_loc = random.randint(10, 500)
                churn_commits = random.randint(1, 20)
                cc_total = random.randint(5, 100)
                funcs = random.randint(3, 30)
                cc_avg = cc_total / funcs if funcs else 0.0
                blame_conc = random.uniform(0.3, 1.0)

                # risk = normalized(churn) + normalized(complexity)
                risk_score = random.uniform(-1.0, 3.0)

                records.append(
                    FileHotspotDaily(
                        repo_id=self.repo_id,
                        day=day,
                        file_path=file_path,
                        churn_loc_30d=churn_loc,
                        churn_commits_30d=churn_commits,
                        cyclomatic_total=cc_total,
                        cyclomatic_avg=cc_avg,
                        blame_concentration=blame_conc,
                        risk_score=risk_score,
                        computed_at=computed_at,
                    )
                )
        return records

    def generate_file_metrics(self) -> list[FileMetricsRecord]:
        records = []
        computed_at = datetime.now(timezone.utc)
        today = computed_at.date()
        for file_path in self.files:
            records.append(
                FileMetricsRecord(
                    repo_id=self.repo_id,
                    day=today,
                    path=file_path,
                    churn=random.randint(10, 1000),
                    contributors=random.randint(1, 5),
                    commits_count=random.randint(1, 20),
                    hotspot_score=random.uniform(0.0, 1.0),
                    computed_at=computed_at,
                )
            )
        return records
