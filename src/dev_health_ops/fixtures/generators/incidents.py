"""Incident and security alert fixture generators."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.models.git import Incident, Repo, SecurityAlert


class IncidentsGeneratorMixin(BaseGeneratorMixin):
    """Generates incidents and security alerts."""

    def generate_incidents(
        self, days: int = 30, incidents_per_day: int = 1
    ) -> list[Incident]:
        incidents = []
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        incident_index = 0
        current_date = start_date
        while current_date <= end_date:
            daily_count = random.randint(0, max(1, incidents_per_day * 2))
            for _ in range(daily_count):
                started_at = current_date + timedelta(
                    minutes=random.randint(0, 60 * 20)
                )
                resolved_at: datetime | None = started_at + timedelta(
                    hours=random.randint(1, 12)
                )
                status = random.choices(["resolved", "open"], weights=[0.8, 0.2], k=1)[
                    0
                ]
                if status == "open":
                    resolved_at = None

                incident_index += 1
                incidents.append(
                    Incident(
                        repo_id=self.repo_id,
                        incident_id=f"synth-incident-{incident_index}",
                        status=status,
                        started_at=started_at,
                        resolved_at=resolved_at,
                    )
                )
            current_date += timedelta(days=1)
        return incidents

    def generate_security_alerts(
        self,
        repos: list[Repo],
        *,
        count_per_repo: int = 15,
        days: int = 90,
    ) -> list[SecurityAlert]:
        """Generate synthetic SecurityAlert rows for the given repos.

        Produces deterministic, realistic distributions of severity, source,
        and state so local / demo environments render non-trivial security UIs.
        """
        _PACKAGES = [
            "requests",
            "lodash",
            "django",
            "axios",
            "urllib3",
            "express",
            "pillow",
            "numpy",
            "pyyaml",
            "moment",
            "jsonwebtoken",
            "sqlalchemy",
            "node-fetch",
            "jinja2",
            "cryptography",
            "flask",
            "cors",
            "markdown-it",
            "marked",
            "protobuf",
        ]
        _BASE36_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"

        severity_choices = ["critical", "high", "medium", "low", "unknown"]
        severity_weights = [5, 20, 40, 30, 5]

        github_sources = ["dependabot", "code_scanning", "advisory"]
        github_weights = [45, 25, 10]
        gitlab_sources = ["gitlab_vulnerability", "gitlab_dependency"]
        gitlab_weights = [15, 5]

        state_choices = [
            "open",
            "detected",
            "confirmed",
            "fixed",
            "dismissed",
            "resolved",
        ]
        state_weights = [30, 15, 15, 30, 8, 2]

        now = datetime.now(timezone.utc)
        current_year = now.year
        cve_years = [current_year - 2, current_year - 1, current_year]

        alerts: list[SecurityAlert] = []

        for repo in repos:
            is_gitlab = getattr(repo, "provider", "") == "gitlab"

            if is_gitlab:
                sources = gitlab_sources + github_sources
                src_weights = gitlab_weights + [w // 4 for w in github_weights]
            else:
                sources = github_sources + gitlab_sources
                src_weights = github_weights + gitlab_weights

            repo_slug = getattr(repo, "repo", None) or getattr(
                repo, "name", str(repo.id)
            )

            for i in range(count_per_repo):
                alert_id = f"alert-{repo.id}-{i:04d}"

                severity = random.choices(
                    severity_choices, weights=severity_weights, k=1
                )[0]
                source = random.choices(sources, weights=src_weights, k=1)[0]
                state = random.choices(state_choices, weights=state_weights, k=1)[0]

                # created_at — uniform within window
                offset_seconds = random.randint(0, days * 86400)
                created_at = now - timedelta(seconds=offset_seconds)

                # terminal timestamps
                fixed_at = None
                dismissed_at = None
                if state in {"fixed", "resolved"}:
                    span = int((now - created_at).total_seconds())
                    if span > 0:
                        fixed_at = created_at + timedelta(
                            seconds=random.randint(0, span)
                        )
                elif state == "dismissed":
                    span = int((now - created_at).total_seconds())
                    if span > 0:
                        dismissed_at = created_at + timedelta(
                            seconds=random.randint(0, span)
                        )

                # package_name
                package_name: str | None = None
                if source in {"dependabot", "gitlab_dependency"}:
                    package_name = random.choice(_PACKAGES)

                # CVE id — 70% of alerts
                cve_id: str | None = None
                if random.random() < 0.7:
                    cve_year = random.choice(cve_years)
                    cve_num = random.randint(1000, 99999)
                    cve_id = f"CVE-{cve_year}-{cve_num:05d}"

                # URL
                numeric_index = i + 1
                if source == "dependabot":
                    url = (
                        f"https://github.com/{repo_slug}/security/dependabot/{alert_id}"
                    )
                elif source == "code_scanning":
                    url = f"https://github.com/{repo_slug}/security/code-scanning/{numeric_index}"
                elif source == "advisory":
                    seg = lambda: "".join(random.choices(_BASE36_CHARS, k=4))  # noqa: E731
                    url = (
                        f"https://github.com/{repo_slug}/security/advisories/"
                        f"GHSA-{seg()}-{seg()}-{seg()}"
                    )
                elif source == "gitlab_vulnerability":
                    url = f"https://gitlab.com/{repo_slug}/-/security/vulnerabilities/{numeric_index}"
                else:  # gitlab_dependency
                    url = f"https://gitlab.com/{repo_slug}/-/dependencies"

                # title / description
                severity_word = severity.capitalize()
                component = package_name or "Component"
                title = f"{component}: {severity_word} severity vulnerability"
                if cve_id:
                    description = (
                        f"A {severity} severity issue ({cve_id}) was detected in "
                        f"{component}. Review and remediate as appropriate."
                    )
                else:
                    description = (
                        f"A {severity} severity issue was detected in {component}. "
                        "Review and remediate as appropriate."
                    )

                alerts.append(
                    SecurityAlert(
                        repo_id=repo.id,
                        alert_id=alert_id,
                        source=source,
                        severity=severity,
                        state=state,
                        package_name=package_name,
                        cve_id=cve_id,
                        url=url,
                        title=title,
                        description=description,
                        created_at=created_at,
                        fixed_at=fixed_at,
                        dismissed_at=dismissed_at,
                        last_synced=now,
                    )
                )

        return alerts
