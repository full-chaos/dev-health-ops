from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
)
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.sync.datasets import supported_datasets
from dev_health_ops.workers.task_utils import _extract_owner_repo


@dataclass(frozen=True)
class MigrationIssue:
    config_id: str
    provider: str
    reason: str
    repaired: bool = False


@dataclass
class MigrationReport:
    dry_run: bool
    integrations_created: int = 0
    sources_created: int = 0
    datasets_created: int = 0
    configs_linked: int = 0
    sources_linked: int = 0
    issues: list[MigrationIssue] = field(default_factory=list)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _as_options(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_targets(value: object | None) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _is_numeric(value: object | None) -> bool:
    return value is not None and str(value).isdigit()


def _integration_for_parent(
    session: Session,
    parent: SyncConfiguration,
    report: MigrationReport,
    *,
    dry_run: bool,
) -> Integration | None:
    migrated_id = getattr(parent, "migrated_integration_id", None)
    if migrated_id:
        existing = session.get(Integration, migrated_id)
        if existing is not None:
            return existing

    if dry_run:
        return None

    options = _as_options(parent.sync_options)
    integration = Integration(
        org_id=parent.org_id,
        provider=parent.provider,
        credential_id=parent.credential_id,
        name=parent.name,
        config={
            **options,
            "legacy_sync_config_id": str(parent.id),
            "legacy_sync_config_name": parent.name,
        },
        is_active=parent.is_active,
        schedule_cron=(
            str(options["schedule_cron"]) if options.get("schedule_cron") else None
        ),
        timezone=str(options.get("timezone") or "UTC"),
    )
    session.add(integration)
    session.flush()
    parent.migrated_integration_id = integration.id
    report.integrations_created += 1
    report.configs_linked += 1
    return integration


def _dataset_keys_for_config(config: SyncConfiguration) -> list[str]:
    targets = set(_as_targets(config.sync_targets))
    if not targets:
        return []
    keys: list[str] = []
    for spec in supported_datasets(config.provider):
        if targets.intersection(spec.legacy_targets):
            keys.append(spec.dataset_key)
    return keys


def _ensure_datasets(
    session: Session,
    integration: Integration | None,
    configs: list[SyncConfiguration],
    report: MigrationReport,
    *,
    dry_run: bool,
) -> None:
    dataset_keys: dict[str, set[str]] = {}
    for config in configs:
        config_targets = set(_as_targets(config.sync_targets))
        for dataset_key in _dataset_keys_for_config(config):
            dataset_keys.setdefault(dataset_key, set()).update(config_targets)

    if dry_run or integration is None:
        report.datasets_created += len(dataset_keys)
        return

    existing = {
        dataset.dataset_key: dataset
        for dataset in session.query(IntegrationDataset).filter(
            IntegrationDataset.org_id == integration.org_id,
            IntegrationDataset.integration_id == integration.id,
        )
    }
    for dataset_key, legacy_targets in sorted(dataset_keys.items()):
        if dataset_key in existing:
            continue
        session.add(
            IntegrationDataset(
                org_id=integration.org_id,
                integration_id=integration.id,
                dataset_key=dataset_key,
                is_enabled=True,
                options={"legacy_targets": sorted(legacy_targets)},
            )
        )
        report.datasets_created += 1


def _gitlab_project_id(
    child: SyncConfiguration,
    report: MigrationReport,
    *,
    dry_run: bool,
) -> str | None:
    options = _as_options(child.sync_options)
    project_id = options.get("project_id")
    repo = options.get("repo")
    if project_id is None and repo is not None:
        if not _is_numeric(repo):
            report.issues.append(
                MigrationIssue(
                    config_id=str(child.id),
                    provider=child.provider,
                    reason="gitlab_child_repo_without_numeric_project_id",
                )
            )
            return None
        project_id = str(repo)
        report.issues.append(
            MigrationIssue(
                config_id=str(child.id),
                provider=child.provider,
                reason="gitlab_child_project_id_repaired_from_repo",
                repaired=True,
            )
        )
        if not dry_run:
            options["project_id"] = int(project_id)
            child.sync_options = options

    if not _is_numeric(project_id):
        report.issues.append(
            MigrationIssue(
                config_id=str(child.id),
                provider=child.provider,
                reason="gitlab_child_missing_numeric_project_id",
            )
        )
        return None
    return str(project_id)


def _source_fields(
    child: SyncConfiguration,
    report: MigrationReport,
    *,
    dry_run: bool,
) -> dict[str, Any] | None:
    provider = (child.provider or "").lower()
    options = _as_options(child.sync_options)
    if provider == "github":
        owner_repo = _extract_owner_repo(child.name, options)
        if owner_repo is None:
            report.issues.append(
                MigrationIssue(
                    str(child.id), child.provider, "github_child_missing_owner_repo"
                )
            )
            return None
        owner, repo_name = owner_repo
        full_name = f"{owner}/{repo_name}"
        return {
            "source_type": "repository",
            "external_id": full_name,
            "name": repo_name,
            "full_name": full_name,
            "metadata_": {"owner": owner, "legacy_sync_config_id": str(child.id)},
        }

    if provider == "gitlab":
        project_id = _gitlab_project_id(child, report, dry_run=dry_run)
        if project_id is None:
            return None
        full_name = str(
            options.get("path_with_namespace")
            or options.get("full_name")
            or options.get("project_path")
            or options.get("repo")
            or child.name
        )
        return {
            "source_type": "project",
            "external_id": project_id,
            "name": full_name.rsplit("/", 1)[-1] if full_name else project_id,
            "full_name": full_name,
            "metadata_": {
                "path_with_namespace": full_name,
                "legacy_sync_config_id": str(child.id),
            },
        }

    external_id = (
        options.get("project_id")
        or options.get("project_key")
        or options.get("team_id")
        or options.get("repo")
        or child.name
    )
    source_type = "project" if provider in {"jira", "linear"} else "source"
    return {
        "source_type": source_type,
        "external_id": str(external_id),
        "name": child.name,
        "full_name": str(options.get("full_name") or external_id),
        "metadata_": {"legacy_sync_config_id": str(child.id)},
    }


def _ensure_source(
    session: Session,
    integration: Integration | None,
    child: SyncConfiguration,
    report: MigrationReport,
    *,
    dry_run: bool,
) -> None:
    fields = _source_fields(child, report, dry_run=dry_run)
    if fields is None:
        return
    if dry_run or integration is None:
        report.sources_created += 1
        return

    migrated_id = getattr(child, "migrated_source_id", None)
    if migrated_id and session.get(IntegrationSource, migrated_id) is not None:
        return

    existing = (
        session.query(IntegrationSource)
        .filter(
            IntegrationSource.org_id == child.org_id,
            IntegrationSource.integration_id == integration.id,
            IntegrationSource.provider == child.provider,
            IntegrationSource.external_id == fields["external_id"],
        )
        .one_or_none()
    )
    if existing is not None:
        child.migrated_source_id = existing.id
        report.sources_linked += 1
        return

    now = _now_utc()
    source = IntegrationSource(
        org_id=child.org_id,
        integration_id=integration.id,
        provider=child.provider,
        source_type=fields["source_type"],
        external_id=fields["external_id"],
        name=fields["name"],
        full_name=fields["full_name"],
        metadata_=fields["metadata_"],
        is_enabled=child.is_active,
        discovered_at=now,
        last_seen_at=now,
        last_sync_at=child.last_sync_at,
        last_sync_success=child.last_sync_success,
        last_sync_error=child.last_sync_error,
    )
    session.add(source)
    session.flush()
    child.migrated_source_id = source.id
    report.sources_created += 1
    report.sources_linked += 1


def _parent_configs(session: Session) -> list[SyncConfiguration]:
    return list(
        session.query(SyncConfiguration)
        .filter(SyncConfiguration.parent_id.is_(None))
        .order_by(
            SyncConfiguration.org_id, SyncConfiguration.provider, SyncConfiguration.name
        )
        .all()
    )


def _children_for_parent(
    session: Session, parent: SyncConfiguration
) -> list[SyncConfiguration]:
    return list(
        session.query(SyncConfiguration)
        .filter(SyncConfiguration.parent_id == parent.id)
        .order_by(SyncConfiguration.name)
        .all()
    )


def migrate_configs_to_integrations(
    session: Session, *, dry_run: bool = False
) -> MigrationReport:
    report = MigrationReport(dry_run=dry_run)
    for parent in _parent_configs(session):
        integration = _integration_for_parent(session, parent, report, dry_run=dry_run)
        children = _children_for_parent(session, parent)
        _ensure_datasets(
            session,
            integration,
            [parent, *children],
            report,
            dry_run=dry_run,
        )
        for child in children:
            if (
                integration is not None
                and getattr(child, "migrated_integration_id", None) is None
            ):
                child.migrated_integration_id = integration.id
                report.configs_linked += 1
            _ensure_source(session, integration, child, report, dry_run=dry_run)

    if not dry_run:
        session.flush()
    return report


__all__ = [
    "MigrationIssue",
    "MigrationReport",
    "migrate_configs_to_integrations",
]
