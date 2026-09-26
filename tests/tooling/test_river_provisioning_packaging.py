"""River role provisioning must travel with an image, not a host file.

(CHAOS-6950 deleted the Compose overlay this pinned; compose.yml is the one
Compose surface left, and the same properties hold there.)

CHAOS-3925: the provisioning SQL was bind-mounted into the compose overlay from
a relative source path (``../../scripts/worker/provision_river_roles.sql``). A
host with only Docker and a compose file cannot produce that path, and Docker
answers a missing bind-mount source by creating an empty DIRECTORY rather than
failing -- so the first production bring-up died with
``psql: could not read from input file: Is a directory``, a message that names
psql instead of the missing artifact, and the empty directory then re-won on
every recreate.

The SQL remains the source of truth for the domain role's grants. Only its
delivery changed, so these tests pin the delivery.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "docker" / "Dockerfile"
OVERLAY = (
    ROOT / "compose.yml"
)  # CHAOS-6950: the compose overlay is deleted; compose.yml is the one Compose surface
SOURCE_SQL = ROOT / "scripts" / "worker" / "provision_river_roles.sql"

_COPY = re.compile(
    r"^COPY\s+scripts/worker/provision_river_roles\.sql\s+(?P<dest>\S+)\s*$",
    re.MULTILINE,
)


def _packaged_path() -> str:
    match = _COPY.search(DOCKERFILE.read_text(encoding="utf-8"))
    assert match is not None, (
        "docker/Dockerfile no longer packages provision_river_roles.sql; a "
        "pull-only host cannot provision the River runtime roles without it"
    )
    return match.group("dest")


def _provision_service() -> dict:
    overlay = yaml.safe_load(OVERLAY.read_text(encoding="utf-8"))
    return overlay["services"]["go-river-provision"]


def test_the_provisioning_sql_is_packaged_into_the_runtime_image() -> None:
    assert SOURCE_SQL.is_file()
    assert _packaged_path()


def test_the_overlay_does_not_bind_mount_the_provisioning_sql() -> None:
    # A relative bind-mount source is the defect itself: absent on the host it
    # becomes an empty directory instead of an error.
    volumes = _provision_service().get("volumes") or []
    offending = [
        entry for entry in volumes if "provision_river_roles.sql" in str(entry)
    ]
    assert not offending, (
        f"go-river-provision still mounts the SQL from the host: {offending}"
    )


def test_the_overlay_provisions_with_the_go_leg_not_the_packaged_sql() -> None:
    # CHAOS-6904: go-river-provision is `dho migrate roles` on the Go operator
    # image (no shell, no psql, no SQL path to drift). The packaged SQL stays in
    # the runtime image only for the callers that have not moved yet (the chart's
    # provision-roles Job, ci/lib/go_worker_fixture.sh).
    service = _provision_service()
    assert "entrypoint" not in service
    assert service["command"] == ["migrate", "roles"]
    assert service["build"]["target"] == "operator"


def test_provisioning_runs_before_the_river_schema_migration() -> None:
    # Ordering is a correctness property, not a convenience: the pinned River
    # migration's preflight rejects a runtime role that does not yet exist.
    overlay = yaml.safe_load(OVERLAY.read_text(encoding="utf-8"))
    depends = overlay["services"]["go-river-migrate"]["depends_on"]
    assert (
        depends["go-river-provision"]["condition"] == "service_completed_successfully"
    )
