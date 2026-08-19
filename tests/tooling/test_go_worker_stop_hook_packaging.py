"""The Compose-inherited stop hook must resolve inside the built worker image.

CHAOS-3808: on the local Go-worker Compose stack, `docker compose stop
go-worker` execs ``/usr/local/bin/dev-health-workerctl`` inside the running
container as its lifecycle stop hook. The `worker` Docker target used to stage
only `dev-health-worker`, so the exec failed closed with
``stat /usr/local/bin/dev-health-workerctl: no such file or directory`` --
Compose reported the stop as an error even though a bare `docker stop`
(which never execs into the container) succeeded. The distroless runtime has
no shell to fall back on, so the hook has exactly one way to work: the exact
binary, at the exact path, staged in the image the hook runs against.

A test that only greps the Dockerfile for the `cp` line survives someone
renaming the build stage's staging prefix on one side and not the other, or
letting the container smoke gate's own exec path drift from what actually
gets packaged -- both would leave the literal string intact while the real
path resolution silently breaks again. This instead recomputes the artifact's
final in-image path from the Dockerfile's own build-stage structure and pins
it against the one place that actually execs the binary inside a built image:
ci/check_go_containers.sh's container smoke gate.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "docker" / "go-worker.Dockerfile"
CONTAINER_GATE = ROOT / "ci" / "check_go_containers.sh"
WORKERCTL_SOURCE = ROOT / "cmd" / "dev-health-workerctl"

_STAGE_HEADER = re.compile(r"^FROM\s+.+\s+AS\s+(?P<name>\S+)\s*$", re.MULTILINE)
_CP_WORKERCTL = re.compile(r"cp\s+/out/dev-health-workerctl\s+(?P<dest>\S+);")
_SMOKE_ENTRYPOINT = re.compile(r"--entrypoint\s+(?P<path>\S+)")


def _copy_from_build_source(body: str) -> str | None:
    """The source path of a ``COPY --from=build ... <src> <dst>`` line,
    tolerant of any number of intervening flags (``--chown=...`` etc.) --
    only the token layout (COPY ... --from=build ... <src> <dst>) matters."""
    for line in body.splitlines():
        tokens = line.strip().split()
        if len(tokens) < 3 or tokens[0] != "COPY":
            continue
        if not any(token.startswith("--from=build") for token in tokens):
            continue
        return tokens[-2]
    return None


def _dockerfile_text() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


def _build_stage_body() -> str:
    """The Dockerfile's ``build`` stage, where binaries are compiled and
    staged under /runtime/<target>/ before any final image copies from it."""
    dockerfile = _dockerfile_text()
    headers = list(_STAGE_HEADER.finditer(dockerfile))
    for index, header in enumerate(headers):
        if header.group("name") != "build":
            continue
        end = (
            headers[index + 1].start() if index + 1 < len(headers) else len(dockerfile)
        )
        return dockerfile[header.end() : end]
    raise AssertionError("docker/go-worker.Dockerfile has no 'build' stage")


def _final_stage_body(name: str) -> str:
    dockerfile = _dockerfile_text()
    headers = list(_STAGE_HEADER.finditer(dockerfile))
    for index, header in enumerate(headers):
        if header.group("name") != name:
            continue
        end = (
            headers[index + 1].start() if index + 1 < len(headers) else len(dockerfile)
        )
        return dockerfile[header.end() : end]
    raise AssertionError(f"docker/go-worker.Dockerfile has no {name!r} build stage")


def _packaged_worker_image_path() -> str:
    """The absolute path dev-health-workerctl lands at inside the `worker`
    image, derived from the build stage's staging `cp` plus the final
    `worker` stage's `COPY --from=build` source prefix -- not a pinned
    literal that could go stale on either side independently."""
    cp_match = _CP_WORKERCTL.search(_build_stage_body())
    assert cp_match is not None, (
        "docker/go-worker.Dockerfile's build stage no longer stages "
        "dev-health-workerctl for the worker image; the Compose-inherited "
        "stop hook has nothing to exec"
    )
    dest = cp_match.group("dest")

    prefix = _copy_from_build_source(_final_stage_body("worker"))
    assert prefix is not None, (
        "docker/go-worker.Dockerfile's worker stage no longer copies the "
        "staged build output into the final image"
    )

    assert dest.startswith(prefix), (
        f"dev-health-workerctl is staged at {dest!r}, which the worker "
        f"stage's own COPY (source {prefix!r}) will not pick up -- it will "
        "be absent from the final image"
    )
    return "/" + dest[len(prefix) :]


def test_dev_health_workerctl_source_exists() -> None:
    assert WORKERCTL_SOURCE.is_dir(), (
        "cmd/dev-health-workerctl is missing; there is nothing to package"
    )


def test_the_worker_image_packages_dev_health_workerctl_at_a_real_path() -> None:
    assert _packaged_worker_image_path() == "/usr/local/bin/dev-health-workerctl"


def test_the_container_smoke_gate_execs_the_exact_path_the_image_packages() -> None:
    # This gate is the one place that actually builds the worker image and
    # execs this binary inside it as an entrypoint override, the same way
    # Compose's inherited stop hook does. If the packaged path and the
    # exec'd path ever drift, the exec fails closed with
    # `stat ...: no such file or directory` -- the exact failure this ticket
    # reproduced -- and this assertion is what would have caught it.
    gate = CONTAINER_GATE.read_text(encoding="utf-8")
    match = _SMOKE_ENTRYPOINT.search(gate)
    assert match is not None, (
        "ci/check_go_containers.sh no longer execs dev-health-workerctl as "
        "an entrypoint override, so nothing proves the packaged binary is "
        "actually runnable inside the built worker image"
    )
    assert match.group("path") == _packaged_worker_image_path()
