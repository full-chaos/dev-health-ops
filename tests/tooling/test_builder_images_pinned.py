"""CHAOS-9064: no builder setup step may pull its image anonymously from Docker Hub.

docker/setup-buildx-action pulls moby/buildkit and docker/setup-qemu-action pulls
tonistiigi/binfmt, both from Docker Hub by default. An anonymous pull from a
hosted runner fails once the shared IP uses up Docker Hub's rate limit, and the
job fails before it builds anything. ci/builder_images.tsv names the two images,
mirror-test-images.yml copies them into ghcr.io by digest, and every setup step
in every workflow must point at that ghcr copy and at exactly that digest.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))
OWNER = "full-chaos"


def _declared() -> dict[str, str]:
    """name -> ghcr ref, through the one reader of ci/builder_images.tsv."""
    out = subprocess.run(
        ["bash", str(ROOT / "ci" / "builder_images.sh"), OWNER],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
    ).stdout
    return {
        fields[1]: fields[2]
        for fields in (line.split("\t") for line in out.splitlines())
        if fields[0] == "ghcr"
    }


def _setup_steps() -> list[tuple[str, str, str, dict]]:
    """(file, job, action, with) for every setup-buildx / setup-qemu step."""
    found = []
    for path in WORKFLOWS:
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (document.get("jobs") or {}).items():
            for step in (job or {}).get("steps") or []:
                uses = str(step.get("uses", ""))
                match = re.match(r"docker/(setup-buildx|setup-qemu)-action@", uses)
                if match:
                    found.append((path.name, job_name, match.group(1), step.get("with") or {}))
    return found


def test_the_builder_image_file_names_both_images() -> None:
    assert set(_declared()) == {"buildkit", "binfmt"}


def test_there_are_setup_steps_to_hold() -> None:
    # Fails loudly if the walk finds nothing, so it can never pass vacuously.
    assert len(_setup_steps()) >= 12


@pytest.mark.parametrize("step", _setup_steps(), ids=lambda s: f"{s[0]}:{s[1]}:{s[2]}")
def test_every_setup_step_pulls_the_pinned_ghcr_image(step: tuple[str, str, str, dict]) -> None:
    file_name, job, action, options = step
    declared = _declared()
    if action == "setup-buildx":
        want = declared["buildkit"]
        driver_opts = str(options.get("driver-opts", ""))
        images = [
            line.strip()[len("image=") :]
            for line in driver_opts.splitlines()
            if line.strip().startswith("image=")
        ]
        assert images == [want], (
            f"{file_name}:{job}: docker/setup-buildx-action must set "
            f"`driver-opts: image={want}` (the default pulls moby/buildkit from "
            f"Docker Hub anonymously); got {driver_opts!r}"
        )
    else:
        want = declared["binfmt"]
        assert options.get("image") == want, (
            f"{file_name}:{job}: docker/setup-qemu-action must set `image: {want}` "
            f"(the default pulls tonistiigi/binfmt from Docker Hub anonymously); "
            f"got {options.get('image')!r}"
        )


def test_the_reader_refuses_an_unpinned_ref(tmp_path: Path) -> None:
    bad = tmp_path / "builder_images.tsv"
    bad.write_text("buildkit\tmoby/buildkit:buildx-stable-1\n", encoding="utf-8")
    result = subprocess.run(
        ["bash", str(ROOT / "ci" / "builder_images.sh"), OWNER, str(bad)],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode != 0
    assert "@sha256" in result.stderr
