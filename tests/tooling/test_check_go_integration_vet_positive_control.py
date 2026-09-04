"""CHAOS-4959 positive control: `go vet -tags=integration` must actually catch
a broken `//go:build integration` file, not merely exit 0 because the tag
matched nothing.

check_integration_vet() in ci/check_go.sh (wired into the `ci`/`fast`/`all`
verbs, and from there into go-quality.yml) is what makes a tagged compile
error fail fast instead of surfacing only from the 30-minute Docker-backed
integration job. test_check_go_public_verbs.py already asserts that wiring
(the verb exists and every composite verb calls it), but wiring alone does
not prove the tag reaches the compiler -- a typo'd tag, a build-constraint
regression in the toolchain, or a `./...` pattern that silently excludes the
tagged files would all leave `go vet -tags=integration` reporting a clean
tree for the wrong reason. ci/check_go.sh:~1341 sets the same precedent for
`check_integration_coverage`: die loudly rather than trust a green run that
never exercised the thing it claims to guard.

This test supplies the missing proof directly: it drops a deliberately
broken integration-tagged file into a throwaway package and confirms
`go vet -tags=integration` reports it, then fixes the file in place and
confirms the same invocation goes clean. Only then is a green
`check_integration_vet` in CI trustworthy.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# A leading underscore keeps this scratch package invisible to every `./...`
# expansion in the tree (Go's tool ignores directory names starting with `.`
# or `_` when expanding `...`) -- verified directly: `go list -tags=integration
# ./...` from ROOT does not name it, so a concurrent `check_go.sh` invocation
# elsewhere in the tree cannot trip over this fixture while it exists. It is
# still addressable directly (not via `...`), which is exactly what the vet
# calls below do.
_SCRATCH_DIR_NAME = "_chaos4959_integration_vet_positive_control_scratch"
_PACKAGE_NAME = "positivecontrolscratch"

_BROKEN_FIXTURE = f"""//go:build integration

package {_PACKAGE_NAME}

// Deliberately invalid: a string literal cannot satisfy an int return. This
// file exists only for the duration of the test below.
func Broken() int {{
	return "not an int"
}}
"""

_FIXED_FIXTURE = _BROKEN_FIXTURE.replace('return "not an int"', "return 0")

_VET_TIMEOUT_SECONDS = 60


def _vet_scratch_package() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["go", "vet", "-mod=readonly", "-tags=integration", f"./{_SCRATCH_DIR_NAME}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=_VET_TIMEOUT_SECONDS,
    )


def test_integration_vet_catches_a_broken_tagged_file() -> None:
    scratch = ROOT / _SCRATCH_DIR_NAME
    assert not scratch.exists(), (
        f"{scratch} already exists -- a previous run of this test did not "
        "clean up, remove it before re-running"
    )
    scratch.mkdir()
    fixture = scratch / "broken_fixture.go"
    try:
        fixture.write_text(_BROKEN_FIXTURE, encoding="utf-8")

        broken = _vet_scratch_package()
        assert broken.returncode != 0, (
            "go vet -tags=integration exited 0 on a deliberately broken "
            "integration-tagged file -- the tag is not reaching the "
            "compiler, so a real compile error under //go:build integration "
            f"would go uncaught by check_integration_vet\nstdout: {broken.stdout}\n"
            f"stderr: {broken.stderr}"
        )
        assert _SCRATCH_DIR_NAME in broken.stderr, (
            "go vet failed, but not on the scratch package -- confirm the "
            f"failure is the injected error, not unrelated noise\nstdout: "
            f"{broken.stdout}\nstderr: {broken.stderr}"
        )

        fixture.write_text(_FIXED_FIXTURE, encoding="utf-8")

        fixed = _vet_scratch_package()
        assert fixed.returncode == 0, (
            "go vet -tags=integration still failed after the fixture's only "
            f"error was fixed -- this test's setup is broken, not the gate\n"
            f"stdout: {fixed.stdout}\nstderr: {fixed.stderr}"
        )
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
