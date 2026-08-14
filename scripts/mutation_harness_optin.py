"""Fail-closed plan validation for mutation-harness sharding.

The serial harness accepts version-1 plans. Parallel execution adds a reviewed
resource and toolchain contract, so it validates the complete plan vocabulary
before any execution tree is staged.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA_VERSION = 1

TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "name",
        "build",
        "mutations",
        "sharding",
        "$build_note",
        "$comment",
        "$determination",
        "$limitation",
        "$where_mutations_die",
    }
)
MUTATION_KEYS = frozenset(
    {
        "id",
        "file",
        "find",
        "replace",
        "proof",
        "rationale",
        "build",
        "expect_occurrences",
        "allow_comment_anchor",
        "expected_survivor_reason",
        "$note",
        "observed_kill",
    }
)
SHARDING_KEYS = frozenset(
    {
        "max_shards",
        "workspace_inputs",
        "external_resources",
        "shared_mutable_resource_exclusions",
    }
)
EXTERNAL_RESOURCE_POLICIES = frozenset({"none", "isolated", "shared-readonly"})
GO_CACHE_EXCLUSIONS = ("go-build-cache", "go-module-cache")

_BARE_PYTHON_RE = re.compile(
    r"(?:^|[;&|()\s])(?:python(?:3(?:\.\d+)*)?|pytest|uv)(?=\s|$)"
)
_PWD_PYTHON_RE = re.compile(r"\$PWD/[^\s'\"]*/bin/python(?:3(?:\.\d+)*)?")
_PWD_CONSOLE_SCRIPT_RE = re.compile(r"\$PWD/[^\s'\"]*/bin/(?:pytest|uv)(?=\s|$)")
_SHARD_IMPORT_RE = re.compile(
    r"(?:^|\s)PYTHONPATH\s*=\s*(?:['\"])?\$PWD(?:/[^\s'\"]*)?"
)
_SHELL_GO_TEST_RE = re.compile(r"(?:^|[;&|()\s])go\s+test(?:\s|$)")
_SHELL_COUNT_RE = re.compile(
    r"(?:^|\s)-count(?:=(?P<equals>[^\s;&|]+)|\s+(?P<spaced>[^\s;&|]+))"
)
_SHELL_RUN_RE = re.compile(
    r"(?:^|\s)-run(?:=|\s+)(?P<pattern>'[^']*'|\"[^\"]*\"|[^\s;&|]+)"
)
_SAFE_ENV_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


class PlanContractError(RuntimeError):
    """A plan cannot safely opt in to sharded execution."""


@dataclass(frozen=True)
class ShardingPlan:
    """The reviewed, additive sharding contract for a version-1 plan."""

    max_shards: int
    workspace_inputs: tuple[str, ...]
    external_resources: str
    shared_mutable_resource_exclusions: tuple[str, ...]


def _refuse_unknown_keys(
    path: Path, raw: dict[str, Any], allowed: frozenset[str], context: str
) -> None:
    unknown = sorted(set(raw).difference(allowed))
    if unknown:
        rendered = ", ".join(repr(key) for key in unknown)
        raise PlanContractError(f"{path}: unknown {context} key(s): {rendered}")


def _safe_workspace_input(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise PlanContractError(
            "sharding.workspace_inputs entries must be non-empty repo-relative "
            "POSIX paths"
        )
    if os.path.isabs(value):
        raise PlanContractError(
            f"sharding.workspace_inputs must be repo-relative, got {value!r}"
        )
    candidate = PurePosixPath(value)
    if candidate in {
        PurePosixPath("."),
        PurePosixPath(".git"),
        PurePosixPath(".mutation-harness"),
    }:
        raise PlanContractError(
            f"sharding.workspace_inputs may not name {candidate.as_posix()!r}"
        )
    if ".." in candidate.parts or candidate.parts[0] in {".git", ".mutation-harness"}:
        raise PlanContractError(f"sharding.workspace_inputs path is unsafe: {value!r}")
    return candidate.as_posix()


def _validated_argv(value: object, context: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise PlanContractError(f"{context} must be a non-empty argv array")
    if not all(isinstance(word, str) and word for word in value):
        raise PlanContractError(f"{context} argv entries must be non-empty strings")
    return tuple(value)


def _command_text(argv: tuple[str, ...]) -> str:
    if len(argv) >= 3 and argv[0] in {"bash", "sh"} and argv[1] in {"-c", "-lc"}:
        return argv[2]
    return " ".join(argv)


def _entry_point(argv: tuple[str, ...]) -> str:
    if argv[0] != "env":
        return argv[0]
    for word in argv[1:]:
        if word.startswith("-") or "=" in word:
            continue
        return word
    return ""


def _validate_python_resolution(identifier: str, argv: tuple[str, ...]) -> None:
    text = _command_text(argv)
    direct = _entry_point(argv)
    is_shell_wrapper = (
        len(argv) >= 3
        and direct in {"bash", "sh"}
        and argv[1]
        in {
            "-c",
            "-lc",
        }
    )
    path_resolved = bool(re.fullmatch(r"python(?:3(?:\.\d+)*)?|pytest|uv", direct))
    path_resolved = path_resolved or (
        is_shell_wrapper and bool(_BARE_PYTHON_RE.search(text))
    )
    if path_resolved:
        raise PlanContractError(
            f"{identifier}: sharding refuses a PATH-resolved Python entry point; "
            "use a $PWD-anchored interpreter"
        )

    if (
        _PWD_CONSOLE_SCRIPT_RE.search(text)
        or direct.startswith("$PWD/")
        and direct.endswith(("/pytest", "/uv"))
    ):
        raise PlanContractError(
            f"{identifier}: a $PWD-anchored Python console script retains an "
            "absolute shebang; use $PWD/.venv/bin/python -m <tool>"
        )

    reaches_python = bool(_PWD_PYTHON_RE.search(text)) or (
        direct.startswith("$PWD/") and "/bin/python" in direct
    )
    has_shard_import = bool(_SHARD_IMPORT_RE.search(text)) or any(
        word.startswith("PYTHONPATH=$PWD/") for word in argv
    )
    if reaches_python and not has_shard_import:
        raise PlanContractError(
            f"{identifier}: Python proof lacks a shard-rooted import path such "
            'as PYTHONPATH="$PWD/src"'
        )


def _direct_flag(argv: tuple[str, ...], name: str) -> str | None:
    for index, word in enumerate(argv):
        prefix = f"{name}="
        if word.startswith(prefix):
            return word[len(prefix) :]
        if word == name and index + 1 < len(argv):
            return argv[index + 1]
    return None


def _direct_flag_values(argv: tuple[str, ...], name: str) -> list[str]:
    values: list[str] = []
    for index, word in enumerate(argv):
        prefix = f"{name}="
        if word.startswith(prefix):
            values.append(word[len(prefix) :])
        elif word == name:
            values.append(argv[index + 1] if index + 1 < len(argv) else "")
    return values


def _shell_count_values(text: str) -> list[str]:
    values: list[str] = []
    for match in _SHELL_COUNT_RE.finditer(text):
        value = match.group("equals") or match.group("spaced") or ""
        values.append(value.strip("'\""))
    return values


def _require_single_count_one(
    identifier: str, pattern: str, count_values: list[str]
) -> None:
    if count_values != ["1"]:
        raise PlanContractError(
            f"{identifier}: named go test proof {pattern!r} must declare exactly "
            "one unambiguous -count=1"
        )


def _validate_go_test_freshness(identifier: str, argv: tuple[str, ...]) -> bool:
    """Validate a named ``go test`` proof and report whether it uses Go."""

    if len(argv) >= 2 and argv[0] == "go" and argv[1] == "test":
        pattern = _direct_flag(argv, "-run")
        if pattern is None or pattern == "^$":
            return True
        _require_single_count_one(
            identifier, pattern, _direct_flag_values(argv, "-count")
        )
        return True

    text = _command_text(argv)
    if not _SHELL_GO_TEST_RE.search(text):
        return False
    run_match = _SHELL_RUN_RE.search(text)
    if run_match is None:
        return True
    pattern = run_match.group("pattern").strip("'\"")
    if pattern != "^$":
        _require_single_count_one(identifier, pattern, _shell_count_values(text))
    return True


def _validate_mutation_commands(raw: dict[str, Any]) -> bool:
    entries = raw.get("mutations")
    if not isinstance(entries, list) or not entries:
        raise PlanContractError("plan must declare a non-empty mutations array")
    uses_go = False
    plan_build = raw.get("build")
    if plan_build is not None:
        argv = _validated_argv(plan_build, "plan.build")
        _validate_python_resolution("plan.build", argv)
        uses_go = (len(argv) >= 2 and argv[0] == "go" and argv[1] == "test") or bool(
            _SHELL_GO_TEST_RE.search(_command_text(argv))
        )
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise PlanContractError(f"mutation {index} must be an object")
        identifier = str(entry.get("id") or f"mutation {index}")
        proof = entry.get("proof")
        if not isinstance(proof, list) or not proof:
            raise PlanContractError(f"{identifier}: proof must be a non-empty array")
        mutation_build = entry.get("build")
        if mutation_build is not None:
            build_argv = _validated_argv(mutation_build, f"{identifier}.build")
            _validate_python_resolution(f"{identifier}.build", build_argv)
            uses_go = (
                uses_go
                or (
                    len(build_argv) >= 2
                    and build_argv[0] == "go"
                    and build_argv[1] == "test"
                )
                or bool(_SHELL_GO_TEST_RE.search(_command_text(build_argv)))
            )
        for proof_index, value in enumerate(proof):
            argv = _validated_argv(value, f"{identifier}.proof[{proof_index}]")
            _validate_python_resolution(identifier, argv)
            uses_go = _validate_go_test_freshness(identifier, argv) or uses_go
    return uses_go


def _load_sharding_plan(
    path: Path, raw: dict[str, Any], *, uses_go: bool
) -> ShardingPlan | None:
    value = raw.get("sharding")
    if value is None:
        return None
    if not isinstance(value, dict):
        raise PlanContractError(f"{path}: sharding must be an object")
    _refuse_unknown_keys(path, value, SHARDING_KEYS, "sharding")

    max_shards = value.get("max_shards")
    if (
        not isinstance(max_shards, int)
        or isinstance(max_shards, bool)
        or max_shards < 1
    ):
        raise PlanContractError("sharding.max_shards must be an integer at least 1")

    workspace_raw = value.get("workspace_inputs", [])
    if not isinstance(workspace_raw, list):
        raise PlanContractError("sharding.workspace_inputs must be an array")
    workspace_inputs = tuple(_safe_workspace_input(item) for item in workspace_raw)
    if len(workspace_inputs) != len(set(workspace_inputs)):
        raise PlanContractError("sharding.workspace_inputs contains a duplicate path")

    external_resources = value.get("external_resources")
    if external_resources not in EXTERNAL_RESOURCE_POLICIES:
        allowed = "|".join(sorted(EXTERNAL_RESOURCE_POLICIES))
        raise PlanContractError(
            f"sharding.external_resources must be exactly one of {allowed}"
        )

    exclusions_raw = value.get("shared_mutable_resource_exclusions", [])
    if not isinstance(exclusions_raw, list) or not all(
        isinstance(item, str) and item for item in exclusions_raw
    ):
        raise PlanContractError(
            "sharding.shared_mutable_resource_exclusions must be an array of names"
        )
    exclusions = tuple(exclusions_raw)
    unknown_exclusions = sorted(set(exclusions).difference(GO_CACHE_EXCLUSIONS))
    if unknown_exclusions:
        raise PlanContractError(
            "unknown shared mutable resource exclusion(s): "
            + ", ".join(unknown_exclusions)
        )
    if len(exclusions) != len(set(exclusions)):
        raise PlanContractError("shared mutable resource exclusions contain duplicates")
    if (
        uses_go
        and external_resources == "none"
        and set(exclusions) != set(GO_CACHE_EXCLUSIONS)
    ):
        raise PlanContractError(
            "Go plans declaring external_resources 'none' must explicitly name "
            "the go-build-cache and go-module-cache exclusions"
        )

    return ShardingPlan(
        max_shards=max_shards,
        workspace_inputs=workspace_inputs,
        external_resources=external_resources,
        shared_mutable_resource_exclusions=exclusions,
    )


def load_plan_contract(path: Path) -> tuple[dict[str, Any], ShardingPlan | None]:
    """Load the closed version-1 plan vocabulary and optional shard opt-in."""

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PlanContractError(f"could not read plan {path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise PlanContractError(f"{path} must be a JSON object")
    _refuse_unknown_keys(path, loaded, TOP_LEVEL_KEYS, "top-level")
    if loaded.get("schema_version") != SCHEMA_VERSION:
        raise PlanContractError(
            f"{path}: schema_version must be {SCHEMA_VERSION}, got "
            f"{loaded.get('schema_version')!r}"
        )
    entries = loaded.get("mutations")
    if not isinstance(entries, list) or not entries:
        raise PlanContractError(f"{path}: mutations must be a non-empty array")
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise PlanContractError(f"{path}: mutation {index} must be an object")
        _refuse_unknown_keys(path, entry, MUTATION_KEYS, f"mutation {index}")

    # Toolchain gates apply only to an actual opt-in. Existing serial plans stay
    # compatible, but cannot be made parallel by adding only max_shards.
    if "sharding" not in loaded:
        return loaded, None
    uses_go = _validate_mutation_commands(loaded)
    return loaded, _load_sharding_plan(path, loaded, uses_go=uses_go)


def validate_requested_shards(
    contract: ShardingPlan | None, requested_shards: int
) -> None:
    """Refuse invalid, non-opted, or over-limit parallel requests."""

    if (
        not isinstance(requested_shards, int)
        or isinstance(requested_shards, bool)
        or requested_shards < 1
    ):
        raise PlanContractError("requested shards must be an integer at least 1")
    if requested_shards == 1:
        return
    if contract is None:
        raise PlanContractError(
            "the plan does not opt in to sharding; add a reviewed sharding object"
        )
    if requested_shards > contract.max_shards:
        raise PlanContractError(
            f"requested {requested_shards} shards exceeds declared max_shards "
            f"{contract.max_shards}"
        )


def external_resource_environment(
    contract: ShardingPlan, run_id: str, shard_index: int
) -> dict[str, str]:
    """Return the required namespace for an isolated external-resource plan."""

    if contract.external_resources != "isolated":
        return {}
    if not _SAFE_ENV_COMPONENT.fullmatch(run_id):
        raise PlanContractError(f"run id {run_id!r} is not safe for an environment")
    if (
        not isinstance(shard_index, int)
        or isinstance(shard_index, bool)
        or shard_index < 0
    ):
        raise PlanContractError("shard index must be a non-negative integer")
    return {
        "MUTATION_HARNESS_RUN_ID": run_id,
        "MUTATION_HARNESS_SHARD_INDEX": str(shard_index),
    }
