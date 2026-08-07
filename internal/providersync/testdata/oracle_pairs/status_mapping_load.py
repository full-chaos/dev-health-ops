"""Oracle pair `status/mapping/load` -- the loader boundary.

Compares the FOUR index maps `load_status_mapping` builds, in full, against the
Go port's. Reflecting the frozen StatusMapping dataclass rather than naming the
maps by hand means a fifth index added upstream fails every case here until the
Go struct and this row grow it too.

Comparing at the INDEX rather than only through a normalize call is deliberate:
loader-level divergences (base-then-override last-wins, `_index_values`
last-wins, the silent skip of an unrecognised category, `_norm_key`'s internal
whitespace collapse, `str(raw)` on a non-string YAML node, and the omission of
the `linear` provider) are visible here directly, instead of only where some
normalize case happens to probe the affected key.
"""

from __future__ import annotations

import contextlib
import io
import os
import pathlib
from collections.abc import Iterator, Mapping
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dataclass_field_names
from internal.providersync.testdata.python_oracle_loader import load_live_module

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_STATUS_MAPPING_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/status_mapping.py"
_REAL_CONFIG = REPO_ROOT / "src/dev_health_ops/config/status_mapping.yaml"
_SYNTHETIC_CONFIG_DIR = (
    REPO_ROOT / "internal/providersync/testdata/status_mapping_configs"
)

_ENV_VAR = "STATUS_MAPPING_PATH"


def _reflected_fields() -> frozenset[str]:
    return dataclass_field_names(_STATUS_MAPPING_SOURCE.read_text(), "StatusMapping")


def resolve_config(name: str | None) -> str | None:
    """Resolve a case's config NAME to a path.

    Both engines resolve the same name through the same rule (the Go side's
    resolveStatusMappingConfig mirrors this exactly), so a case names a config
    and neither side can quietly read a different file.
    """
    if name is None:
        return None
    if name == "real":
        return str(_REAL_CONFIG)
    return str(_SYNTHETIC_CONFIG_DIR / f"{name}.yaml")


@contextlib.contextmanager
def status_mapping_path_env(value: str | None) -> Iterator[None]:
    """Set STATUS_MAPPING_PATH to `value`, or REMOVE it when value is None.

    Removing it is not optional bookkeeping. This oracle runs as a standalone
    process, outside the pytest env-neutralization fixture that normally scrubs
    the variable, so an ambient STATUS_MAPPING_PATH inherited from the caller's
    shell would silently redirect every "explicit path" case at the same file
    and make the whole pair pass vacuously.
    """
    previous = os.environ.get(_ENV_VAR)
    try:
        if value is None:
            os.environ.pop(_ENV_VAR, None)
        else:
            os.environ[_ENV_VAR] = value
        yield
    finally:
        if previous is None:
            os.environ.pop(_ENV_VAR, None)
        else:
            os.environ[_ENV_VAR] = previous


def load_mapping_for_case(case: dict[str, Any]) -> Any:
    """Load via the REAL production entry point under the case's env/path setup.

    stdout AND stderr are both captured: any byte production code writes to
    either stream lands in the middle of this process's single JSON document and
    breaks the decode on the Go side. `load_status_mapping` pointed at a bad
    path is one of the paths that can emit.
    """
    module = load_live_module(_STATUS_MAPPING_SOURCE)
    explicit = resolve_config(case.get("explicit_config"))
    env_value = resolve_config(case.get("env_config"))
    sink = io.StringIO()
    with status_mapping_path_env(env_value):
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            return module.load_status_mapping(
                pathlib.Path(explicit) if explicit is not None else None
            )


def _plain(index: Mapping[str, Mapping[str, str]]) -> dict[str, dict[str, str]]:
    return {provider: dict(entries) for provider, entries in index.items()}


# The value both engines report for the D20 timestamp divergence. It is
# deliberately DIFFERENT on each side and therefore declared in excluded_fields:
# see the registration below for why a differential case cannot express it.
D20_PYTHON_BEHAVIOUR = "python:resolves-timestamp-and-str()s-it"


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    """Build one row, carrying the PHASE and exception type when Python raises.

    A config Python REJECTS and a config Python merely finds empty are wildly
    different outcomes that a bare "four empty maps" row would render
    identical. Carrying `outcome` as "<phase>:<ExceptionType>" makes an error
    that moves phases, or changes class, compare unequal -- which is the whole
    point of pinning the structural cases at all.
    """
    try:
        mapping = load_mapping_for_case(case)
    except Exception as exc:  # noqa: BLE001 -- the exception IS the observation
        return {
            "outcome": f"load:{type(exc).__name__}",
            "status_by_provider": None,
            "label_status_by_provider": None,
            "type_by_provider": None,
            "label_type_by_provider": None,
            "d20_timestamp_probe": D20_PYTHON_BEHAVIOUR,
        }
    return {
        "outcome": "ok",
        "status_by_provider": _plain(mapping.status_by_provider),
        "label_status_by_provider": _plain(mapping.label_status_by_provider),
        "type_by_provider": _plain(mapping.type_by_provider),
        "label_type_by_provider": _plain(mapping.label_type_by_provider),
        "d20_timestamp_probe": D20_PYTHON_BEHAVIOUR,
    }


oracle_registry.register(
    oracle_registry.PairSpec(
        id="status/mapping/load",
        build_row=_build_row,
        reflected_fields=_reflected_fields,
        excluded_fields={
            "d20_timestamp_probe": (
                "Decision Log D20, declared divergence: PyYAML resolves a plain "
                "`2026-01-01` scalar to a datetime.date and str()s it into an "
                "index key, while the Go port REFUSES LOUDLY rather than "
                "carrying a second, unversioned date formatter. The two sides "
                "therefore report different values here BY DESIGN, which is "
                "exactly why this is a declared exclusion and not a comparison: "
                "a differential case cannot both diverge and pass. The "
                "divergence is asserted, not merely described -- "
                "TestTimestampScalarIsDeclaredDivergenceD20 fails if Go ever "
                "stops erroring on testdata/status_mapping_configs/"
                "timestamp_d20.yaml, and this field records what Python does "
                "in every proof run so the claim stays visible on both sides."
            ),
        },
    )
)
