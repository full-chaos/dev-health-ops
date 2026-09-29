#!/usr/bin/env python3
"""render-dho-api-ch-users.py <rendered-release.yaml> <out-file> -- CHAOS-7162.

Writes ClickHouse's users.d declaration of dho_api_ch for bigboy from a rendered release's
`...clickhouse-usersd` ConfigMap (key dho_api_ch.xml: the grants are the posture manifest's, so they
track the ops sha the release was rendered from), with <password_sha256_hex> replaced by the SHA-256 of the
password in the environment variable API_CH_PASSWORD. The password is read from the environment only (never
argv), is never printed, and the output holds only its hash; the file is written 0644 because
clickhouse-server runs as uid 101 in its container and exits on a mounted users.d file it cannot read.
Prints names and counts only.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import yaml


def _load_validate() -> Callable[[str], None]:
    """The shape module is a sibling file, loaded by path so the script runs from any directory."""
    path = Path(__file__).resolve().parent / "dho_api_ch_users_shape.py"
    spec = importlib.util.spec_from_file_location("dho_api_ch_users_shape", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    validator: Callable[[str], None] = module.validate
    return validator


validate = _load_validate()

KEY = "dho_api_ch.xml"
HASH = re.compile(r"<password_sha256_hex>[0-9a-f]*</password_sha256_hex>")


def render(rendered_release: Path, password: str) -> str:
    for doc in yaml.safe_load_all(rendered_release.read_text()):
        if (
            doc
            and doc.get("kind") == "ConfigMap"
            and doc["metadata"]["name"].endswith("clickhouse-usersd")
            and KEY in (doc.get("data") or {})
        ):
            xml = doc["data"][KEY]
            # Refuse first: a source that carries a plaintext <password> (or any authentication element but the
            # hash) must never be copied into a world-readable file, so it is checked BEFORE the hash is set.
            hashed = HASH.sub(
                "<password_sha256_hex>" + "0" * 64 + "</password_sha256_hex>", xml
            )
            try:
                validate(hashed)
            except ValueError as error:
                raise SystemExit(
                    f"{KEY} in {rendered_release} is refused, nothing written: {error}"
                ) from None
            digest = hashlib.sha256(password.encode()).hexdigest()
            rendered, _ = HASH.subn(
                f"<password_sha256_hex>{digest}</password_sha256_hex>", xml
            )
            validate(rendered)
            return rendered
    raise SystemExit(
        f"no clickhouse-usersd ConfigMap with key {KEY} in {rendered_release}"
    )


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "usage: render-dho-api-ch-users.py <rendered-release.yaml> <out-file>",
            file=sys.stderr,
        )
        return 2
    password = os.environ.get("API_CH_PASSWORD", "")
    if not password:
        print("API_CH_PASSWORD is not set in the environment", file=sys.stderr)
        return 2
    xml = render(Path(argv[1]), password)
    out = Path(argv[2])
    fd, tmp = tempfile.mkstemp(dir=out.parent, prefix=f".{out.name}.")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(xml)
        os.chmod(tmp, 0o644)
        os.replace(tmp, out)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    print(f"wrote {out} mode=644 grants={xml.count('<query>')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
