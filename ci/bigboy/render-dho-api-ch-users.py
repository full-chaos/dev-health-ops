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
import os
import re
import sys
import tempfile
from pathlib import Path

import yaml

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
            digest = hashlib.sha256(password.encode()).hexdigest()
            rendered, count = HASH.subn(
                f"<password_sha256_hex>{digest}</password_sha256_hex>", xml
            )
            if count != 1 or "<dho_api_ch>" not in rendered:
                raise SystemExit(
                    f"{KEY} must declare <dho_api_ch> with exactly one <password_sha256_hex> (found {count})"
                )
            return rendered
    raise SystemExit(f"no clickhouse-usersd ConfigMap with key {KEY} in {rendered_release}")


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: render-dho-api-ch-users.py <rendered-release.yaml> <out-file>", file=sys.stderr)
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
