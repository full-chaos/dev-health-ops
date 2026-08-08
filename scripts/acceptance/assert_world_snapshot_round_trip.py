#!/usr/bin/env python3
"""CHAOS-3463: prove the world snapshot round trip is lossless.

Runs INSIDE the acceptance ``api`` container, as the last step of
``mint_ask_dev_world_snapshot.sh``. Compares the ``WORLD_DIGEST`` computed
from the SCRATCH database the world was generated in against the one
re-minted from the RESTORED serving database.

If those two differ, the snapshot dropped or altered something the digest
covers -- and because the pin is minted from the restored side, the
difference would then be invisible forever after: every boot would restore
the same lossy bytes and "verify" them against a pin minted from the same
lossy bytes. Asserting equality here is what stops a lossy round trip from
being baked into the pin.

A separate FILE rather than an inline ``python -c``/heredoc in the shell
script on purpose: ``docker compose exec -T api python -`` fed from a
heredoc hangs when the calling script runs non-interactively (observed --
the mint sat on this step indefinitely), and the ``python -c "$(cat <<'PY'
… PY)"`` rewrite is fragile across shells. A file has neither problem and
can be linted and type-checked like the rest of the tree.
"""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--generated",
        required=True,
        help="WORLD_DIGEST written by `fixtures world` against the scratch DB.",
    )
    parser.add_argument(
        "--restored",
        required=True,
        help="WORLD_DIGEST re-minted by `fixtures world-restore --mint-digest`.",
    )
    args = parser.parse_args(argv)

    with open(args.generated, encoding="utf-8") as handle:
        generated = json.load(handle)["digest"]
    with open(args.restored, encoding="utf-8") as handle:
        restored = json.load(handle)["digest"]

    print(f"generated digest = {generated}")
    print(f"restored  digest = {restored}")
    if generated != restored:
        print(
            "mint: FAILED -- the restored world does not hash identically to "
            "the generated one, so the snapshot round trip is lossy. Do NOT "
            "commit this artifact; fix the codec/format first.",
            file=sys.stderr,
        )
        return 1
    print("mint: round trip is lossless")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
