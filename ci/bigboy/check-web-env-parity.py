#!/usr/bin/env python3
"""CHAOS-6987 (D2736): web env-name parity between prod and bigboy -- names only.

Prod's web gets its env from deploy values.prod.yaml `ops.web.env` + `ops.web.extraEnv`.
Bigboy's web is compose-defined; its bigboy-specific values live in
ci/bigboy/compose.bigboy.router.yml. D2736 found AUTH_URL absent on bigboy (logout
redirected to the Node bind address). Per the lead's ruling this stays hand-maintained
(one name does not earn a generator), so this check makes the hand list LOUD:

  * every prod web name must be CLASSIFIED below -- a new prod name fails, named;
  * every `overlay` name must be set in compose.bigboy.router.yml's web.environment;
  * every classified name must still exist in prod -- a stale entry fails, named;
  * with --live-names FILE (output of container-env-names.sh for the web container),
    every prod web name must be present in the running container.

Only NAMES are compared. Values are never read from the running stack. The two
overlay values checked here are public URLs committed in the overlay file itself.

Usage: check-web-env-parity.py <values.prod.yaml> <compose.bigboy.router.yml> [--live-names FILE]
Exit: 0 parity; 1 drift (each finding on stderr as `DRIFT: ...`); 2 usage/shape error.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

# name -> "overlay" (bigboy value set in compose.bigboy.router.yml) or "base: <reason>"
# (bigboy's own compose.yml sets a venue-local value on purpose).
CLASSIFIED: dict[str, str] = {
    "BACKEND_URL": "overlay",
    "AUTH_URL": "overlay",
    "TRUST_PROXY": "overlay",
    "TRUSTED_PROXY_HOPS": "overlay",
    "ACR_API_ORIGIN": "base: compose.yml sets venue-local ACR wiring; ACR is not a bigboy parity target",
    "ACR_WEB_ASSERTION_ISSUER": "base: venue-local ACR wiring (compose.yml)",
    "ACR_WEB_ASSERTION_AUDIENCE": "base: venue-local ACR wiring (compose.yml)",
    "ACR_WEB_ASSERTION_KID": "base: venue-local ACR wiring (compose.yml)",
    "ACR_WEB_ASSERTION_KEY_FILE": "base: venue-local ACR wiring (compose.yml)",
}

# Public, non-secret values the overlay must carry (D2724 router target, D2736 origin).
EXPECTED_OVERLAY_VALUES: dict[str, str] = {
    "BACKEND_URL": "http://traefik:3000",
    "AUTH_URL": "https://www.commanderkeen.dev",
    # CHAOS-7205 (deploy #318): web derives the rate-limit client IP from the
    # TRUSTED_PROXY_HOPS-th X-Forwarded-For entry from the right. Bigboy's one trusted proxy
    # is traefik, which appends the peer it saw, so the same literals prod uses hold here.
    "TRUST_PROXY": "true",
    "TRUSTED_PROXY_HOPS": "1",
}


def prod_web_names(values_path: str) -> list[str]:
    doc = yaml.safe_load(Path(values_path).read_text(encoding="utf-8"))
    try:
        web = doc["ops"]["web"]
    except (KeyError, TypeError):
        raise SystemExit(
            "check-web-env-parity: values has no ops.web block (shape changed?)"
        ) from None
    names: list[str] = []
    env = web.get("env") or {}
    if isinstance(env, dict):
        names.extend(env)
    else:
        names.extend(e["name"] for e in env)
    names.extend(e["name"] for e in (web.get("extraEnv") or []))
    if not names:
        raise SystemExit(
            "check-web-env-parity: prod web has ZERO env names -- refusing to pass on an empty set"
        )
    return names


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that accepts compose merge tags (!override, !reset) as plain nodes."""


def _untagged(loader: yaml.SafeLoader, _suffix: str, node: yaml.Node) -> object:
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    raise yaml.constructor.ConstructorError(
        None, None, f"unsupported tagged node {node.tag}", node.start_mark
    )


_ComposeLoader.add_multi_constructor("!", _untagged)


def load_compose_yaml(text: str) -> Any:
    loader = _ComposeLoader(text)
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


def overlay_web_env(overlay_path: str) -> dict[str, str]:
    doc = load_compose_yaml(Path(overlay_path).read_text(encoding="utf-8"))
    env = (((doc or {}).get("services") or {}).get("web") or {}).get(
        "environment"
    ) or {}
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env)
    return {str(k): str(v) for k, v in env.items()}


def check(
    values_path: str, overlay_path: str, live_names: set[str] | None
) -> list[str]:
    findings: list[str] = []
    prod = prod_web_names(values_path)
    overlay = overlay_web_env(overlay_path)
    for name in prod:
        if name not in CLASSIFIED:
            findings.append(
                f"prod web env name {name} is not classified -- decide overlay vs base in "
                f"check-web-env-parity.py and set it on bigboy if it applies"
            )
    for name, cls in CLASSIFIED.items():
        if name not in prod:
            findings.append(
                f"classified name {name} no longer exists in prod web env -- drop it from CLASSIFIED"
            )
        if cls == "overlay" and name not in overlay:
            findings.append(
                f"overlay name {name} missing from compose.bigboy.router.yml web.environment"
            )
    for name, want in EXPECTED_OVERLAY_VALUES.items():
        if name in overlay and overlay[name] != want:
            findings.append(f"overlay {name} is not the expected public value {want}")
    if live_names is not None:
        for name in prod:
            if name not in live_names:
                findings.append(f"running web container has no env var named {name}")
    return findings


def main(argv: list[str]) -> int:
    args = list(argv)
    live: set[str] | None = None
    if "--live-names" in args:
        i = args.index("--live-names")
        if i + 1 >= len(args):
            print("usage: --live-names needs a file", file=sys.stderr)
            return 2
        live = {
            ln.strip()
            for ln in Path(args[i + 1]).read_text(encoding="utf-8").splitlines()
            if ln.strip()
        }
        if not live:
            print(
                "check-web-env-parity: --live-names file is empty -- a names read that did not happen is not a pass",
                file=sys.stderr,
            )
            return 2
        del args[i : i + 2]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    findings = check(args[0], args[1], live)
    for f in findings:
        print(f"DRIFT: {f}", file=sys.stderr)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
