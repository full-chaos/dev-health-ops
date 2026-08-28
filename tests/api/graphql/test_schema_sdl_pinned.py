"""Pin the exported GraphQL SDL against the checked-in canonical artifact.

CHAOS-4366 (Go API epic, plan §4/§6 Wave 0) requires the invariant
``Strawberry export == checked-in canonical SDL`` to be a CI-checked gate,
not a convention. ``contracts/graphql/v1/schema.graphql`` is the pin; this
test is what makes drift a hard failure via the standing
``ci/local_validate.sh`` full-suite run (root AGENTS.md's rule #4: "a
measurement that did not happen must FAIL, loudly" — this test runs inside
the unmarked pure-Python unit suite, so it cannot be silently skipped the
way an opt-in live-schema check could be).

Consumers of this pin:
- Web codegen (``dev-health/web/codegen.ts``) points its GraphQL Code
  Generator ``schema:`` at a copy of this file
  (``web/src/lib/graphql/schema.graphql``) and regenerates TypeScript types
  from it; web's own CI drift-checks that copy against a fresh export from
  this repo (``.github/workflows/live-e2e.yml`` in the web repo).
- ``query-api`` (Go, gqlgen, schema-first) takes this same file as its
  gqlgen input SDL (see ``docs/architecture/go-api/query-api.md``).

If this test fails, the SDL genuinely changed: regenerate the pin with

    PYTHONPATH=src .venv/bin/python -m dev_health_ops.api.graphql.export_schema \\
      --out contracts/graphql/v1/schema.graphql

review the diff, and commit it in the same PR as the schema change that
caused it -- and update the checked-in copy in ``dev-health/web`` in a
paired PR (schema drift across repos is exactly the risk this pin exists
to catch early, in this repo's own gate, rather than downstream in web's
optional live-e2e job).
"""

from __future__ import annotations

from pathlib import Path

from dev_health_ops.api.graphql.schema import schema

_PINNED_SDL_PATH = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "graphql"
    / "v1"
    / "schema.graphql"
)


def test_exported_sdl_matches_checked_in_pin() -> None:
    """Strawberry's live schema export must byte-for-byte match the pin."""
    assert _PINNED_SDL_PATH.exists(), (
        f"Canonical SDL pin missing at {_PINNED_SDL_PATH}. Generate it with "
        "`PYTHONPATH=src .venv/bin/python -m dev_health_ops.api.graphql."
        "export_schema --out contracts/graphql/v1/schema.graphql`."
    )

    pinned_sdl = _PINNED_SDL_PATH.read_text()
    live_sdl = schema.as_str()

    assert live_sdl == pinned_sdl, (
        "GraphQL schema drift detected: the live Strawberry schema export no "
        "longer matches contracts/graphql/v1/schema.graphql. Regenerate the "
        "pin (see this test's module docstring for the exact command), "
        "review the diff, and commit it alongside the resolver/type change "
        "that caused the drift. Do not edit the pin file by hand."
    )


def test_pinned_sdl_is_nonempty_and_well_formed() -> None:
    """Guard against a pin that was truncated or corrupted rather than regenerated.

    A byte-for-byte compare above would still "pass" if both the live
    export and the pin were empty/garbage for the same reason (e.g. schema
    import raised and export_schema silently produced an empty file some
    other way). Assert independently on shape.
    """
    pinned_sdl = _PINNED_SDL_PATH.read_text()
    assert len(pinned_sdl) > 1000, "Pinned SDL is suspiciously small"
    assert "type Query" in pinned_sdl
    assert "type Mutation" in pinned_sdl
