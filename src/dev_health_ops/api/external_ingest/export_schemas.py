"""Export the external-ingest JSON Schema bundle as a static artifact.

Usage:
    python3 -m dev_health_ops.api.external_ingest.export_schemas [--out <path>]

Mirrors ``api/graphql/export_schema.py``'s pattern (argparse, ``--out``,
stdout fallback): lets customers/CI vendor a schema file for offline
``ajv``/``jsonschema`` validation, gives docs a stable linkable artifact,
and is a stable machine contract co-located with its generator. It is compared
byte-for-byte against the committed source-owned
``src/dev_health_ops/api/external_ingest/schema.json`` by
``tests/api/external_ingest/test_schema_export_no_drift.py`` at PR time
(brief D7) — deliberately a pytest, not a new CI workflow.

Deviation from the GraphQL precedent: ``main()`` runs under an
``if __name__ == "__main__":`` guard rather than unconditionally at module
scope, so ``render_schema_json`` can be imported (by the no-drift test)
without triggering argparse against pytest's own argv.
"""

from __future__ import annotations

import argparse
import json
import sys

from .schema_registry import SUPPORTED_SCHEMA_VERSIONS, get_bundle

DEFAULT_SCHEMA_VERSION = SUPPORTED_SCHEMA_VERSIONS[0]


def render_schema_json(schema_version: str = DEFAULT_SCHEMA_VERSION) -> str:
    bundle = get_bundle(schema_version)
    if bundle is None:
        raise KeyError(f"Unknown schema version: {schema_version!r}")
    return json.dumps(bundle.document, indent=2, sort_keys=True) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export the external-ingest JSON Schema bundle"
    )
    parser.add_argument(
        "--out", metavar="PATH", help="Write JSON Schema to file instead of stdout"
    )
    parser.add_argument(
        "--schema-version",
        default=DEFAULT_SCHEMA_VERSION,
        help=f"Schema version to export (default: {DEFAULT_SCHEMA_VERSION})",
    )
    args = parser.parse_args()

    text = render_schema_json(args.schema_version)

    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
