"""Export GraphQL schema as SDL.

Usage:
    python3 -m dev_health_ops.api.graphql.export_schema [--out <path>]

If --out is provided, writes SDL to the given file path.
Note: The --out path is primarily used by CI pipelines (e.g. live-e2e schema drift checks)
to export the current SDL for comparison against stored schema files. In normal development,
stdout is convenient for quick inspection.
Otherwise, prints SDL to stdout.
"""

import argparse
import sys

from .schema import schema


def main() -> None:
    parser = argparse.ArgumentParser(description="Export GraphQL schema as SDL")
    parser.add_argument(
        "--out", metavar="PATH", help="Write SDL to file instead of stdout"
    )
    args = parser.parse_args()

    sdl = schema.as_str()

    if args.out:
        with open(args.out, "w") as f:
            f.write(sdl)
    else:
        sys.stdout.write(sdl)


main()
