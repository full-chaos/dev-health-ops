"""Export GraphQL schema as SDL.

Usage:
    python3 -m dev_health_ops.api.graphql.export_schema [--out <path>]

If --out is provided, writes SDL to the given file path.
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
