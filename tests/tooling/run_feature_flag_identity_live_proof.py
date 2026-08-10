"""Run the feature-flag registry proof against an isolated ClickHouse database."""

from __future__ import annotations

import os
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import clickhouse_connect


def main() -> int:
    admin_dsn = os.environ.get(
        "CLICKHOUSE_ADMIN_URI",
        "clickhouse://ch:ch@localhost:8123/default",
    )
    parsed = urlsplit(admin_dsn)
    database = f"chaos_3703_feature_flag_{uuid4().hex}"
    scratch_dsn = urlunsplit(parsed._replace(path=f"/{database}"))
    client = clickhouse_connect.get_client(dsn=admin_dsn)
    client.command(f"CREATE DATABASE `{database}`")
    try:
        environment = os.environ.copy()
        environment["CLICKHOUSE_URI"] = scratch_dsn
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "tests/graphql/test_feature_flags_live.py",
                "-m",
                "clickhouse",
            ],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
        )
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        if result.returncode != 0:
            return result.returncode
        if "1 passed" not in result.stdout:
            print("feature-flag live proof did not execute exactly one passing test")
            return 1
        return 0
    finally:
        client.command(f"DROP DATABASE IF EXISTS `{database}`")
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
