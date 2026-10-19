"""Shared body of cut-contents-pg-api-posture.py and cut-contents-pg-domain-posture.py (CHAOS-10113).

Reads a postgres posture function (RequiredTables {table, insert, update, delete}, SELECT implied, and
ColumnScoped {table, column, privilege}) at two git shas and writes every privilege ADDED between them
as an expectation line for pg-grants-check.sh:
  devhealth_<role> <table> <PRIV>[,<PRIV>]            (has_table_privilege read-back)
  devhealth_<role> <table>.<column> <PRIV>[,<PRIV>]   (has_column_privilege read-back)

Scope: an added-privileges read-back. It names what a cut grants and proves the grants are held; it cannot
see an unwanted extra (a table-wide UPDATE also makes has_column_privilege true). Exactness is the readiness
gate's job (postgres.CheckRolePosture / CheckAPIAuthorization).
"""

import os
import re
import subprocess
import sys
from pathlib import Path


def ops_repo():
    """BIGBOY_OPS_REPO, else the checkout these tools come from; never the operator checkout (CHAOS-10137)."""
    env = os.environ.get("BIGBOY_OPS_REPO")
    if env:
        return env
    here = Path(__file__).resolve().parents[2]
    return str(here) if (here / ".git").exists() else None


PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES")


def load(sha, gofile, func):
    """(tables, columns) of posture `func` in `gofile` at `sha`, or None when it cannot be read."""
    repo = ops_repo()
    if repo is None:
        return None
    try:
        src = subprocess.check_output(
            [
                "git",
                "-C",
                repo,
                "show",
                f"{sha}:internal/storage/postgres/{gofile}",
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
        body = src.split(f"func {func}()", 1)[1].split("\nfunc ", 1)[0]
    except (subprocess.CalledProcessError, OSError, IndexError):
        return None
    tables = {}
    for m in re.finditer(
        r'\{"(\w+)", (true|false), (true|false), (true|false)\}', body
    ):
        tables[m.group(1)] = {
            "SELECT": True,
            "INSERT": m.group(2) == "true",
            "UPDATE": m.group(3) == "true",
            "DELETE": m.group(4) == "true",
        }
    columns = {}
    for m in re.finditer(
        r'\{"(\w+)", "(\w+)", "(SELECT|INSERT|UPDATE|REFERENCES)"\}', body
    ):
        columns.setdefault(f"{m.group(1)}.{m.group(2)}", set()).add(m.group(3))
    return tables, columns


def delta(role, gofile, func, unparsable):
    """Print the added privileges and append their expectation lines to the file named by argv[3]."""
    old, new, outf = sys.argv[1:4]
    before, after = load(old, gofile, func), load(new, gofile, func)
    if before is None or after is None:
        print(unparsable)
        sys.exit(1)
    lines = []
    for table, privs in sorted(after[0].items()):
        had = before[0].get(table, {})
        added = [p for p in PRIVILEGES if privs.get(p) and not had.get(p)]
        if added:
            lines.append(f"devhealth_{role} {table} {','.join(added)}")
    for column, privs in sorted(after[1].items()):
        had = before[1].get(column, set())
        added = [p for p in PRIVILEGES if p in privs and p not in had]
        if added:
            lines.append(f"devhealth_{role} {column} {','.join(added)}")
    prefix = f"devhealth_{role} "
    print(
        f"PG {role} posture delta (privileges added): "
        + "; ".join(line[len(prefix) :] for line in lines)
    )
    with open(outf, "a") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))
