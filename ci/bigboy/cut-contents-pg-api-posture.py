"""cut-contents.sh's PG api posture delta computation, extracted per CHAOS-3362/CHAOS-6964: this
body is over the 400-byte here-document pipe budget (bash writes a here-document into a pipe and
only then forks the reader; a document that does not fit the pipe buffer hangs the writer forever --
see tests/tooling/test_local_validate_heredocs.py's own doc comment for the full forensics). Read
from disk instead of piped.

Usage: cut-contents-pg-api-posture.py <old-sha> <new-sha> <pg-expect-out-file>
Computed from the manifest itself (RequiredTables {table, insert, update, delete}; SELECT implied):
every privilege ADDED between old and new for the api role becomes an expectation
`devhealth_api <table> <PRIV>` (has_table_privilege read-back). Replaces by-name item matching for
PG api grants.
"""

import re
import subprocess
import sys

old, new, outf = sys.argv[1:4]


def load(sha):
    try:
        src = subprocess.check_output(
            [
                "git",
                "-C",
                "/home/ubuntu/devhealth/ops",
                "show",
                f"{sha}:internal/storage/postgres/api_authorization.go",
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
    except Exception:
        return None
    body = src.split("func apiPosture()", 1)[1].split("\nfunc ", 1)[0]
    t = {}
    for m in re.finditer(
        r'\{"(\w+)", (true|false), (true|false), (true|false)\}', body
    ):
        t[m.group(1)] = {
            "SELECT": True,
            "INSERT": m.group(2) == "true",
            "UPDATE": m.group(3) == "true",
            "DELETE": m.group(4) == "true",
        }
    return t


o, n = load(old), load(new)
if o is None or n is None:
    print("PG_POSTURE_DELTA unparsable (manifest missing at a sha)")
    sys.exit(0)
lines = []
for tbl, privs in sorted(n.items()):
    before = o.get(tbl, {})
    add = [
        p
        for p in ("SELECT", "INSERT", "UPDATE", "DELETE")
        if privs[p] and not before.get(p)
    ]
    if add:
        lines.append(f"devhealth_api {tbl} {','.join(add)}")
print(
    "PG api posture delta (privileges added): "
    + "; ".join(line[len("devhealth_api ") :] for line in lines)
)
with open(outf, "a") as f:
    f.write("\n".join(lines) + ("\n" if lines else ""))
