"""cut-contents.sh's PG domain posture delta computation, extracted per CHAOS-3362/CHAOS-6964 (see
cut-contents-pg-api-posture.py's own doc comment for the full heredoc-pipe-budget rationale).

Usage: cut-contents-pg-domain-posture.py <old-sha> <new-sha> <pg-expect-out-file>
Same struct shape as the api posture ({table, insert, update, delete}, SELECT implied): expectations
for devhealth_domain.
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
                f"{sha}:internal/storage/postgres/domain_authorization.go",
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
    except Exception:
        return None
    body = src.split("func domainPosture()", 1)[1].split("\nfunc ", 1)[0]
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
    print("PG_DOMAIN_POSTURE_DELTA unparsable")
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
        lines.append(f"devhealth_domain {tbl} {','.join(add)}")
print(
    "PG domain posture delta (privileges added): "
    + "; ".join(line[len("devhealth_domain ") :] for line in lines)
)
with open(outf, "a") as f:
    f.write("\n".join(lines) + ("\n" if lines else ""))
