"""bigboy-grants-check.sh's ClickHouse-grants-vs-APIPosture-manifest comparison, extracted per
CHAOS-3362/CHAOS-6964 (see cut-contents-pg-api-posture.py's doc comment for the full heredoc-pipe-
budget rationale: this body is over the 400-byte here-document pipe budget).

Usage: bigboy-grants-check-compare.py <actual-grants-tsv> <authorization.go source at sha>
Prints bigboy_grants_check summary + GRANTS_CHECK_OK/GRANTS_CHECK_FAIL, exit 0 on match else 1.
"""

import re
import sys

act_raw, src = sys.argv[1:3]
body = src.split("func APIPosture(", 1)[1].split("\n}\n", 1)[0]
exp = set()
for line in body.splitlines():
    m = re.search(r'\{Database: database, Table: "(\w+)",([^}]*)\}', line)
    if not m:
        continue
    for f, n in (
        ("AllowSelect", "SELECT"),
        ("AllowInsert", "INSERT"),
        ("AllowDelete", "ALTER DELETE"),
    ):
        if f"{f}: true" in m.group(2):
            exp.add((n, m.group(1)))
if not exp:
    print("GRANTS_CHECK_FAIL manifest not parsed")
    sys.exit(1)
act = {tuple(line.split("\t")) for line in act_raw.splitlines() if "\t" in line}
mi, ex = sorted(exp - act), sorted(act - exp)
print(
    f"bigboy_grants_check expected={len(exp)} actual={len(act)} missing={mi} extra={ex}"
)
print("GRANTS_CHECK_OK" if not mi and not ex else "GRANTS_CHECK_FAIL")
sys.exit(0 if not mi and not ex else 1)
