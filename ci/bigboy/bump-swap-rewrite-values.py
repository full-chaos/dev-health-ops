"""bump-swap.sh's values.prod.yaml/values.local.yaml sha rewrite, extracted per CHAOS-3362/CHAOS-6964
(see cut-contents-pg-api-posture.py's doc comment for the full heredoc-pipe-budget rationale: this
body is over the 400-byte here-document pipe budget).

Usage: bump-swap-rewrite-values.py <old-sha> <new-sha> <o7> <s7> <old-api-digest> <new-api-digest>
       <old-operator-digest> <new-operator-digest> <old-dho-digest> <new-dho-digest> <dho-arm-child-digest>
Run from the deploy worktree (cwd) bump-swap.sh already cd'd into.
"""

import re
import sys

old, new, o7, s7, oa, na, oo, no, od, nd, arm = sys.argv[1:]
p = "values.prod.yaml"
with open(p) as f:
    s = f.read()
for a, b in ((old, new), (o7, s7), (oa, na), (oo, no), (od, nd)):
    assert a in s, a
    s = s.replace(a, b)
# arm64 child comment in the queryApi block
s, n = re.subn(
    r"(The arm64 child \(prod's own arch\) is\n\s+# sha256:)[0-9a-f]{64}",
    lambda m: m.group(1) + arm.split(":")[1],
    s,
)
assert n == 1, "arm64 child comment not found"
with open(p, "w") as f:
    f.write(s)
q = "values.local.yaml"
with open(q) as f:
    t = f.read()
assert old[:12] in t and od in t
with open(q, "w") as f:
    f.write(t.replace(old[:12], new[:12]).replace(od, nd))
