import glob
import os
import sys

out = sys.argv[1]
IGN = {
    "date",
    "server",
    "x-request-id",
    "transfer-encoding",
    "content-length",
    "connection",
    "x-dev-health-plane",
    "x-dev-health-build",
}
labels = sorted(
    {os.path.basename(f).split(".")[0] for f in glob.glob(out + "/*.headers.raw")}
)


def hdrs(f):
    ls = open(f, errors="replace").read().replace("\r", "").split("\n")
    st = ls[0].split(" ", 2)[1] if ls and " " in ls[0] else "-"
    h = []
    for line in ls[1:]:
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip().lower()
        if k in IGN or k.startswith("x-dev-health-"):
            continue
        h.append((k, v.strip()))
    return st, sorted(h)


print("route | mode | status go/py | body identical | headers identical | header diff")
bad = 0
for label in labels:
    for mode in ("noorigin", "origin"):
        if not os.path.exists(f"{out}/{label}.go.{mode}.headers.raw"):
            continue
        g = hdrs(f"{out}/{label}.go.{mode}.headers.raw")
        p = hdrs(f"{out}/{label}.python.{mode}.headers.raw")
        bg = open(f"{out}/{label}.go.{mode}.body.raw", "rb").read()
        bp = open(f"{out}/{label}.python.{mode}.body.raw", "rb").read()
        gs, ps = set(g[1]), set(p[1])
        diff = (
            ";".join(
                sorted(
                    ["-go:" + k for k, _ in gs - ps] + ["+py:" + k for k, _ in ps - gs]
                )
            )
            or "-"
        )
        note = ""
        if "install-url" in label:
            note = " (status-only: signed state differs per call)"
        if label.startswith("a-gap-"):
            note = " (EXPECTED-GAP: Go 404, Python 200)"
        print(
            f"{label} | {mode} | {g[0]}/{p[0]} | {bg == bp} | {g[1] == p[1]} | {diff}{note}"
        )
