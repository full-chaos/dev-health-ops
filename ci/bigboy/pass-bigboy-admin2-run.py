import http.client
import json
import re
import sys

# CHAOS-8369 (D4566 class rules): the admin proof token arrives on STDIN (first line), never in argv or
# the environment of this process; results are held in memory and leave as ONE framed stream on stdout
# (no file is written in the container); progress goes to stderr. Frame grammar, read by
# pass-bigboy-admin2-unpack.py: "@@FILE\t<name>\t<nbytes>\n<bytes>\n" per file, then "@@END\t<nfiles>\t<status>\n".
# A stream with no END frame is a failed measurement on the host side.
ORIGIN = "http://localhost:3000"
ADMIN = sys.stdin.readline().strip()
if not ADMIN:
    print("ABORT: no admin proof token on stdin", file=sys.stderr, flush=True)
    sys.exit(4)
FILES = {}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def emit(status):
    w = sys.stdout.buffer
    for name in sorted(FILES):
        w.write(f"@@FILE\t{name}\t{len(FILES[name])}\n".encode() + FILES[name] + b"\n")
    w.write(f"@@END\t{len(FILES)}\t{status}\n".encode())
    w.flush()


Z = "00000000-0000-4000-8000-000000000000"
PLANES = {"go": ("go-api", 8000), "python": ("localhost", 8000)}


def call(plane, method, path, body=None, origin=False):
    host, port = PLANES[plane]
    h = {"Authorization": "Bearer " + ADMIN}
    if origin:
        h["Origin"] = ORIGIN
    if body is not None:
        h["Content-Type"] = "application/json"
    c = http.client.HTTPConnection(host, port, timeout=25)
    try:
        c.request(method, path, body=body, headers=h)
        r = c.getresponse()
        data = r.read()
        hd = (
            f"HTTP/1.1 {r.status} {r.reason}\r\n"
            + "".join(f"{k}: {v}\r\n" for k, v in r.getheaders())
            + "\r\n"
        )
        return r.status, hd, data
    except Exception as e:
        return 0, f"HTTP/1.1 000 ERR {type(e).__name__}\r\n\r\n", b""


def save(label, plane, mode, hd, data):
    base = f"{label}.{plane}.{mode}"
    FILES[base + ".headers.raw"] = hd.encode()
    FILES[base + ".body.raw"] = data


def get(p, plane="go"):
    st, _, b = call(plane, "GET", p)
    try:
        return st, json.loads(b)
    except Exception:
        return st, None


def items(d, *keys):
    if isinstance(d, dict):
        for k in keys + ("items", "data"):
            if isinstance(d.get(k), list):
                return d[k]
        return []
    return d if isinstance(d, list) else []


A = "/api/v1/admin/"
# ---- discovery (read-only, from the Go plane) ----
_, cats = get(A + "settings/categories")
cn = [
    c if isinstance(c, str) else (c.get("category") or c.get("name"))
    for c in items(cats, "categories")
]
CAT = cn[0] if cn else None
KEY = None
if CAT:
    _, cd = get(A + "settings/" + CAT)
    sl = items(cd, "settings")
    if sl:
        KEY = sl[0].get("key") if isinstance(sl[0], dict) else sl[0]
_, u = get(A + "users")
ul = items(u, "users")
UID = (ul[0].get("id") or ul[0].get("user_id")) if ul else Z
_, cr = get(A + "credentials")
cl = items(cr, "credentials")
CRED = (
    ("credentials/{}/{}".format(cl[0].get("provider"), cl[0].get("name")))
    if cl
    else None
)
_, al = get(A + "audit-logs")
ai = items(al)
AID = ai[0].get("id") if ai else Z
_, sc = get(A + "sync-configs")
scl = items(sc, "configs")
SCID = scl[0].get("id") if scl else None
_, cs = get(A + "customer-push/sources")
csl = items(cs, "sources")
CSID = csl[0].get("id") if csl else None
_, bj = get(A + "backfill-jobs")
bl = items(bj)
BID = bl[0].get("id") if bl else None
_, me = get("/api/v1/orgs/me")
ORGID = (
    (me or {}).get("id")
    or (me or {}).get("org_id")
    or "67f1add8-9fcb-4272-addb-044b70c442c8"
)
log(
    f"DISCOVER cats={cn} key={KEY} users={len(ul)} creds={len(cl)} audit={len(ai)} "
    f"sync_configs={len(scl)} cp_sources={len(csl)} backfill={len(bl)} org={bool(ORGID)}"
)
# ---- Part A: reads ----
reads = [
    "audit-logs",
    "audit-logs/" + Z,
    f"audit-logs/{AID}",
    "audit-logs/resource/user/" + Z,
    "audit-logs/user/" + Z,
    "ip-allowlist",
    "ip-allowlist/" + Z,
    "retention-policies",
    "retention-policies/" + Z,
    "impersonate/status",
    "users",
    "users/" + Z,
    f"users/{UID}",
    f"orgs/{ORGID}/members",
    "credentials",
    "credentials/github/zz-missing",
    "teams",
    "teams/" + Z,
    "teams/" + Z + "/discover-members?provider=github",
    "teams/" + Z + "/infer-members",
    "identities",
    "sync-configs",
    "sync-configs/auto-import-capabilities",
    "sync-targets",
    "backfill-jobs",
    "backfill-jobs/" + Z,
    "sync-configs/" + Z,
    "sync-configs/" + Z + "/coverage",
    "sync-configs/" + Z + "/jobs",
    "sync-configs/" + Z + "/repositories",
    "sync-runs/" + Z,
    "sync-runs/" + Z + "/units",
    "customer-push/schemas",
    "customer-push/schemas/external-ingest.v1",
    "customer-push/sources",
    "customer-push/sources/" + Z,
    "customer-push/sources/" + Z + "/batches",
    "customer-push/sources/" + Z + "/tokens",
    "customer-push/tokens",
    "customer-push/batches/" + Z,
    "settings/categories",
    "settings/zz-missing-category",
    "llm-settings",
    "integrations/pagerduty/status",
    "integrations/pagerduty/status?credential_name=zz-missing",
]
if CRED:
    reads.append(CRED)
if CAT:
    reads += ["settings/" + CAT, f"settings/{CAT}/zz-missing-key"]
if CAT and KEY:
    reads.append(f"settings/{CAT}/{KEY}")
if SCID:
    reads += [
        f"sync-configs/{SCID}",
        f"sync-configs/{SCID}/jobs",
        f"sync-configs/{SCID}/coverage",
    ]
if CSID:
    reads += [
        f"customer-push/sources/{CSID}",
        f"customer-push/sources/{CSID}/batches",
        f"customer-push/sources/{CSID}/tokens",
    ]
if BID:
    reads.append(f"backfill-jobs/{BID}")
# EXPECTED-GAP rows: Go answers 404 where Python answers 200 today; kept so the gap stays visible.
gaps = ["llm-settings/status", "llm-settings/budget", "llm-settings/spend"]
rows = (
    [("GET", p, None, "") for p in reads]
    + [("GET", p, None, "gap-") for p in gaps]
    + [("POST", "integrations/github/install-url", "{}", "")]
)
for method, p, body, tag in rows:
    label = "a-" + tag + method.lower() + "-" + re.sub(r"[^A-Za-z0-9-]", "-", p)[:80]
    for plane in PLANES:
        for mode in ("noorigin", "origin"):
            st, hd, data = call(
                plane, method, "/api/v1/admin/" + p, body, mode == "origin"
            )
            save(label, plane, mode, hd, data)
# ---- Part B: Fixture-Org writes ----
PK = "zz-venue-probe"
CATW = "general"


# CHAOS-6688 follow-up (gwc-corpus, credited -- prod-ops carried this fix from their untracked
# _records/bigboy-a2b9bf79/pass-bigboy-admin2.sh into the tracked copy): once
# /api/v1/admin/settings/{category}/{key} is fully cut over to Go-only (ingress routes it there
# and Python no longer registers it), Python answers EVERY request on this path with a 500
# "... is served by go-api and has no Python implementation. ingress routing did not intercept
# this request ..." -- including the missing-key case a 404 used to mean. A plain 404-only check
# then reads that retired-route 500 as "dirty state" and can never pass again, regardless of real
# Fixture Org state. "Clean" on a plane now means: the key is absent (404), OR the plane no longer
# serves this route at all (the NAMED retired-route 500 shape, not any 500 -- a real
# object-does-not-exist bug on that path must still abort/restore).
RETIRED_ROUTE_MARKER = "is served by go-api and has no Python implementation"


def clean_on(plane):
    status, _, data = call(plane, "GET", A + f"settings/{CATW}/{PK}")
    if status == 404:
        return True
    if status == 500 and RETIRED_ROUTE_MARKER in data.decode(errors="replace"):
        return True
    return False


def state_clean():
    return all(clean_on(pl) for pl in PLANES)


if not state_clean():
    log(
        f"ABORT part B: the probe setting {CATW}/{PK} already exists on a plane; not writing"
    )
    emit("abort-state-not-clean")
    sys.exit(3)


def writes(plane):
    other = "python" if plane == "go" else "go"
    seq = [
        (
            "w01-put-create",
            "PUT",
            A + f"settings/{CATW}/{PK}",
            json.dumps(
                {"value": "probe-1", "encrypt": False, "description": "venue probe"}
            ),
        ),
        (
            "w02-put-update",
            "PUT",
            A + f"settings/{CATW}/{PK}",
            json.dumps({"value": "probe-2"}),
        ),
        (
            "w03-post-upsert",
            "POST",
            A + "settings",
            json.dumps(
                {
                    "key": PK,
                    "category": CATW,
                    "value": "probe-3",
                    "encrypt": False,
                    "description": "venue probe",
                }
            ),
        ),
        ("w04-delete", "DELETE", A + f"settings/{CATW}/{PK}", None),
        ("w05-delete-again", "DELETE", A + f"settings/{CATW}/{PK}", None),
        (
            "w06-put-llm-category-refused",
            "PUT",
            A + f"settings/llm/{PK}",
            json.dumps({"value": "x"}),
        ),
        (
            "w07-post-invalid-body",
            "POST",
            A + "settings",
            json.dumps({"category": CATW}),
        ),
        (
            "w08-put-llm-settings-invalid",
            "PUT",
            A + "llm-settings",
            json.dumps({"provider": 1}),
        ),
        ("w09-delete-llm-settings", "DELETE", A + "llm-settings", None),
        ("w10-delete-sync-config-missing", "DELETE", A + "sync-configs/" + Z, None),
        (
            "w11-put-sync-config-repositories-missing",
            "PUT",
            A + f"sync-configs/{Z}/repositories",
            json.dumps({"owner": "probe", "repos": []}),
        ),
    ]
    for label, method, path, body in seq:
        st, hd, data = call(plane, method, path, body)
        save(label, plane, "noorigin", hd, data)
        if label.startswith(("w01", "w02", "w03", "w04")):
            # read-back after every settings write: on the SAME plane and on the OTHER plane
            rb = A + f"settings/{CATW}/{PK}"
            s1, h1, d1 = call(plane, "GET", rb)
            save(label + "-rb", plane, "noorigin", h1, d1)
            s2, h2, d2 = call(other, "GET", rb)
            save(label + "-rbx", plane, "noorigin", h2, d2)


for plane in ("go", "python"):
    writes(plane)
    # restore check: the probe setting is gone on both planes (w04 deleted it); force-delete if a
    # step failed midway. Uses clean_on(), not a raw 404 check -- the same post-cutover stub shape
    # applies here too (a bare 404-only check would spuriously force-DELETE against the python
    # plane on every run once that plane no longer implements the route at all).
    for pl in PLANES:
        if not clean_on(pl):
            call(pl, "DELETE", A + f"settings/{CATW}/{PK}")
            log(f"RESTORE forced on {pl}")
clean = state_clean()
log(f"PART B done; clean={clean}")
emit("ok" if clean else "not-restored")
sys.exit(0 if clean else 5)
