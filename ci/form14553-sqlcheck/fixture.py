#!/usr/bin/env python3
# Synthetic SigNoz v3 rows for the FORM-14553-1 exec cell, and the expected output derived from the fixture rule
# (FORM-14553-1.md section 3 class table), independent of the SQL. Usage: fixture.py rows|expected START_S
import datetime
import sys

mode, START_S = sys.argv[1], int(sys.argv[2])
END_S = START_S + 7 * 86400
SVC = "dev-health-go-sync-provider"
REQ = "dev_health.provider.request"
ROUTES = [
    "/repos/{owner}/{repo}/releases", "/repos/{owner}/{repo}/deployments",
    "/repos/{owner}/{repo}/deployments/{id}/statuses", "/repos/{owner}/{repo}/commits/{sha}/pulls",
    "/repos/{owner}/{repo}/dependabot/alerts", "/repos/{owner}/{repo}/code-scanning/alerts",
    "/repos/{owner}/{repo}/security-advisories", "/projects/{project}/releases", "/projects/{project}/deployments",
    "/projects/{project}/repository/commits/{sha}/merge_requests", "/api/v2/code-refs/repositories",
]
# (status, outcome, error.type, attempts, gate_wait_ms): the 15 spans per route
CLASSES = [
    (401, "failed", "authentication", 1, 0), (403, "failed", "rate_limited", 1, 0),
    (403, "failed", "authentication", 1, 0), (403, "cancelled", "", 1, 0),
    (404, "failed", "not_found", 1, 0), (409, "failed", "conflict", 1, 0),
    (400, "failed", "permanent", 1, 0), (422, "failed", "permanent", 1, 0), (418, "failed", "permanent", 1, 0),
    (429, "failed", "rate_limited", 1, 0), (503, "failed", "transient", 1, 0),
    (200, "response", "", 1, 0), (200, "response", "", 3, 5),
    (None, "failed", "rate_limited", 1, 0), (None, "budget_contended", "", 1, 0),
]
spans = []  # dicts: svc, name, ms, route (None = no key), prov, oc, et, st, att, gw


def add(ms, route, prov="github", oc="response", et="", st=200, att=1, gw=0, svc=SVC, name=REQ):
    spans.append(dict(svc=svc, name=name, ms=ms, route=route, prov=prov, oc=oc, et=et, st=st, att=att, gw=gw))


def prov_of(route):
    return "gitlab" if route.startswith("/projects/") else "launchdarkly" if route.startswith("/api/") else "github"


seq = [0]


def at(day):
    seq[0] += 1
    return (START_S + day * 86400 + 3600 + seq[0]) * 1000


for ri, route in enumerate(ROUTES):
    for ci, (st, oc, et, att, gw) in enumerate(CLASSES):
        add(at((ri + ci) % 7), route, prov_of(route), oc, et, st, att, gw)
    add(START_S * 1000 - 1, route, prov_of(route))   # excluded: START - 1 ms
    add(END_S * 1000, route, prov_of(route))         # excluded: END
SHARED = "/repos/{owner}/{repo}"
for d in range(3):
    add(at(d), SHARED)
add(at(3), "unrouted")
add(at(4), None, prov="gitlab")
for _ in range(5):
    add(at(5), None, prov="", name="dev_health.job.execute")
for i in range(5):
    add(at(i), ROUTES[i], svc="other-svc")
FILLER = 5000
for i in range(FILLER):
    add((START_S + 5 + i * 100) * 1000, None, prov="", name="filler.op", svc="other-service")
OLD = (END_S - 9 * 86400) * 1000
add(OLD, None, prov="", name="old.op", svc="other-service")


def utc(ms):
    return datetime.datetime.fromtimestamp(ms // 1000, datetime.timezone.utc)


if mode == "rows":
    def q(v):
        return "'%s'" % v

    out = []
    for i, s in enumerate(spans):
        ts = "%s.%03d" % (utc(s["ms"]).strftime("%Y-%m-%d %H:%M:%S"), s["ms"] % 1000)
        a = {"provider": s["prov"], "outcome": s["oc"]}
        if s["route"] is not None:
            a["http.route"] = s["route"]
        if s["et"]:
            a["error.type"] = s["et"]
        n = {"dev_health.provider.attempts": s["att"], "dev_health.provider.gate_wait_ms": s["gw"]}
        if s["st"] is not None:
            n["http.response.status_code"] = s["st"]
        mp = lambda d: "map(" + ", ".join("%s, %s" % (q(k), q(v) if isinstance(v, str) else v) for k, v in d.items()) + ")"
        out.append("(%d, toDateTime64('%s', 9, 'UTC'), '%032x', '%016x', '', '%s', 1000000, %s, %s, map('service.name', '%s'))" % (
            (s["ms"] // 1000) // 1800 * 1800, ts, i + 1, i + 1, s["name"], mp(a), mp(n), s["svc"]))
    print("INSERT INTO signoz_traces.signoz_index_v3 (ts_bucket_start, timestamp, trace_id, span_id, parent_span_id, name, duration_nano, attributes_string, attributes_number, resources_string) VALUES")
    print(",\n".join(out) + ";")
    sys.exit(0)

# expected: apply the md rule to the spans that are in the window, of the service, named request
def klass(s):
    st, et = s["st"] or 0, s["et"]
    if st == 401: return "401"
    if st == 403 and et == "rate_limited": return "403_rate_limited"
    if st == 403 and et == "authentication": return "403_permission"
    if st == 403: return "403_other_outcome"
    if st == 404: return "404"
    if st == 409: return "409"
    if st in (400, 422): return "400_422"
    if st == 429: return "429"
    if 400 <= st < 500: return "4xx_other"
    if 500 <= st < 600: return "5xx"
    if 200 <= st < 400: return "2xx_3xx"
    return "no_status"


acc = {}


def bump(key, s):
    r = acc.setdefault(key, [0, 0, 0, 0, 0])
    r[0] += 1
    r[1] += s["att"] > 1
    r[2] += s["att"] > 1 and s["oc"] == "response"
    r[3] += s["gw"] > 0
    r[4] += s["att"]


for s in spans:
    if not (START_S * 1000 <= s["ms"] < END_S * 1000 and s["svc"] == SVC and s["name"] == REQ):
        continue
    bump(("day", "", utc(s["ms"]).strftime("%Y-%m-%d")), s)
    if s["route"] in ROUTES:
        bump(("control", s["route"], "all"), s)
        bump(("class", s["route"], klass(s)), s)
        bump(("outcome", s["route"], s["oc"] + " | " + s["et"]), s)
    if s["route"] in ("unrouted", None):
        bump(("unrouted", s["prov"], s["route"] or ""), s)
# the md WANT per route (section 10 item 1), checked against the model
for route in ROUTES:
    assert acc[("control", route, "all")] == [15, 1, 1, 1, 17], route
    want = {"401": 1, "403_rate_limited": 1, "403_permission": 1, "403_other_outcome": 1, "404": 1, "409": 1,
            "400_422": 2, "4xx_other": 1, "429": 1, "5xx": 1, "2xx_3xx": 2, "no_status": 2}
    for k, v in want.items():
        assert acc[("class", route, k)][0] == v, (route, k)
    assert acc[("class", route, "2xx_3xx")][1:4] == [1, 1, 1]
assert len({k[2] for k in acc if k[0] == "day"}) == 7
assert acc[("unrouted", "github", "unrouted")][0] == 1 and acc[("unrouted", "gitlab", "")][0] == 1
for key in sorted(acc):
    print("\t".join(str(c) for c in key + tuple(acc[key])))
