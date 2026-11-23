#!/usr/bin/env python3
# Synthetic SigNoz v3 rows for the FORM-14542-2 exec cell. Output: INSERT statement on stdout.
import datetime
import sys

START_S = int(sys.argv[1])
T_ORG = "c6a38355-0000-4000-8000-000000000001"
O_ORG = "99999999-0000-4000-8000-000000000009"
SVC = "dev-health-go-sync-provider"
LIST = "/repos/{owner}/{repo}/actions/runs/{run}/artifacts"
ZIP = "/repos/{owner}/{repo}/actions/artifacts/{artifact}/zip"
REPO = "/repos/{owner}/{repo}"
RUNS = "/repos/{owner}/{repo}/actions/runs"
rows = []
seq = [0]


def sid():
    seq[0] += 1
    return "%016x" % seq[0]


def m(d):
    return "map(" + ", ".join("'%s', %s" % (k, ("'%s'" % v) if isinstance(v, str) else v) for k, v in d.items()) + ")"


def span(tr, off_ms, name, dur_ms, parent="", s=None, n=None, svc=SVC):
    span_id = sid()
    ms = START_S * 1000 + off_ms
    dt = datetime.datetime.fromtimestamp(ms // 1000, datetime.timezone.utc)
    ts = "%s.%03d" % (dt.strftime("%Y-%m-%d %H:%M:%S"), ms % 1000)
    bucket = (ms // 1000) // 1800 * 1800
    rows.append("(%d, toDateTime64('%s', 9, 'UTC'), '%s', '%s', '%s', '%s', %d, %s, %s, %s)" % (
        bucket, ts, tr, span_id, parent, name, dur_ms * 1000000, m(s or {}), m(n or {}), m({"service.name": svc})))
    return span_id


def job(tr, off_ms, dur_ms, dom, job_id, org=T_ORG, dtype="sync_run_unit"):
    return span(tr, off_ms, "dev_health.job.execute", dur_ms,
                s={"dev_health.domain.id": dom, "dev_health.domain.type": dtype, "dev_health.organization_id": org},
                n={"dev_health.job.id": job_id})


def req(tr, off_ms, dur_ms, route, target, gate, parent):
    return span(tr, off_ms, "dev_health.provider.request", dur_ms, parent,
                s={"provider": "github", "http.route": route, "dev_health.provider.target": target},
                n={"dev_health.provider.gate_wait_ms": gate})


def dl(tr, off_ms, dur_ms, parent, zip_ms, zip_gate, red_ms):
    d = span(tr, off_ms, "dev_health.github_tests.artifact_download", dur_ms, parent, s={"outcome": "read"})
    req(tr, off_ms + 1, zip_ms, ZIP, "api", zip_gate, d)
    req(tr, off_ms + 2, red_ms, ZIP, "redirect", 0, d)


def tid(i):
    return "%032x" % i


S = 1000
UA = "aaaaaaaa-0000-4000-8000-000000000001"
UB = "bbbbbbbb-0000-4000-8000-000000000002"
# T1 kept: snooze (2 job.execute, same job id 42); D0 L1 D1 D2 D3 L2 L3+D4 (list and download share one timestamp)
t = tid(1)
j1 = job(t, 600 * S, 30000, UA, 42)
j2 = job(t, 700 * S, 20000, UA, 42)
span(t, 650 * S, "dev_health.provider_unit.chunk_commit", 1500, j1)
span(t, 750 * S, "dev_health.provider_unit.chunk_commit", 2500, j2)
dl(t, 601 * S, 4000, j1, 210, 1, 1000)
req(t, 602 * S, 120, LIST, "api", 1, j1)
dl(t, 603 * S, 3000, j1, 220, 0, 900)
dl(t, 604 * S, 2000, j1, 230, 2, 800)
dl(t, 605 * S, 1000, j1, 240, 0, 700)
req(t, 607 * S, 130, LIST, "api", 2, j1)
req(t, 608 * S, 140, LIST, "api", 3, j1)
dl(t, 608 * S, 500, j1, 250, 3, 600)
req(t, 610 * S, 150, REPO, "api", 4, j1)
req(t, 611 * S, 300, RUNS, "api", 5, j1)
req(t, 710 * S, 400, ZIP, "api", 7, j2)
# T2 not kept: two job ids, one download
t = tid(2)
job(t, 1000 * S, 5000, "cccccccc-0000-4000-8000-000000000003", 43)
job(t, 1001 * S, 5000, "cccccccc-0000-4000-8000-000000000003", 44)
dl(t, 1002 * S, 1000, "", 100, 0, 100)
# T3 kept: unit bbbbbbbb across 00:00Z (START + 61897 s); list before midnight, download after
t = tid(3)
j1 = job(t, 61880 * S, 20000, UB, 45)
j2 = job(t, 61905 * S, 10000, UB, 45)
span(t, 61906 * S, "dev_health.provider_unit.chunk_commit", 800, j2)
req(t, 61890 * S, 100, LIST, "api", 2, j1)
dl(t, 61900 * S, 2000, j1, 300, 1, 700)
# T4 other org, one download
t = tid(4)
job(t, 1500 * S, 3000, "dddddddd-0000-4000-8000-000000000004", 46, org=O_ORG)
dl(t, 1501 * S, 1000, "", 100, 0, 100)
# T5 family: sync-provider traces with no download (control and coverage only)
t = tid(5)
j = job(t, 2000 * S, 1000, "eeeeeeee-0000-4000-8000-000000000005", 47)
req(t, 2001 * S, 50, RUNS, "api", 0, j)
job(tid(8), 30000 * S, 1000, "eeeeeeee-0000-4000-8000-000000000006", 49)
job(tid(9), 80000 * S, 1000, "eeeeeeee-0000-4000-8000-000000000007", 52)
job(tid(10), 3000 * S, 1000, "eeeeeeee-0000-4000-8000-000000000008", 53)
# outside the window: START - 1 s and END (START + 86400 s) with a download each
for i, off in ((6, -1 * S), (7, 86400 * S)):
    t = tid(i)
    j = job(t, off, 7000, "ffffffff-0000-4000-8000-00000000000%d" % i, 50 + i)
    dl(t, off, 1000, j, 100, 0, 100)
# filler: 2000 rows of another service inside the window buckets
for i in range(2000):
    span(tid(100 + i), 10 * S + (i * 1700), "filler.op", 1, svc="other-service")

print("INSERT INTO signoz_traces.signoz_index_v3 (ts_bucket_start, timestamp, trace_id, span_id, parent_span_id, name, duration_nano, attributes_string, attributes_number, resources_string) VALUES")
print(",\n".join(rows) + ";")
