"""CHAOS-6181: clickhouse.usersd renders a users.d ConfigMap + subPath mounts.

The bundled ClickHouse StatefulSet used to mount only /var/lib/clickhouse, so
a read-only user (acr_ro) could only exist as a one-off file written into the
pod and lost on restart. `clickhouse.usersd` (filename -> XML) makes it
durable. Unset must render exactly what the chart rendered before.
"""

import json
from pathlib import Path
from subprocess import run

import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"
_XML = (
    "<clickhouse><users><ro><password_sha256_hex>" + "a" * 64 +
    "</password_sha256_hex><profile>ro_profile</profile></ro></users></clickhouse>"
)


def _docs(*extra_set: str) -> list[dict]:
    args = ["helm", "template", "t", str(_CHART), "--show-only", "templates/clickhouse.yaml"]
    for value in extra_set:
        args += ["--set-string" if "usersd" in value else "--set", value]
    out = run(args, check=True, capture_output=True, text=True).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def _sts(docs: list[dict]) -> dict:
    return next(d for d in docs if d["kind"] == "StatefulSet")


def test_default_renders_no_usersd_objects() -> None:
    docs = _docs()
    assert sorted(d["kind"] for d in docs) == ["Service", "StatefulSet"]
    pod = _sts(docs)["spec"]["template"]
    assert "volumes" not in pod["spec"]
    assert "annotations" not in pod["metadata"]
    mounts = pod["spec"]["containers"][0]["volumeMounts"]
    assert [m["mountPath"] for m in mounts] == ["/var/lib/clickhouse"]


def test_usersd_renders_configmap_volume_and_subpath_mount() -> None:
    docs = _docs(f"clickhouse.usersd.acr_ro\\.xml={_XML}")
    cm = next(d for d in docs if d["kind"] == "ConfigMap")
    assert cm["metadata"]["name"] == "t-dev-health-clickhouse-usersd"
    assert cm["data"] == {"acr_ro.xml": _XML}

    pod = _sts(docs)["spec"]["template"]
    assert pod["spec"]["volumes"] == [
        {"name": "clickhouse-usersd", "configMap": {"name": "t-dev-health-clickhouse-usersd"}}
    ]
    mounts = {m["mountPath"]: m for m in pod["spec"]["containers"][0]["volumeMounts"]}
    # data mount stays; users.d is per-file (subPath), never the whole directory,
    # so the image's own users.d files are not shadowed.
    assert "/var/lib/clickhouse" in mounts
    mount = mounts["/etc/clickhouse-server/users.d/acr_ro.xml"]
    assert mount["subPath"] == "acr_ro.xml" and mount["readOnly"] is True
    assert "/etc/clickhouse-server/users.d" not in mounts
    # content change must roll the pod (subPath mounts never hot-update)
    assert pod["metadata"]["annotations"]["checksum/clickhouse-usersd"]


def test_usersd_checksum_tracks_content() -> None:
    a = _sts(_docs(f"clickhouse.usersd.a\\.xml={_XML}"))["spec"]["template"]["metadata"]
    b = _sts(_docs(f"clickhouse.usersd.a\\.xml={_XML}x"))["spec"]["template"]["metadata"]
    assert a["annotations"] != b["annotations"]


def test_usersd_works_without_persistence() -> None:
    docs = _docs("clickhouse.persistence.enabled=false", f"clickhouse.usersd.a\\.xml={_XML}")
    mounts = _sts(docs)["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    assert [m["mountPath"] for m in mounts] == ["/etc/clickhouse-server/users.d/a.xml"]


def _docs_json(usersd_json: str) -> list[dict]:
    out = run(
        ["helm", "template", "t", str(_CHART), "--show-only", "templates/clickhouse.yaml",
         "--set-json", f"clickhouse.usersd={usersd_json}"],
        check=True, capture_output=True, text=True,
    ).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def test_usersd_null_renders_like_default() -> None:
    docs = _docs_json("null")
    assert sorted(d["kind"] for d in docs) == ["Service", "StatefulSet"]


def test_usersd_leading_whitespace_and_numeric_filename_round_trip() -> None:
    content = "  <clickhouse>\n<users/>\n</clickhouse>"
    docs = _docs_json(json.dumps({"123": content, "b.xml": content}))
    cm = next(d for d in docs if d["kind"] == "ConfigMap")
    assert cm["data"] == {"123": content, "b.xml": content}
    mounts = _sts(docs)["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    sub = {m["subPath"] for m in mounts if "subPath" in m}
    assert sub == {"123", "b.xml"}
