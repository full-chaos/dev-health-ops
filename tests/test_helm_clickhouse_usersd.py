"""CHAOS-6181/CHAOS-6474: clickhouse.usersd renders a users.d ConfigMap, a directory mount and symlinks.

The bundled ClickHouse StatefulSet used to mount only /var/lib/clickhouse, so
a read-only user (acr_ro) could only exist as a one-off file written into the
pod and lost on restart. `clickhouse.usersd` (filename -> XML) makes it
durable. Unset must render exactly what the chart rendered before.

CHAOS-6474: the ConfigMap is mounted as a DIRECTORY (kubelet refreshes it in
place; a per-file subPath mount is never refreshed) and an initContainer links
each file into an emptyDir users.d, so a grant change reloads without a pod
restart. Only the FILE SET rolls the pod.
"""

import json
from pathlib import Path
from subprocess import run

import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"
_XML = (
    "<clickhouse><users><ro><password_sha256_hex>"
    + "a" * 64
    + "</password_sha256_hex><profile>ro_profile</profile></ro></users></clickhouse>"
)


def _docs(*extra_set: str) -> list[dict]:
    args = [
        "helm",
        "template",
        "t",
        str(_CHART),
        "--show-only",
        "templates/clickhouse.yaml",
    ]
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


def test_usersd_renders_configmap_directory_mount_and_link_initcontainer() -> None:
    docs = _docs(f"clickhouse.usersd.acr_ro\\.xml={_XML}")
    cm = next(d for d in docs if d["kind"] == "ConfigMap")
    assert cm["metadata"]["name"] == "t-dev-health-clickhouse-usersd"
    assert cm["data"] == {"acr_ro.xml": _XML}

    pod = _sts(docs)["spec"]["template"]
    assert pod["spec"]["volumes"] == [
        {
            "name": "clickhouse-usersd",
            "configMap": {"name": "t-dev-health-clickhouse-usersd"},
        },
        {"name": "clickhouse-usersd-links", "emptyDir": {}},
    ]
    mounts = {m["mountPath"]: m for m in pod["spec"]["containers"][0]["volumeMounts"]}
    assert "/var/lib/clickhouse" in mounts
    # never a per-file subPath (kubelet does not refresh those): the ConfigMap is a
    # directory mount, users.d is the emptyDir the links live in.
    assert not any("subPath" in m for m in mounts.values())
    cm_mount = mounts["/etc/clickhouse-server/usersd-cm"]
    assert cm_mount["name"] == "clickhouse-usersd" and cm_mount["readOnly"] is True
    assert mounts["/etc/clickhouse-server/users.d"]["name"] == "clickhouse-usersd-links"

    (init,) = pod["spec"]["initContainers"]
    assert init["name"] == "clickhouse-usersd-links"
    assert init["image"] == pod["spec"]["containers"][0]["image"]
    assert init["command"][-2:] == ["--", "acr_ro.xml"]
    assert "../usersd-cm/$n" in init["command"][2]
    assert init["volumeMounts"] == [
        {"name": "clickhouse-usersd-links", "mountPath": "/links"}
    ]


def test_usersd_content_change_does_not_roll_the_pod() -> None:
    a = _sts(_docs(f"clickhouse.usersd.a\\.xml={_XML}"))["spec"]["template"]["metadata"]
    b = _sts(_docs(f"clickhouse.usersd.a\\.xml={_XML}x"))["spec"]["template"][
        "metadata"
    ]
    assert a["annotations"] == b["annotations"]
    assert "checksum/clickhouse-usersd" not in a["annotations"]


def test_usersd_file_set_change_rolls_the_pod() -> None:
    a = _sts(_docs(f"clickhouse.usersd.a\\.xml={_XML}"))["spec"]["template"]["metadata"]
    b = _sts(
        _docs(f"clickhouse.usersd.a\\.xml={_XML}", f"clickhouse.usersd.b\\.xml={_XML}")
    )["spec"]["template"]["metadata"]
    assert a["annotations"] != b["annotations"]


def test_usersd_works_without_persistence() -> None:
    docs = _docs(
        "clickhouse.persistence.enabled=false", f"clickhouse.usersd.a\\.xml={_XML}"
    )
    mounts = _sts(docs)["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    assert [m["mountPath"] for m in mounts] == [
        "/etc/clickhouse-server/users.d",
        "/etc/clickhouse-server/usersd-cm",
    ]


def _docs_json(usersd_json: str) -> list[dict]:
    out = run(
        [
            "helm",
            "template",
            "t",
            str(_CHART),
            "--show-only",
            "templates/clickhouse.yaml",
            "--set-json",
            f"clickhouse.usersd={usersd_json}",
        ],
        check=True,
        capture_output=True,
        text=True,
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
    (init,) = _sts(docs)["spec"]["template"]["spec"]["initContainers"]
    assert init["command"][3:] == ["--", "123", "b.xml"]
