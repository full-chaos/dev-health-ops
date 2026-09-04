"""The api Service's type/nodePort must be expressible in values (CHAOS-4984).

api-service.yaml hardcoded `type: ClusterIP` with no values-driven way to
change it. A local kind/kiac profile that needs NodePort access (no ingress
controller, no cloud LoadBalancer) had no way to set it through Helm, so it
hand-patched the rendered Service to NodePort 30530 out of band
(docs/contribute/development/lane-isolation-kiac.md). That patch is invisible
to Helm's own state: the next `helm upgrade` re-asserts `type: ClusterIP` via
Server-Side Apply, and the two field managers (the hand patch's and Helm's)
conflict on `spec.type`/`spec.ports[].nodePort`.

`api.service.type` and `api.service.nodePort` are now real values,
defaulting to `ClusterIP` and unset (no nodePort key rendered at all -- `""`
renders nothing via `with`, letting Kubernetes allocate one if/when type is
ever NodePort/LoadBalancer).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from subprocess import run

import pytest
import yaml

_CHART = Path(__file__).parents[1] / "deploy/helm/dev-health"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm is not installed"
)


def _api_service(*extra_set: str) -> dict:
    argv = [
        "helm",
        "template",
        "api-service-contract",
        str(_CHART),
        "--show-only",
        "templates/api-service.yaml",
    ]
    for value in extra_set:
        argv += ["--set", value]
    rendered = run(argv, check=True, capture_output=True, text=True)
    documents = [d for d in yaml.safe_load_all(rendered.stdout) if d]
    assert len(documents) == 1, f"expected exactly one document, got {len(documents)}"
    return documents[0]


def test_default_service_is_clusterip_with_no_nodeport_key() -> None:
    service = _api_service()
    assert service["spec"]["type"] == "ClusterIP"
    assert "nodePort" not in service["spec"]["ports"][0], (
        "an unset nodePort must not render the key at all -- a literal "
        '`nodePort: ""`/`nodePort: null` is itself invalid on a ClusterIP '
        f"Service, got {service['spec']['ports'][0]!r}"
    )


def test_service_type_and_nodeport_are_settable_via_values() -> None:
    """The exact shape the kiac local profile needs, expressed through
    values instead of a hand kubectl-patch that a subsequent `helm
    upgrade` would conflict with."""
    service = _api_service("api.service.type=NodePort", "api.service.nodePort=30530")
    assert service["spec"]["type"] == "NodePort"
    assert service["spec"]["ports"][0]["nodePort"] == 30530


def test_nodeport_without_a_matching_service_type_still_renders() -> None:
    """The template does not validate type/nodePort consistency -- the
    Kubernetes API server does that (a nodePort on a ClusterIP Service is
    rejected there, not by this chart). Documented behavior, not a chart
    guard: asserted here so a future change to add client-side validation
    is a deliberate choice, not an accidental behavior change nobody
    noticed."""
    service = _api_service("api.service.nodePort=30530")
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["ports"][0]["nodePort"] == 30530
