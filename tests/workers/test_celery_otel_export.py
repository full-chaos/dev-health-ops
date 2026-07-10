from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import grpc
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2_grpc import (
    MetricsServiceServicer,
    add_MetricsServiceServicer_to_server,
)


class _MetricsReceiver(MetricsServiceServicer):
    def __init__(self) -> None:
        self.requests: list[ExportMetricsServiceRequest] = []

    def Export(
        self,
        request: ExportMetricsServiceRequest,
        context: grpc.ServicerContext,
    ) -> ExportMetricsServiceResponse:
        self.requests.append(request)
        return ExportMetricsServiceResponse()


WORKER_SCRIPT = """
import os
from opentelemetry.sdk.resources import Resource
from dev_health_ops.tracing import init_metrics, shutdown_metrics
from dev_health_ops.work_graph.investment import llm_telemetry as telemetry

resource = None
if os.getenv("TEST_SUPPLIED_RESOURCE") == "true":
    resource = Resource.create({"service.name": "supplied-api-resource"})
assert init_metrics(resource=resource, shutdown_on_exit=False)
with telemetry.llm_call_metrics(
    provider="openai",
    model="gpt-5-nano",
    stage=telemetry.STAGE_INITIAL,
    prompt_kind=telemetry.PROMPT_KIND_CATEGORIZE,
    prompt_version="investment-categorization-v2",
) as call:
    call.set_result(model="gpt-5-nano", input_tokens=12, output_tokens=4, text="{}")
telemetry.record_validation(
    provider="openai",
    model="gpt-5-nano",
    stage=telemetry.STAGE_REPAIR,
    prompt_version="investment-categorization-v2",
    errors=["all_weights_zero"],
)
telemetry.record_batch_completion(
    provider="openai",
    model="gpt-5-nano",
    prompt_version="investment-categorization-v2",
    duration_seconds=0.5,
    input_tokens=3,
    output_tokens=2,
    output_chars=20,
    succeeded=False,
)
telemetry.record_categorization_outcome(
    provider="openai",
    model="gpt-5-nano",
    prompt_version="investment-categorization-v2",
    status="invalid_llm_output",
)
telemetry.record_explanation_parse(
    provider="openai",
    model="gpt-5-nano",
    prompt_version="investment-mix-explain-v2",
    status="fallback",
)
assert shutdown_metrics()
"""


def test_recycled_workers_export_investment_metrics_to_configured_collector() -> None:
    receiver = _MetricsReceiver()
    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    add_MetricsServiceServicer_to_server(receiver, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    env = {
        **os.environ,
        "OTEL_ENABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{port}",
        "OTEL_METRIC_EXPORT_INTERVAL": "60000",
        "OTEL_SERVICE_NAME": "dev-health-worker-test",
    }

    try:
        for supplied_resource in (False, True):
            worker_env = {
                **env,
                "TEST_SUPPLIED_RESOURCE": str(supplied_resource).lower(),
            }
            subprocess.run(
                [sys.executable, "-c", WORKER_SCRIPT],
                check=True,
                env=worker_env,
                capture_output=True,
                text=True,
            )
    finally:
        server.stop(grace=0).wait()

    metric_names = {
        metric.name
        for request in receiver.requests
        for resource_metrics in request.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
    }
    assert {
        "devhealth_investment_llm_requests_total",
        "devhealth_investment_llm_request_duration_seconds",
        "devhealth_investment_llm_request_errors_total",
        "devhealth_investment_llm_tokens_total",
        "devhealth_investment_llm_output_chars",
        "devhealth_investment_llm_validation_total",
        "devhealth_investment_llm_validation_failures_total",
        "devhealth_investment_llm_categorization_outcomes_total",
        "devhealth_investment_llm_explanation_parse_total",
    } <= metric_names
    instance_ids = {
        attribute.value.string_value
        for request in receiver.requests
        for resource_metrics in request.resource_metrics
        for attribute in resource_metrics.resource.attributes
        if attribute.key == "service.instance.id"
    }
    assert len(instance_ids) == 2
