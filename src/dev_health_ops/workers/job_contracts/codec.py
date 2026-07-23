"""Strict decoder and producer adapter for versioned worker arguments."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .models import (
    CONTRACT_VERSION_V1,
    KIND_BILLING_NOTIFICATION,
    KIND_DAILY_METRICS_DISPATCH,
    KIND_DAILY_METRICS_FINALIZE,
    KIND_DAILY_METRICS_PARTITION,
    KIND_HEARTBEAT,
    KIND_INVESTMENT_CHUNK,
    KIND_INVESTMENT_DISPATCH,
    KIND_INVESTMENT_FINALIZE,
    KIND_INVESTMENT_MATERIALIZE,
    KIND_REPORT_EXECUTE_ON_DEMAND,
    KIND_REPORT_EXECUTE_SCHEDULED,
    KIND_RETENTION_CLEANUP,
    KIND_WEBHOOK_DELIVERY,
    KIND_WORK_GRAPH_BUILD,
    MAX_ENVELOPE_BYTES,
    RETENTION_WORKER_TERMINAL,
    BillingNotificationPayload,
    ContractPayload,
    DailyMetricsDispatchPayload,
    DailyMetricsFinalizePayload,
    DailyMetricsPartitionPayload,
    DomainLink,
    Envelope,
    HeartbeatPayload,
    InvestmentChunkPayload,
    InvestmentDispatchPayload,
    InvestmentFinalizePayload,
    InvestmentMaterializePayload,
    JobPayload,
    OnDemandReportExecutionPayload,
    RemainingCapacityPayload,
    RemainingComplexityPayload,
    RemainingDORAPayload,
    RemainingExtraMetricsPayload,
    RemainingMembershipPayload,
    RemainingRecommendationsPayload,
    RemainingReleaseImpactPayload,
    RemainingTeamMetricsPayload,
    RetentionCleanupPayload,
    ScheduledReportExecutionPayload,
    WebhookDeliveryPayload,
    WorkGraphBuildPayload,
)

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_DOMAIN_TYPE = re.compile(r"^[a-z][a-z0-9_]*$")
_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_RFC3339_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_MAX_JSON_DEPTH = 16
_REMAINING_PAYLOAD_TYPES = (
    RemainingCapacityPayload,
    RemainingComplexityPayload,
    RemainingDORAPayload,
    RemainingExtraMetricsPayload,
    RemainingMembershipPayload,
    RemainingRecommendationsPayload,
    RemainingReleaseImpactPayload,
    RemainingTeamMetricsPayload,
)
_REMAINING_PAYLOAD_BY_KIND = {
    payload_type.KIND: payload_type for payload_type in _REMAINING_PAYLOAD_TYPES
}


class ContractDecodeError(ValueError):
    """Safe, value-free rejection of malformed or unsupported arguments."""


def decode_envelope(kind: str, data: bytes | str) -> Envelope:
    """Decode one known kind without accepting extensions or duplicate keys."""

    if kind not in {
        KIND_HEARTBEAT,
        KIND_BILLING_NOTIFICATION,
        KIND_RETENTION_CLEANUP,
        KIND_REPORT_EXECUTE_ON_DEMAND,
        KIND_REPORT_EXECUTE_SCHEDULED,
        KIND_WEBHOOK_DELIVERY,
        KIND_DAILY_METRICS_DISPATCH,
        KIND_DAILY_METRICS_PARTITION,
        KIND_DAILY_METRICS_FINALIZE,
        KIND_WORK_GRAPH_BUILD,
        KIND_INVESTMENT_MATERIALIZE,
        KIND_INVESTMENT_DISPATCH,
        KIND_INVESTMENT_CHUNK,
        KIND_INVESTMENT_FINALIZE,
        *_REMAINING_PAYLOAD_BY_KIND,
    }:
        raise ContractDecodeError("unknown job kind")
    document = load_json_document(data, max_bytes=MAX_ENVELOPE_BYTES)
    envelope = _expect_object(
        document,
        required={
            "contract_version",
            "correlation_id",
            "idempotency_key",
            "domain",
            "payload",
        },
        optional={"organization_id"},
        label="envelope",
    )
    version = _expect_int(envelope["contract_version"], "contract_version")
    if version != CONTRACT_VERSION_V1:
        raise ContractDecodeError("unsupported contract version")

    organization_id_value = envelope.get("organization_id")
    organization_id: str | None
    if organization_id_value is None:
        organization_id = None
    else:
        organization_id = _expect_string(organization_id_value, "organization_id")
        _validate_uuid("organization_id", organization_id)
    tenant_kind = kind in {
        KIND_BILLING_NOTIFICATION,
        KIND_DAILY_METRICS_DISPATCH,
        KIND_DAILY_METRICS_PARTITION,
        KIND_DAILY_METRICS_FINALIZE,
        KIND_INVESTMENT_CHUNK,
        KIND_INVESTMENT_DISPATCH,
        KIND_INVESTMENT_FINALIZE,
        KIND_INVESTMENT_MATERIALIZE,
        KIND_WORK_GRAPH_BUILD,
        *_REMAINING_PAYLOAD_BY_KIND,
    }
    if tenant_kind and organization_id is None:
        raise ContractDecodeError("organization_id is required for a tenant job")
    if not tenant_kind and organization_id is not None:
        raise ContractDecodeError("organization_id is forbidden for a global job")

    correlation_id = _expect_string(envelope["correlation_id"], "correlation_id")
    idempotency_key = _expect_string(envelope["idempotency_key"], "idempotency_key")
    _validate_safe_id("correlation_id", correlation_id, 128)
    _validate_safe_id("idempotency_key", idempotency_key, 256)

    domain_raw = _expect_object(
        envelope["domain"], required={"type", "id"}, optional=set(), label="domain"
    )
    domain_type = _expect_string(domain_raw["type"], "domain.type")
    domain_id = _expect_string(domain_raw["id"], "domain.id")
    if not _DOMAIN_TYPE.fullmatch(domain_type) or len(domain_type) > 64:
        raise ContractDecodeError("domain.type is invalid")
    _validate_uuid("domain.id", domain_id)

    payload: JobPayload
    if kind == KIND_BILLING_NOTIFICATION:
        if domain_type != BillingNotificationPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_billing_notification(envelope["payload"])
    elif kind == KIND_WEBHOOK_DELIVERY:
        if domain_type != WebhookDeliveryPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_webhook_delivery(envelope["payload"])
    elif kind == KIND_HEARTBEAT:
        if domain_type != HeartbeatPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_heartbeat(envelope["payload"])
    elif kind == KIND_RETENTION_CLEANUP:
        if domain_type != RetentionCleanupPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_retention(envelope["payload"])
    elif kind == KIND_REPORT_EXECUTE_ON_DEMAND:
        if domain_type != OnDemandReportExecutionPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_on_demand_report_execution(envelope["payload"])
    elif kind == KIND_REPORT_EXECUTE_SCHEDULED:
        if domain_type != ScheduledReportExecutionPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_scheduled_report_execution(envelope["payload"])
    elif kind == KIND_DAILY_METRICS_DISPATCH:
        if domain_type != DailyMetricsDispatchPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_daily_dispatch(envelope["payload"])
    elif kind == KIND_DAILY_METRICS_PARTITION:
        if domain_type != DailyMetricsPartitionPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_daily_partition(envelope["payload"])
    elif kind == KIND_DAILY_METRICS_FINALIZE:
        if domain_type != DailyMetricsFinalizePayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_daily_finalize(envelope["payload"])
    elif kind == KIND_WORK_GRAPH_BUILD:
        if domain_type != WorkGraphBuildPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = WorkGraphBuildPayload(
            request_id=_decode_reference(envelope["payload"], "request_id")
        )
    elif kind == KIND_INVESTMENT_MATERIALIZE:
        if domain_type != InvestmentMaterializePayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = InvestmentMaterializePayload(
            request_id=_decode_reference(envelope["payload"], "request_id")
        )
    elif kind == KIND_INVESTMENT_DISPATCH:
        if domain_type != InvestmentDispatchPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = InvestmentDispatchPayload(
            request_id=_decode_reference(envelope["payload"], "request_id")
        )
    elif kind == KIND_INVESTMENT_CHUNK:
        if domain_type != InvestmentChunkPayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = InvestmentChunkPayload(
            chunk_id=_decode_reference(envelope["payload"], "chunk_id")
        )
    elif kind == KIND_INVESTMENT_FINALIZE:
        if domain_type != InvestmentFinalizePayload.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = InvestmentFinalizePayload(
            run_id=_decode_reference(envelope["payload"], "run_id")
        )
    else:
        payload_type = _REMAINING_PAYLOAD_BY_KIND[kind]
        if domain_type != payload_type.DOMAIN_TYPE:
            raise ContractDecodeError("domain.type does not match job kind")
        payload = _decode_remaining_partition(envelope["payload"], payload_type)

    return Envelope(
        contract_version=version,
        organization_id=organization_id,
        correlation_id=correlation_id,
        idempotency_key=idempotency_key,
        domain=DomainLink(type=domain_type, id=domain_id),
        payload=payload,
    )


def build_envelope(
    payload: ContractPayload,
    *,
    correlation_id: str,
    idempotency_key: str,
    domain_id: str,
    organization_id: str | None = None,
) -> Envelope:
    """Build and validate arguments for the transitional outbox producer."""

    if not isinstance(
        payload,
        (
            HeartbeatPayload,
            BillingNotificationPayload,
            RetentionCleanupPayload,
            OnDemandReportExecutionPayload,
            ScheduledReportExecutionPayload,
            DailyMetricsDispatchPayload,
            DailyMetricsPartitionPayload,
            DailyMetricsFinalizePayload,
            WorkGraphBuildPayload,
            InvestmentMaterializePayload,
            InvestmentDispatchPayload,
            InvestmentChunkPayload,
            InvestmentFinalizePayload,
            WebhookDeliveryPayload,
            *_REMAINING_PAYLOAD_TYPES,
        ),
    ):
        raise ContractDecodeError("unsupported payload type")
    envelope = Envelope(
        contract_version=payload.CONTRACT_VERSION,
        organization_id=organization_id,
        correlation_id=correlation_id,
        idempotency_key=idempotency_key,
        domain=DomainLink(type=payload.DOMAIN_TYPE, id=domain_id),
        payload=payload,
    )
    # A round trip keeps producer and consumer validation identical.
    return decode_envelope(payload.KIND, encode_envelope(envelope))


def encode_envelope(envelope: Envelope) -> bytes:
    """Emit the canonical JSON representation shared with the Go types."""

    document: dict[str, Any] = {"contract_version": envelope.contract_version}
    if envelope.organization_id is not None:
        document["organization_id"] = envelope.organization_id
    document.update(
        {
            "correlation_id": envelope.correlation_id,
            "idempotency_key": envelope.idempotency_key,
            "domain": {"type": envelope.domain.type, "id": envelope.domain.id},
            "payload": _payload_document(envelope.payload),
        }
    )
    encoded = (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode()
    if len(encoded) > MAX_ENVELOPE_BYTES:
        raise ContractDecodeError("encoded envelope exceeds size limit")
    if isinstance(envelope.payload, BillingNotificationPayload):
        kind = KIND_BILLING_NOTIFICATION
    elif isinstance(envelope.payload, WebhookDeliveryPayload):
        kind = KIND_WEBHOOK_DELIVERY
    elif isinstance(envelope.payload, HeartbeatPayload):
        kind = KIND_HEARTBEAT
    elif isinstance(envelope.payload, RetentionCleanupPayload):
        kind = KIND_RETENTION_CLEANUP
    elif isinstance(envelope.payload, OnDemandReportExecutionPayload):
        kind = KIND_REPORT_EXECUTE_ON_DEMAND
    elif isinstance(envelope.payload, ScheduledReportExecutionPayload):
        kind = KIND_REPORT_EXECUTE_SCHEDULED
    elif isinstance(envelope.payload, DailyMetricsDispatchPayload):
        kind = KIND_DAILY_METRICS_DISPATCH
    elif isinstance(envelope.payload, DailyMetricsPartitionPayload):
        kind = KIND_DAILY_METRICS_PARTITION
    elif isinstance(envelope.payload, DailyMetricsFinalizePayload):
        kind = KIND_DAILY_METRICS_FINALIZE
    elif isinstance(envelope.payload, WorkGraphBuildPayload):
        kind = KIND_WORK_GRAPH_BUILD
    elif isinstance(envelope.payload, InvestmentMaterializePayload):
        kind = KIND_INVESTMENT_MATERIALIZE
    elif isinstance(envelope.payload, InvestmentDispatchPayload):
        kind = KIND_INVESTMENT_DISPATCH
    elif isinstance(envelope.payload, InvestmentChunkPayload):
        kind = KIND_INVESTMENT_CHUNK
    elif isinstance(envelope.payload, InvestmentFinalizePayload):
        kind = KIND_INVESTMENT_FINALIZE
    elif isinstance(envelope.payload, _REMAINING_PAYLOAD_TYPES):
        kind = envelope.payload.KIND
    else:
        raise ContractDecodeError("unsupported payload type")
    decode_envelope(kind, encoded)
    return encoded


def load_json_document(data: bytes | str, *, max_bytes: int) -> Any:
    """Load bounded JSON while rejecting duplicate keys and non-finite numbers."""

    if isinstance(data, bytes):
        raw = data
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ContractDecodeError("JSON must be UTF-8") from error
    else:
        text = data
        raw = data.encode("utf-8")
    if not raw:
        raise ContractDecodeError("JSON value is empty")
    if len(raw) > max_bytes:
        raise ContractDecodeError("JSON value exceeds size limit")

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ContractDecodeError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ContractDecodeError("non-finite number is forbidden")

    try:
        document = json.loads(
            text, object_pairs_hook=object_pairs, parse_constant=reject_constant
        )
    except ContractDecodeError:
        raise
    except (json.JSONDecodeError, RecursionError) as error:
        raise ContractDecodeError("invalid JSON") from error
    _validate_depth(document, 0)
    return document


def _decode_heartbeat(value: Any) -> HeartbeatPayload:
    payload = _expect_object(
        value, required={"scheduled_for"}, optional=set(), label="heartbeat payload"
    )
    scheduled_for = _expect_string(payload["scheduled_for"], "scheduled_for")
    _validate_utc_timestamp("scheduled_for", scheduled_for)
    return HeartbeatPayload(scheduled_for=scheduled_for)


def _decode_retention(value: Any) -> RetentionCleanupPayload:
    payload = _expect_object(
        value,
        required={"batch_size", "delete_before", "retention_policy"},
        optional=set(),
        label="retention payload",
    )
    batch_size = _expect_int(payload["batch_size"], "batch_size")
    if not 1 <= batch_size <= 1000:
        raise ContractDecodeError("batch_size is outside its bounds")
    delete_before = _expect_string(payload["delete_before"], "delete_before")
    _validate_utc_timestamp("delete_before", delete_before)
    retention_policy = _expect_string(payload["retention_policy"], "retention_policy")
    if retention_policy != RETENTION_WORKER_TERMINAL:
        raise ContractDecodeError("unsupported retention_policy")
    return RetentionCleanupPayload(
        batch_size=batch_size,
        delete_before=delete_before,
        retention_policy=retention_policy,
    )


def _decode_billing_notification(value: Any) -> BillingNotificationPayload:
    payload = _expect_object(
        value,
        required={"notification_id"},
        optional=set(),
        label="billing notification payload",
    )
    notification_id = _expect_string(payload["notification_id"], "notification_id")
    _validate_uuid("notification_id", notification_id)
    return BillingNotificationPayload(notification_id=notification_id)


def _decode_webhook_delivery(value: Any) -> WebhookDeliveryPayload:
    payload = _expect_object(
        value,
        required={"delivery_id"},
        optional=set(),
        label="webhook delivery payload",
    )
    delivery_id = _expect_string(payload["delivery_id"], "delivery_id")
    _validate_uuid("delivery_id", delivery_id)
    return WebhookDeliveryPayload(delivery_id=delivery_id)


def _decode_on_demand_report_execution(value: Any) -> OnDemandReportExecutionPayload:
    payload = _expect_object(
        value, required={"report_id"}, optional=set(), label="report execution payload"
    )
    report_id = _expect_string(payload["report_id"], "report_id")
    _validate_uuid("report_id", report_id)
    return OnDemandReportExecutionPayload(report_id=report_id)


def _decode_scheduled_report_execution(value: Any) -> ScheduledReportExecutionPayload:
    payload = _expect_object(
        value, required={"report_id"}, optional=set(), label="report execution payload"
    )
    report_id = _expect_string(payload["report_id"], "report_id")
    _validate_uuid("report_id", report_id)
    return ScheduledReportExecutionPayload(report_id=report_id)


def _decode_daily_dispatch(value: Any) -> DailyMetricsDispatchPayload:
    payload = _expect_object(
        value, required={"run_id"}, optional=set(), label="daily dispatch payload"
    )
    run_id = _expect_string(payload["run_id"], "run_id")
    _validate_uuid("run_id", run_id)
    return DailyMetricsDispatchPayload(run_id=run_id)


def _decode_daily_partition(value: Any) -> DailyMetricsPartitionPayload:
    payload = _expect_object(
        value,
        required={"partition_id"},
        optional=set(),
        label="daily partition payload",
    )
    partition_id = _expect_string(payload["partition_id"], "partition_id")
    _validate_uuid("partition_id", partition_id)
    return DailyMetricsPartitionPayload(partition_id=partition_id)


def _decode_daily_finalize(value: Any) -> DailyMetricsFinalizePayload:
    payload = _expect_object(
        value, required={"run_id"}, optional=set(), label="daily finalize payload"
    )
    run_id = _expect_string(payload["run_id"], "run_id")
    _validate_uuid("run_id", run_id)
    return DailyMetricsFinalizePayload(run_id=run_id)


def _decode_reference(value: Any, field: str) -> str:
    payload = _expect_object(
        value, required={field}, optional=set(), label="reference payload"
    )
    reference = _expect_string(payload[field], field)
    _validate_uuid(field, reference)
    return reference


def _decode_remaining_partition(
    value: Any,
    payload_type: type[
        RemainingCapacityPayload
        | RemainingComplexityPayload
        | RemainingDORAPayload
        | RemainingExtraMetricsPayload
        | RemainingMembershipPayload
        | RemainingRecommendationsPayload
        | RemainingReleaseImpactPayload
        | RemainingTeamMetricsPayload
    ],
) -> JobPayload:
    payload = _expect_object(
        value,
        required={"partition_id"},
        optional=set(),
        label="remaining metrics partition payload",
    )
    partition_id = _expect_string(payload["partition_id"], "partition_id")
    _validate_uuid("partition_id", partition_id)
    return payload_type(partition_id=partition_id)


def _payload_document(payload: object) -> dict[str, Any]:
    if isinstance(payload, BillingNotificationPayload):
        return {"notification_id": payload.notification_id}
    if isinstance(payload, WebhookDeliveryPayload):
        return {"delivery_id": payload.delivery_id}
    if isinstance(payload, HeartbeatPayload):
        return {"scheduled_for": payload.scheduled_for}
    if isinstance(payload, RetentionCleanupPayload):
        return {
            "batch_size": payload.batch_size,
            "delete_before": payload.delete_before,
            "retention_policy": payload.retention_policy,
        }
    if isinstance(payload, OnDemandReportExecutionPayload):
        return {"report_id": payload.report_id}
    if isinstance(payload, ScheduledReportExecutionPayload):
        return {"report_id": payload.report_id}
    if isinstance(payload, DailyMetricsDispatchPayload):
        return {"run_id": payload.run_id}
    if isinstance(payload, DailyMetricsPartitionPayload):
        return {"partition_id": payload.partition_id}
    if isinstance(payload, DailyMetricsFinalizePayload):
        return {"run_id": payload.run_id}
    if isinstance(payload, WorkGraphBuildPayload):
        return {"request_id": payload.request_id}
    if isinstance(payload, InvestmentMaterializePayload):
        return {"request_id": payload.request_id}
    if isinstance(payload, InvestmentDispatchPayload):
        return {"request_id": payload.request_id}
    if isinstance(payload, InvestmentChunkPayload):
        return {"chunk_id": payload.chunk_id}
    if isinstance(payload, InvestmentFinalizePayload):
        return {"run_id": payload.run_id}
    if isinstance(payload, _REMAINING_PAYLOAD_TYPES):
        return {"partition_id": payload.partition_id}
    raise ContractDecodeError("unsupported payload type")


def _expect_object(
    value: Any, *, required: set[str], optional: set[str], label: str
) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ContractDecodeError(f"{label} must be an object")
    keys = set(value)
    if not required.issubset(keys):
        raise ContractDecodeError(f"{label} is missing required fields")
    if not keys.issubset(required | optional):
        raise ContractDecodeError(f"{label} has unknown fields")
    return value


def _expect_string(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ContractDecodeError(f"{label} must be a string")
    return value


def _expect_int(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ContractDecodeError(f"{label} must be an integer")
    return value


def _validate_safe_id(label: str, value: str, maximum: int) -> None:
    if not value or len(value) > maximum or not _SAFE_ID.fullmatch(value):
        raise ContractDecodeError(f"{label} must be a bounded safe identifier")


def _validate_uuid(label: str, value: str) -> None:
    if not _UUID.fullmatch(value):
        raise ContractDecodeError(f"{label} must be a lowercase UUID")


def _validate_utc_timestamp(label: str, value: str) -> None:
    if not _RFC3339_UTC.fullmatch(value):
        raise ContractDecodeError(f"{label} must use UTC Z notation")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ContractDecodeError(f"{label} must be an RFC3339 timestamp") from error
    if parsed.tzinfo != UTC:
        raise ContractDecodeError(f"{label} must be UTC")


def _validate_depth(value: Any, depth: int) -> None:
    if depth > _MAX_JSON_DEPTH:
        raise ContractDecodeError("JSON nesting exceeds limit")
    if isinstance(value, dict):
        for child in value.values():
            _validate_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _validate_depth(child, depth + 1)
