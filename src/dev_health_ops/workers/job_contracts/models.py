"""Typed, language-neutral job contract models for the Python transition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, TypeAlias

CONTRACT_VERSION_V1 = 1
KIND_HEARTBEAT = "system.heartbeat"
KIND_BILLING_NOTIFICATION = "operational.billing_notification"
KIND_WEBHOOK_DELIVERY = "operational.webhook_delivery"
KIND_RETENTION_CLEANUP = "system.retention_cleanup"
KIND_REPORT_EXECUTE_ON_DEMAND = "report.execute_on_demand"
KIND_REPORT_EXECUTE_SCHEDULED = "report.execute_scheduled"
KIND_DAILY_METRICS_DISPATCH = "metrics.daily_dispatch"
KIND_DAILY_METRICS_PARTITION = "metrics.daily_partition"
KIND_DAILY_METRICS_FINALIZE = "metrics.daily_finalize"
KIND_TEAM_AUTOIMPORT = "sync.team_autoimport"
KIND_WORK_GRAPH_BUILD = "workgraph.build"
KIND_INVESTMENT_MATERIALIZE = "investment.materialize"
KIND_INVESTMENT_DISPATCH = "investment.dispatch"
KIND_INVESTMENT_CHUNK = "investment.chunk"
KIND_INVESTMENT_FINALIZE = "investment.finalize"
KIND_REMAINING_CAPACITY = "metrics.remaining.capacity"
KIND_REMAINING_COMPLEXITY = "metrics.remaining.complexity"
KIND_REMAINING_DORA = "metrics.remaining.dora"
KIND_REMAINING_EXTRA_METRICS = "metrics.remaining.extra_metrics"
KIND_REMAINING_MEMBERSHIP = "metrics.remaining.membership_backfill"
KIND_REMAINING_RECOMMENDATIONS = "metrics.remaining.recommendations"
KIND_REMAINING_RELEASE_IMPACT = "metrics.remaining.release_impact"
KIND_REMAINING_TEAM_METRICS = "metrics.remaining.team_metrics"
KIND_SYNC_PROVIDER_UNIT = "sync.provider_unit"
RETENTION_WORKER_TERMINAL = "worker_job_terminal"
MAX_ENVELOPE_BYTES = 16 * 1024


class ContractPayload(Protocol):
    """Minimum adapter surface required by a transitional Python producer."""

    KIND: ClassVar[str]
    CONTRACT_VERSION: ClassVar[int]
    DOMAIN_TYPE: ClassVar[str]


@dataclass(frozen=True, slots=True)
class DomainLink:
    """Reference to authoritative product or schedule state."""

    type: str
    id: str


@dataclass(frozen=True, slots=True)
class HeartbeatPayload:
    """Version 1 payload for the unique heartbeat occurrence."""

    KIND: ClassVar[str] = KIND_HEARTBEAT
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "schedule_occurrence"

    scheduled_for: str


@dataclass(frozen=True, slots=True)
class RetentionCleanupPayload:
    """Version 1 bounded terminal-job retention request."""

    KIND: ClassVar[str] = KIND_RETENTION_CLEANUP
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "maintenance_run"

    batch_size: int
    delete_before: str
    retention_policy: str


@dataclass(frozen=True, slots=True)
class BillingNotificationPayload:
    KIND: ClassVar[str] = KIND_BILLING_NOTIFICATION
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "billing_notification"

    notification_id: str


@dataclass(frozen=True, slots=True)
class WebhookDeliveryPayload:
    KIND: ClassVar[str] = KIND_WEBHOOK_DELIVERY
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "webhook_delivery"

    delivery_id: str


@dataclass(frozen=True, slots=True)
class OnDemandReportExecutionPayload:
    """Version 1 request to execute one already-created manual ReportRun."""

    KIND: ClassVar[str] = KIND_REPORT_EXECUTE_ON_DEMAND
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "report_run"

    report_id: str


@dataclass(frozen=True, slots=True)
class ScheduledReportExecutionPayload:
    """Version 1 request to execute one already-created scheduled ReportRun."""

    KIND: ClassVar[str] = KIND_REPORT_EXECUTE_SCHEDULED
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "report_run"

    report_id: str


@dataclass(frozen=True, slots=True)
class DailyMetricsDispatchPayload:
    KIND: ClassVar[str] = KIND_DAILY_METRICS_DISPATCH
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "daily_metrics_run"

    run_id: str


@dataclass(frozen=True, slots=True)
class DailyMetricsPartitionPayload:
    KIND: ClassVar[str] = KIND_DAILY_METRICS_PARTITION
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "daily_metrics_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class DailyMetricsFinalizePayload:
    KIND: ClassVar[str] = KIND_DAILY_METRICS_FINALIZE
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "daily_metrics_run"

    run_id: str


@dataclass(frozen=True, slots=True)
class TeamAutoimportPayload:
    """Reference to the successful SyncRun that owns the import inputs."""

    KIND: ClassVar[str] = KIND_TEAM_AUTOIMPORT
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "sync_run"

    sync_run_id: str


@dataclass(frozen=True, slots=True)
class WorkGraphBuildPayload:
    """Reference to a server-owned work-graph request, never source evidence."""

    KIND: ClassVar[str] = KIND_WORK_GRAPH_BUILD
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "work_graph_request"
    request_id: str


@dataclass(frozen=True, slots=True)
class InvestmentMaterializePayload:
    KIND: ClassVar[str] = KIND_INVESTMENT_MATERIALIZE
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "investment_request"
    request_id: str


@dataclass(frozen=True, slots=True)
class InvestmentDispatchPayload:
    KIND: ClassVar[str] = KIND_INVESTMENT_DISPATCH
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "investment_request"
    request_id: str


@dataclass(frozen=True, slots=True)
class InvestmentChunkPayload:
    KIND: ClassVar[str] = KIND_INVESTMENT_CHUNK
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "investment_chunk"
    chunk_id: str


@dataclass(frozen=True, slots=True)
class InvestmentFinalizePayload:
    KIND: ClassVar[str] = KIND_INVESTMENT_FINALIZE
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "investment_run"
    run_id: str


@dataclass(frozen=True, slots=True)
class RemainingCapacityPayload:
    KIND: ClassVar[str] = KIND_REMAINING_CAPACITY
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingComplexityPayload:
    KIND: ClassVar[str] = KIND_REMAINING_COMPLEXITY
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingDORAPayload:
    KIND: ClassVar[str] = KIND_REMAINING_DORA
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingExtraMetricsPayload:
    KIND: ClassVar[str] = KIND_REMAINING_EXTRA_METRICS
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingMembershipPayload:
    KIND: ClassVar[str] = KIND_REMAINING_MEMBERSHIP
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingRecommendationsPayload:
    KIND: ClassVar[str] = KIND_REMAINING_RECOMMENDATIONS
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingReleaseImpactPayload:
    KIND: ClassVar[str] = KIND_REMAINING_RELEASE_IMPACT
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class RemainingTeamMetricsPayload:
    KIND: ClassVar[str] = KIND_REMAINING_TEAM_METRICS
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "remaining_metric_partition"

    partition_id: str


@dataclass(frozen=True, slots=True)
class ProviderUnitPayload:
    """ID-only request to execute one authoritative SyncRunUnit."""

    KIND: ClassVar[str] = KIND_SYNC_PROVIDER_UNIT
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "sync_run_unit"

    unit_id: str


JobPayload: TypeAlias = (
    BillingNotificationPayload
    | WebhookDeliveryPayload
    | HeartbeatPayload
    | RetentionCleanupPayload
    | OnDemandReportExecutionPayload
    | ScheduledReportExecutionPayload
    | DailyMetricsDispatchPayload
    | DailyMetricsPartitionPayload
    | DailyMetricsFinalizePayload
    | TeamAutoimportPayload
    | WorkGraphBuildPayload
    | InvestmentMaterializePayload
    | InvestmentDispatchPayload
    | InvestmentChunkPayload
    | InvestmentFinalizePayload
    | RemainingCapacityPayload
    | RemainingComplexityPayload
    | RemainingDORAPayload
    | RemainingExtraMetricsPayload
    | RemainingMembershipPayload
    | RemainingRecommendationsPayload
    | RemainingReleaseImpactPayload
    | RemainingTeamMetricsPayload
    | ProviderUnitPayload
)


@dataclass(frozen=True, slots=True)
class Envelope:
    """Strict common envelope carried inside River ``encoded_args``."""

    contract_version: int
    organization_id: str | None
    correlation_id: str
    idempotency_key: str
    domain: DomainLink
    payload: JobPayload
