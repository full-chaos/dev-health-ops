from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.billing.audit_service import BillingAuditService
from dev_health_ops.models.billing_audit import BillingAuditLog

logger = logging.getLogger(__name__)


TABLE_CONFIG: dict[str, dict[str, str]] = {
    "subscriptions": {
        "table": "subscriptions",
        "stripe_id_col": "stripe_subscription_id",
    },
    "invoices": {"table": "invoices", "stripe_id_col": "stripe_invoice_id"},
    "refunds": {"table": "refunds", "stripe_id_col": "stripe_refund_id"},
    "billing_subscriptions": {
        "table": "subscriptions",
        "stripe_id_col": "stripe_subscription_id",
    },
    "billing_invoices": {"table": "invoices", "stripe_id_col": "stripe_invoice_id"},
    "billing_refunds": {"table": "refunds", "stripe_id_col": "stripe_refund_id"},
}


@dataclass
class ReconciliationMismatch:
    resource_type: str
    resource_id: uuid.UUID
    stripe_id: str
    field: str
    local_value: Any
    stripe_value: Any
    severity: str


@dataclass
class ReconciliationReport:
    started_at: datetime
    completed_at: datetime
    subscriptions_checked: int
    invoices_checked: int
    refunds_checked: int
    mismatches: list[ReconciliationMismatch]
    missing_local: list[str]
    missing_stripe: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "subscriptions_checked": self.subscriptions_checked,
            "invoices_checked": self.invoices_checked,
            "refunds_checked": self.refunds_checked,
            "mismatches": [asdict(item) for item in self.mismatches],
            "missing_local": self.missing_local,
            "missing_stripe": self.missing_stripe,
        }


class ReconciliationService:
    def __init__(
        self,
        db: AsyncSession,
        stripe_client: Any,
        audit_service: BillingAuditService,
    ):
        self.db = db
        self.stripe_client = stripe_client
        self.audit_service = audit_service

    async def reconcile_subscriptions(
        self, org_id: uuid.UUID | None = None
    ) -> ReconciliationReport:
        started_at = datetime.now(timezone.utc)
        local_rows = await self._fetch_local_rows("subscriptions", org_id)
        stripe_rows = await self._fetch_stripe_rows("subscriptions", org_id)
        report = self._compare("subscription", local_rows, stripe_rows)
        report.started_at = started_at
        report.completed_at = datetime.now(timezone.utc)
        report.subscriptions_checked = len(local_rows)
        await self._log_report(org_id, report)
        return report

    async def reconcile_invoices(
        self,
        org_id: uuid.UUID | None = None,
        since: datetime | None = None,
    ) -> ReconciliationReport:
        started_at = datetime.now(timezone.utc)
        local_rows = await self._fetch_local_rows("invoices", org_id, since)
        stripe_rows = await self._fetch_stripe_rows("invoices", org_id, since)
        report = self._compare("invoice", local_rows, stripe_rows)
        report.started_at = started_at
        report.completed_at = datetime.now(timezone.utc)
        report.invoices_checked = len(local_rows)
        await self._log_report(org_id, report)
        return report

    async def reconcile_all(
        self, org_id: uuid.UUID | None = None
    ) -> ReconciliationReport:
        started_at = datetime.now(timezone.utc)
        await self._log_start(org_id)

        subscriptions = await self.reconcile_subscriptions(org_id=org_id)
        invoices = await self.reconcile_invoices(org_id=org_id)
        refunds_local = await self._fetch_local_rows("refunds", org_id)
        refunds_stripe = await self._fetch_stripe_rows("refunds", org_id)
        refunds = self._compare("refund", refunds_local, refunds_stripe)

        combined = ReconciliationReport(
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            subscriptions_checked=subscriptions.subscriptions_checked,
            invoices_checked=invoices.invoices_checked,
            refunds_checked=len(refunds_local),
            mismatches=[
                *subscriptions.mismatches,
                *invoices.mismatches,
                *refunds.mismatches,
            ],
            missing_local=[
                *subscriptions.missing_local,
                *invoices.missing_local,
                *refunds.missing_local,
            ],
            missing_stripe=[
                *subscriptions.missing_stripe,
                *invoices.missing_stripe,
                *refunds.missing_stripe,
            ],
        )

        await self._log_report(org_id, combined)
        return combined

    async def resolve_mismatch(
        self,
        audit_log_id: uuid.UUID,
        resolution: str,
        actor_id: uuid.UUID,
    ) -> BillingAuditLog | None:
        row = await self.db.get(BillingAuditLog, audit_log_id)
        if row is None:
            return None

        description = f"Mismatch resolved: {resolution}"
        return await self.audit_service.log(
            org_id=row.org_id,
            action="reconciliation.completed",
            resource_type=row.resource_type,
            resource_id=row.resource_id,
            description=description,
            actor_id=actor_id,
            local_state=row.local_state,
            stripe_state=row.stripe_state,
            reconciliation_status="matched",
        )

    async def _fetch_local_rows(
        self,
        table_name: str,
        org_id: uuid.UUID | None = None,
        since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        config = TABLE_CONFIG.get(table_name)
        if config is None:
            logger.warning("Unsupported reconciliation table: %s", table_name)
            return []

        actual_table = config["table"]
        stripe_id_col = config["stripe_id_col"]
        query = (
            f"SELECT id, {stripe_id_col} as stripe_id, status, updated_at, org_id "
            f"FROM {actual_table}"
        )
        params: dict[str, Any] = {}
        where = []
        if org_id is not None:
            where.append("org_id = :org_id")
            params["org_id"] = str(org_id)
        if since is not None:
            where.append("updated_at >= :since")
            params["since"] = since
        if where:
            query = f"{query} WHERE {' AND '.join(where)}"

        try:
            result = await self.db.execute(text(query), params)
            return [dict(row._mapping) for row in result]
        except Exception:
            logger.exception("Failed loading local billing rows from %s", actual_table)
            return []

    async def _fetch_stripe_rows(
        self,
        resource: str,
        org_id: uuid.UUID | None = None,
        since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        try:
            payload = None

            if hasattr(self.stripe_client, resource):
                resource_client = getattr(self.stripe_client, resource)
                if hasattr(resource_client, "list"):
                    payload = resource_client.list(limit=100)

            if payload is None:
                import stripe

                if resource == "subscriptions":
                    payload = stripe.Subscription.list(limit=100)
                elif resource == "invoices":
                    payload = stripe.Invoice.list(limit=100)
                elif resource == "refunds":
                    payload = stripe.Refund.list(limit=100)
                else:
                    return []

            if hasattr(payload, "auto_paging_iter"):
                iterator = payload.auto_paging_iter()
            elif hasattr(payload, "data"):
                iterator = payload.data or []
            elif isinstance(payload, dict):
                iterator = payload.get("data", [])
            elif isinstance(payload, list):
                iterator = payload
            else:
                iterator = []

            rows: list[dict[str, Any]] = []
            for item in iterator:
                item_id = (
                    item.get("id")
                    if isinstance(item, dict)
                    else getattr(item, "id", None)
                )
                status = (
                    item.get("status")
                    if isinstance(item, dict)
                    else getattr(item, "status", None)
                )
                if item_id:
                    rows.append({"id": str(item_id), "status": status})
            return rows
        except Exception:
            logger.exception("Failed loading stripe billing rows for %s", resource)
            return []

    def _compare(
        self,
        resource_type: str,
        local_rows: list[dict[str, Any]],
        stripe_rows: list[dict[str, Any]],
    ) -> ReconciliationReport:
        started_at = datetime.now(timezone.utc)
        local_by_stripe = {
            str(row.get("stripe_id")): row
            for row in local_rows
            if row.get("stripe_id") is not None
        }
        stripe_by_id = {
            str(row.get("id")): row for row in stripe_rows if row.get("id") is not None
        }

        mismatches: list[ReconciliationMismatch] = []
        missing_local: list[str] = []
        missing_stripe: list[str] = []

        for stripe_id, local_row in local_by_stripe.items():
            stripe_row = stripe_by_id.get(stripe_id)
            if stripe_row is None:
                missing_stripe.append(stripe_id)
                continue

            if local_row.get("status") != stripe_row.get("status"):
                mismatches.append(
                    ReconciliationMismatch(
                        resource_type=resource_type,
                        resource_id=uuid.UUID(str(local_row.get("id"))),
                        stripe_id=stripe_id,
                        field="status",
                        local_value=local_row.get("status"),
                        stripe_value=stripe_row.get("status"),
                        severity="critical",
                    )
                )

        for stripe_id in stripe_by_id:
            if stripe_id not in local_by_stripe:
                missing_local.append(stripe_id)

        return ReconciliationReport(
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
            subscriptions_checked=len(local_rows)
            if resource_type == "subscription"
            else 0,
            invoices_checked=len(local_rows) if resource_type == "invoice" else 0,
            refunds_checked=len(local_rows) if resource_type == "refund" else 0,
            mismatches=mismatches,
            missing_local=missing_local,
            missing_stripe=missing_stripe,
        )

    async def _log_start(self, org_id: uuid.UUID | None) -> None:
        if org_id is None:
            return
        await self.audit_service.log(
            org_id=org_id,
            action="reconciliation.started",
            resource_type="reconciliation",
            resource_id=uuid.uuid4(),
            description="Started billing reconciliation",
            reconciliation_status="unresolved",
        )

    async def _log_report(
        self, org_id: uuid.UUID | None, report: ReconciliationReport
    ) -> None:
        if org_id is None:
            return

        for mismatch in report.mismatches:
            await self.audit_service.log(
                org_id=org_id,
                action="reconciliation.mismatch_found",
                resource_type=mismatch.resource_type,
                resource_id=mismatch.resource_id,
                description=(
                    f"Mismatch on {mismatch.field}: "
                    f"local={mismatch.local_value}, stripe={mismatch.stripe_value}"
                ),
                reconciliation_status="mismatch",
                local_state={"field": mismatch.field, "value": mismatch.local_value},
                stripe_state={"field": mismatch.field, "value": mismatch.stripe_value},
            )

        status = "matched"
        action = "reconciliation.completed"
        if report.mismatches or report.missing_local or report.missing_stripe:
            status = "mismatch"
            action = "reconciliation.mismatch_found"
        await self.audit_service.log(
            org_id=org_id,
            action=action,
            resource_type="reconciliation",
            resource_id=uuid.uuid4(),
            description="Completed billing reconciliation run",
            reconciliation_status=status,
            local_state={
                "subscriptions_checked": report.subscriptions_checked,
                "invoices_checked": report.invoices_checked,
                "refunds_checked": report.refunds_checked,
            },
            stripe_state={
                "missing_local": report.missing_local,
                "missing_stripe": report.missing_stripe,
                "mismatch_count": len(report.mismatches),
            },
        )
