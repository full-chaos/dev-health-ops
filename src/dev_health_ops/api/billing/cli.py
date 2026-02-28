from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime

from dev_health_ops.api.billing.audit_service import BillingAuditService
from dev_health_ops.api.billing.reconciliation_service import ReconciliationService
from dev_health_ops.api.billing.stripe_client import get_stripe_client
from dev_health_ops.db import get_postgres_session


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    billing_parser = subparsers.add_parser("billing", help="Billing operations.")
    billing_subparsers = billing_parser.add_subparsers(
        dest="billing_command", required=True
    )

    reconcile_parser = billing_subparsers.add_parser(
        "reconcile", help="Run billing reconciliation."
    )
    reconcile_parser.add_argument("--org-id", dest="org_id", default=None)
    reconcile_parser.add_argument("--since", dest="since", default=None)
    reconcile_parser.set_defaults(func=run_reconcile)


async def run_reconcile(ns: argparse.Namespace) -> int:
    org_id = uuid.UUID(ns.org_id) if ns.org_id else None
    since = datetime.fromisoformat(ns.since) if ns.since else None

    async with get_postgres_session() as db:
        audit_service = BillingAuditService(db)
        service = ReconciliationService(db, get_stripe_client(), audit_service)
        report = await service.reconcile_all(org_id=org_id)
        if since is not None:
            invoices_report = await service.reconcile_invoices(
                org_id=org_id, since=since
            )
            report.invoices_checked = invoices_report.invoices_checked

    print(json.dumps(report.to_dict(), default=str))
    return 0
