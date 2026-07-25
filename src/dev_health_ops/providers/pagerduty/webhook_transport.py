"""Which runtime owns a staged PagerDuty webhook stream entry.

Two modules have to answer this identically: the FastAPI receiver, which
decides whether to dispatch Celery after staging the entry, and the Celery
task, which must stand down when the Go consumer owns the entry. The receiver
module already imports the task module, so the reader cannot live in either of
them without an import cycle. It lives here, below both, so there is exactly
one definition of the switch rather than two that can drift apart.
"""

from __future__ import annotations

import os
from enum import StrEnum

WEBHOOK_TRANSPORT_ENV = "PAGERDUTY_WEBHOOK_TRANSPORT"


class WebhookTransport(StrEnum):
    CELERY = "celery"
    RIVER = "river"


def resolve_webhook_transport() -> WebhookTransport:
    """Resolve which runtime owns the PagerDuty stream for this deployment.

    Exactly one consumer may drain a stream entry: the Celery task deletes the
    entry it processed, which would strip entries the Go River consumer still
    holds in its pending list. This gate keeps them from racing rather than
    letting both reconcile. Anything unset, empty, or unrecognised resolves to
    ``celery`` so a misconfiguration falls back to today's behaviour instead of
    silently handing the stream to a runtime that may not be deployed.

    Callers must re-read this per operation rather than caching it at import
    time: the value changes under a running process during a cutover.
    """
    value = os.getenv(WEBHOOK_TRANSPORT_ENV, "").strip().lower()
    return (
        WebhookTransport.RIVER
        if value == WebhookTransport.RIVER
        else WebhookTransport.CELERY
    )
