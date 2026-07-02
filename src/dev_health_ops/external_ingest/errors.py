"""Worker-side processing exception hierarchy (CHAOS-2693 D5).

Pinned interface contract between this issue's consumer
(``api/external_ingest/consumer.py``) and CHAOS-2697's worker
(``external_ingest/processor.py::process_batch``), documented here rather
than inline in the consumer so both issues import the same canonical type
instead of each declaring their own (master-spec CC11/CC23).

``process_batch`` must raise ``PermanentProcessingError`` for failures that
can never succeed on retry (unsupported schema version, an envelope shape
that survived API-layer validation but is structurally invalid) -- the
consumer routes these straight to the DLQ, no reclaim. Any other exception
is treated conservatively as transient (connection errors, timeouts,
unclassified bugs) -- the consumer leaves the entry un-ACKed for
``reclaim_stale()`` to retry, up to ``max_deliveries``.
"""

from __future__ import annotations


class PermanentProcessingError(Exception):
    """Non-retryable batch-processing failure: DLQ immediately, no reclaim."""


__all__ = ["PermanentProcessingError"]
