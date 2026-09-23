"""Live-Python oracle for the external-ingest schema bundle.

Executed by TestSchemaBundleMatchesLivePython
(internal/api/externalingest/bundle_oracle_test.go), never digested: this
runs the REAL production functions (schema_registry.get_bundle,
schema_registry.compute_etag) and prints their output as JSON, so a change
to schemas.py or schema_registry.py that the checked-in golden asset
(testdata/schema_bundle.v1.json) has not been regenerated to match fails
this test, not just a stale digest comparison.

Merges the SAME default limits router.py's _limits_payload() would (the
MAX_RECORDS_DEFAULT/MAX_BODY_BYTES_DEFAULT constants), matching
DefaultLimits in deps.go, so the printed etag is directly comparable to
computeETag(schemaDocument(DefaultLimits)) on the Go side.
"""

from __future__ import annotations

import json

from dev_health_ops.api.external_ingest.schema_registry import compute_etag, get_bundle
from dev_health_ops.api.external_ingest.schemas import (
    MAX_BODY_BYTES_DEFAULT,
    MAX_RECORDS_DEFAULT,
    SCHEMA_VERSION,
)

bundle = get_bundle(SCHEMA_VERSION)
assert bundle is not None, f"no bundle for {SCHEMA_VERSION!r}"

document = {
    **bundle.document,
    "limits": {
        "maxRecordsPerBatch": MAX_RECORDS_DEFAULT,
        "maxBodyBytes": MAX_BODY_BYTES_DEFAULT,
    },
}

print(json.dumps({"document": document, "etag": compute_etag(document)}))
