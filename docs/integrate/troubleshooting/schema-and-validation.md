---
page_id: int-validation-fail
summary: Diagnose unsupported versions, kinds, fields, enums, timestamps, source-instance, and record-family combinations.
content_type: troubleshooting
owner: platform-api
applicability: current
lifecycle: active
---

# Schema and validation errors

1. Fetch the current server-generated schema.
2. Confirm `schemaVersion`, envelope fields, record kind, and camelCase field names.
3. Remove unrecognized fields; the versioned Customer Push models forbid extras.
4. Validate enums and timestamp ordering.
5. Confirm the record kind is supported for the source system.
6. Confirm git-family repository identifiers belong to the registered source instance.
7. Use each record's external ID to trace the exporter input.

Do not weaken validation to accept a provider-specific field without a versioned canonical contract.
