---
page_id: ref-schemas
summary: Versioned public schemas, canonical record kinds, enums, examples, and compatibility rules.
content_type: landing
owner: platform-api
applicability: current
lifecycle: active
---

# Schemas and contracts

- [Record kinds and enums](record-kinds-and-enums.md)

Public schemas must be generated from the same models that validate requests. Examples must be tested against that schema. Internal storage rows are not automatically public wire contracts.

## Ask Dev v1 contracts (removed)

The Ask Dev v1 schemas, fixture manifest and goldens that lived in
`contracts/ask-dev/v1/` (removed with the Python api) were generated from the
Python Dev API. The Go api does not serve that surface, and
nothing in this repository reads those files, so they were deleted. The v2
record is in [Ask Dev v2 contracts](../../contribute/architecture/ask-dev-contracts-v2.md).
