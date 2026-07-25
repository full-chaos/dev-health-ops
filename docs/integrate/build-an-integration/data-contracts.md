---
page_id: int-contracts
summary: Normalize records into stable canonical identities, enums, relationships, and timestamps.
content_type: concept
owner: engineering
applicability: current
lifecycle: active
---

# Use canonical data contracts

A canonical record must define:

- organization or tenant boundary;
- provider and source instance;
- stable natural identity and version;
- event and synchronization timestamps;
- normalized status or enum plus raw provider value where required;
- repository, team, identity, and work relationships;
- deletion, replay, and partial-update semantics;
- provenance and coverage.

Generate exact schemas and enums from code. Do not duplicate them in narrative integration guides.
