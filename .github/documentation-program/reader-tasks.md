# Representative documentation tasks

These task statements are intentionally plain. They are the minimum set used for navigation, search, prototype, and launch reviews.

| ID | Reader | Task statement | Risk | Expected domain |
| --- | --- | --- | --- | --- |
| T01 | New product user | Understand what Dev Health is before interpreting a result | Medium | Get started |
| T02 | Product user | Choose the view that answers a delivery-flow question | Medium | Use Dev Health |
| T03 | Product user | Set the correct repository, team, and time window | High | Use Dev Health |
| T04 | Product user | Explain why an Investment result is a distribution rather than a ticket count | High | Use Dev Health |
| T05 | Product user | Follow an Investment result to supporting work evidence | High | Use Dev Health |
| T06 | Product user | Understand why a view has no or incomplete data | High | Use Dev Health |
| T07 | Engineering leader | Compare trends without ranking individual people | High | Use Dev Health |
| T08 | Engineering leader | Create and interpret a recurring report with provenance | High | Use Dev Health |
| T09 | Workspace administrator | Configure the workspace, teams, and repository scope | High | Administer Dev Health |
| T10 | Workspace administrator | Connect GitHub with the minimum required permissions | High | Administer Dev Health |
| T11 | Workspace administrator | Rotate or revoke a provider credential | Critical | Administer Dev Health |
| T12 | Workspace administrator | Diagnose a sync or coverage problem before escalating | High | Administer Dev Health |
| T13 | Operator | Decide whether the environment is suitable for production | Critical | Install and operate |
| T14 | Operator | Install Dev Health and verify the first healthy state | Critical | Install and operate |
| T15 | Operator | Upgrade safely with backups and a rollback decision | Critical | Install and operate |
| T16 | Operator | Diagnose worker, queue, ingestion, or database failure | Critical | Install and operate |
| T17 | Operator | Restore a known-good environment after a failed change | Critical | Install and operate |
| T18 | Integrator | Register a Customer Push source and discover the supported schema | High | Integrate and extend |
| T19 | Integrator | Submit data idempotently and interpret an error response | High | Integrate and extend |
| T20 | Integrator | Verify and replay a webhook safely | High | Integrate and extend |
| T21 | API developer | Find the exact GraphQL field, filter, nullability, and error behavior | High | Reference |
| T22 | API developer | Find configuration defaults, secret handling, and reload behavior | High | Reference |
| T23 | Contributor | Choose the correct repository and set up a development environment | Medium | Contribute |
| T24 | Contributor | Run the appropriate test and release workflow for a change | High | Contribute |
| T25 | Security reviewer | Find credential, privacy, data-retention, and incident responsibilities | Critical | Admin / Operate / Reference |
| T26 | Customer | Find future Context Fabric guidance once the product surface is supported | Future | Reserved; no live node yet |

## Review rules

* Primary tasks must be reachable through navigation without search.
* Search queries use the reader's words, error messages, and product labels—not exact document titles only.
* A high-risk task must include prerequisites, failure states, verification, and escalation.
* The vertical slice must test T01–T06 before bulk migration.
