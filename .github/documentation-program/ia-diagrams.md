# IA diagrams and migration topology

These diagrams are explanatory views of the TSV files in `.github/documentation-program/ia/`. The TSV manifest is authoritative for node IDs and URLs.

## 1. Public site tree

```mermaid
flowchart TD
  HOME["/ — Documentation home"]
  GS["/get-started/ — provisional"]
  USE["/use/"]
  ADMIN["/admin/"]
  OPERATE["/operate/"]
  INTEGRATE["/integrate/"]
  REF["/reference/"]
  CONTRIB["/contribute/"]

  HOME --> GS
  HOME --> USE
  HOME --> ADMIN
  HOME --> OPERATE
  HOME --> INTEGRATE
  HOME --> REF
  HOME --> CONTRIB

  GS --> GS_TASK["Choose a task"]
  GS --> GS_PRE["Prerequisites"]
  GS --> GS_CON["Minimal concepts"]
  GS --> GS_GLOSS["Glossary"]

  USE --> U_NAV["Navigate and set context"]
  USE --> U_INV["Investment"]
  USE --> U_FLOW["Delivery flow"]
  USE --> U_CODE["Code and relationships"]
  USE --> U_PLAN["Plan and improve"]
  USE --> U_AI["AI workflows"]
  USE --> U_REPORT["Reports"]
  USE --> U_TROUBLE["Troubleshooting"]

  ADMIN --> A_WORK["Workspace"]
  ADMIN --> A_IDENT["Teams and identities"]
  ADMIN --> A_SRC["Data sources"]
  ADMIN --> A_SYNC["Sync and coverage"]
  ADMIN --> A_SEC["Security and audit"]
  ADMIN --> A_TROUBLE["Troubleshooting"]

  OPERATE --> O_PLAN["Plan"]
  OPERATE --> O_INSTALL["Install"]
  OPERATE --> O_CONFIG["Configure"]
  OPERATE --> O_MAINTAIN["Maintain"]
  OPERATE --> O_RUN["Run"]
  OPERATE --> O_OBSERVE["Observe"]
  OPERATE --> O_SECURITY["Security"]
  OPERATE --> O_RUNBOOKS["Runbooks"]

  INTEGRATE --> I_PUSH["Customer Push"]
  INTEGRATE --> I_API["Supported APIs"]
  INTEGRATE --> I_WEBHOOK["Webhooks"]
  INTEGRATE --> I_BUILD["Build an integration"]
  INTEGRATE --> I_TROUBLE["Troubleshooting"]

  REF --> R_API["API"]
  REF --> R_GQL["GraphQL"]
  REF --> R_CLI["CLI"]
  REF --> R_CONFIG["Configuration"]
  REF --> R_SCHEMA["Schemas"]
  REF --> R_MODEL["Data models"]
  REF --> R_METRIC["Metrics"]
  REF --> R_TAX["Taxonomies"]
  REF --> R_LIMIT["Limits and compatibility"]
  REF --> R_DEP["Deprecations"]

  CONTRIB --> C_START["Start"]
  CONTRIB --> C_ARCH["Architecture"]
  CONTRIB --> C_DEV["Develop and test"]
  CONTRIB --> C_REVIEW["Review and release"]
  CONTRIB --> C_DOCS["Documentation"]
```

## 2. Audience to task domain

```mermaid
flowchart LR
  USER["Product user"] --> USE
  LEADER["Engineering leader"] --> USE
  ADMINR["Workspace administrator"] --> ADMIN
  OP["Platform operator"] --> OPERATE
  INT["Integrator"] --> INTEGRATE
  API["API developer"] --> INTEGRATE
  API --> REF
  DEV["Contributor"] --> CONTRIB
  SEC["Security reviewer"] --> ADMIN
  SEC --> OPERATE
  SEC --> REF
  NEW["New reader"] --> GS["Get started (provisional)"]
  GS -. next real task .-> USE
  GS -. next real task .-> ADMIN
  GS -. next real task .-> OPERATE
  GS -. next real task .-> INTEGRATE
```

## 3. Representative first-use and Investment journey

```mermaid
flowchart LR
  ROOT["/"] --> GS["/get-started/"]
  GS --> CHOOSE["Choose a task"]
  CHOOSE --> INV["Investigate where effort goes"]
  INV --> MIX["Read Investment Mix"]
  MIX --> EVIDENCE["Follow investment evidence"]
  MIX --> METRIC["Reference: weighting and aggregation"]
  EVIDENCE --> NODATA["Troubleshoot incomplete data"]
  NODATA --> ADMIN_SYNC["Admin: sync and coverage"]
  NODATA --> OP_RUNBOOK["Operate: ingestion runbook"]
```

## 4. Content-placement decision tree

```mermaid
flowchart TD
  START["New or changed material"] --> PUBLIC{"Supported public reader need?"}
  PUBLIC -- No --> INTERNAL["Internal plan, PRD, QA, evidence, or implementation record"]
  PUBLIC -- Yes --> INTENT{"Primary intent?"}
  INTENT -- Complete a task --> DOMAIN{"Which task domain owns the outcome?"}
  DOMAIN --> TASK["Task or workflow page in that domain"]
  INTENT -- Diagnose failure --> TROUBLE["Troubleshooting beside the task/domain"]
  INTENT -- Understand concept --> CONCEPT["One canonical concept page"]
  INTENT -- Look up exact fact --> REFERENCE["Reference"]
  INTENT -- Change the product --> CONTRIBUTOR["Contribute"]
  TASK --> EXISTS{"Canonical page already exists?"}
  CONCEPT --> EXISTS
  REFERENCE --> EXISTS
  EXISTS -- Yes --> MERGE["Extend or merge; add contextual links"]
  EXISTS -- No --> NEW["Create one canonical page and URL"]
```

## 5. Current-to-target migration

```mermaid
flowchart LR
  CURRENT["Current WIP sources"] --> INVENTORY["Phase 1 inventory"]
  INVENTORY --> DISPOSITION{"Disposition"}
  DISPOSITION --> RETAIN["Retain facts"]
  DISPOSITION --> REWRITE["Rewrite from blank task brief"]
  DISPOSITION --> MOVE["Move to canonical URL"]
  DISPOSITION --> MERGE["Merge duplicate pages"]
  DISPOSITION --> SPLIT["Split task and reference"]
  DISPOSITION --> INTERNAL["Internal-only"]
  DISPOSITION --> ARCHIVE["Archive or deprecate"]
  MOVE --> REDIRECT["Redirect manifest"]
  MERGE --> REDIRECT
  SPLIT --> REDIRECT
  RETAIN --> REVIEW["Source, IA, editorial, accessibility review"]
  REWRITE --> REVIEW
  MOVE --> REVIEW
  MERGE --> REVIEW
  SPLIT --> REVIEW
  REVIEW --> PUBLIC["Approved public site"]
```

## 6. Page lifecycle and ownership

```mermaid
stateDiagram-v2
  [*] --> Proposed
  Proposed --> Classified
  Classified --> Draft
  Draft --> Review
  Review --> Active: approved
  Review --> Draft: changes required
  Active --> Review: product or source change
  Active --> Deprecated: supported replacement exists
  Deprecated --> Archived: retention period ends
  Active --> Archived: no longer supported and no public task remains
  Archived --> [*]
```

## 7. Hosting-neutral publication flow

```mermaid
flowchart LR
  SOURCE["Canonical source"] --> BUILD["Strict MkDocs build"]
  BUILD --> CHECKS["Lean reader-critical checks"]
  CHECKS --> PREVIEW["Review preview"]
  PREVIEW --> HUMAN["Human IA, content, design, accessibility review"]
  HUMAN --> APPROVE{"Approved?"}
  APPROVE -- No --> SOURCE
  APPROVE -- Yes --> ARTIFACT["Immutable or reproducible artifact"]
  ARTIFACT --> PROD["Production deployment"]
  PROD --> SMOKE["Critical navigation, search, URL, header, and task smoke"]
  SMOKE --> GOOD{"Healthy?"}
  GOOD -- Yes --> MONITOR["Availability and feedback monitoring"]
  GOOD -- No --> ROLLBACK["Restore last known good"]
  ROLLBACK --> SMOKE
```

## 8. Canonical URL and redirect topology

```mermaid
flowchart LR
  APP["Application help links"] --> CANON["docs.fullchaos.dev"]
  README["Repository READMEs"] --> CANON
  SEARCH["Search engines"] --> CANON
  OLD_WORKER["Current Workers preview"] --> POLICY{"Phase 11 disposition"}
  OLD_GHP["Legacy GitHub Pages"] --> REDIRECTS["Redirect manifest"]
  OLD_PATH["Moved or merged paths"] --> REDIRECTS
  REDIRECTS --> CANON
  POLICY -- redirect --> CANON
  POLICY -- archive/noindex --> ARCHIVE["Non-canonical retained preview"]
  POLICY -- remove --> GONE["Retired"]
```

## 9. Vertical-slice dependency graph

```mermaid
flowchart TD
  P1["Phase 1: inventory and tasks"] --> P2["Phase 2: locked IA and URLs"]
  P2 --> P3["Phase 3: content model and templates"]
  P2 --> P4["Phase 4: layout and style"]
  P3 --> SLICE["Get started + Investment vertical slice"]
  P4 --> SLICE
  SLICE --> TEST["Independent task, search, accessibility, and source review"]
  TEST --> DECIDE{"Scale migration?"}
  DECIDE -- No --> P2
  DECIDE -- No --> P3
  DECIDE -- No --> P4
  DECIDE -- Yes --> MIGRATE["Phases 6–9 migration"]
```
