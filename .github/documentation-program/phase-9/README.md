# Phase 9 publication boundary

Phase 9 consolidates the documentation candidate and separates it from legacy and internal material.

## Public candidate

`docs-prototype/` is the only source included by `mkdocs.prototype.yml`. The directory name remains temporary implementation detail until the Phase 11 publication ADR and cutover.

Every Markdown page in that source must appear exactly once in navigation. The Phase 9 validator fails on off-navigation Markdown, duplicate navigation paths, missing pages, duplicate page IDs, candidate URLs outside the approved IA, broken local links, redirect conflicts, or redirect targets outside the IA.

## Legacy baseline

The existing `docs/` tree and live Workers preview remain WIP/legacy evidence. They are not silently copied into the candidate. `redirects.tsv` records the intended canonical destination for every current public source that survives as a move, merge, split, rewrite, or generated reference.

## Internal-only material

Project PRDs, IA drafts, disposition records, QA specifications, browser evidence, screenshot contracts, fixture receipts, implementation plans, and design explorations stay under `.github/documentation-program/`, `docs-qa/`, or another explicitly internal repository location. They are absent from the public candidate and search index.

## Deferred runtime work

Phase 9 does not activate redirects, domains, headers, indexing, Access, or production publication. Those remain Phase 11 responsibilities after the Phase 10 quality gate.
