# Retired documentation QA evidence

The WIP Playwright, screenshot, exact-prose, visual-regression, and local evidence framework in this directory is retired by Phase 10.

It is not installed or executed by CI. The remaining test source files are historical implementation evidence only and must not be used as a release or content-quality gate. They will be deleted after any useful regression scenario is either discarded or rewritten as a small issue-specific test.

The required documentation pull-request gate now lives in `.github/workflows/docs-guards.yml` and protects only:

- explicit publication and IA invariants;
- strict static build success;
- rendered internal links, anchors, and assets;
- natural-language search acceptance;
- deterministic accessibility structure;
- focused objective fact drift.

Human review remains responsible for real task completion, keyboard and screen-reader behavior, zoom and reflow, responsive navigation, and content/source accuracy. Test count, screenshot count, screenshot hashes, and decorative composition are not quality measures.
