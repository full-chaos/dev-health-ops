// Package decision is the production decision adapter for investment
// categorization (CHAOS-8712): it asks a typed-question backend (TypeSafe Jev,
// POST /v1/systemone) one score question for each of the 15 canonical
// subcategories plus one evidence question, and turns the typed answers into
// the generative-schema text that the shared categorize validation accepts.
//
// It plugs into package categorize through one seam, categorize.BundleCompleter
// (Completer implements it), and is driven by categorize.CategorizeBundleOnce:
// one call, the same ValidateLLMPayload as the served path, no repair call.
// Nothing in the served path constructs or calls this package.
//
// What is fixed in code, on purpose (no environment override): the rubric
// (decision-support-v1f.json, embedded and pinned by its sha256), the level
// rule (presence-floor:0.4) and the weight map (support-map-v1). They are the
// configuration that the experiment evaluated; a configuration that could
// drift from it would be a category system an operator can change.
//
// The HTTP client is not here: Transport is an interface, implemented by the
// TypeSafe client on the shared LLM HTTP layer of package categorize.
//
// Architecture of the adapter, the shadow phase, the served mode and the two
// tables: .github/docs-legacy/architecture/investment-decision-adapter.md (the
// directory name reads as legacy; it holds the live engineering architecture set).
//
// This code was ported from the experiment package decisioneval (branch
// experiment/decision-categorization-eval, commit cf881a356), Jev parts only.
// The replay oracle (replay_oracle_test.go) is the acceptance gate of any
// change here: a change that moves one stored outcome needs a new
// AdapterVersion.
package decision
