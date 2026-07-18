# Documentation editorial and accessibility style

The goal is precision. Documentation should make the supported task obvious, provide the minimum context required to do it safely, and then get out of the reader's way.

## Voice

* Use direct, calm, concrete language.
* Lead with the reader's outcome or question.
* Prefer verbs over category nouns: **Connect GitHub**, not **GitHub integration configuration**.
* State supported behavior before edge cases.
* Use short paragraphs and informative headings.
* Do not narrate the implementation history, project history, or why a documentation page exists.
* Brand personality belongs in restraint, confidence, and visual identity—not jokes in failure, security, or destructive procedures.

## Titles

Use titles that name the task or lookup.

Good:

* Connect GitHub
* Set scope and time window
* Diagnose incomplete data
* Investment distribution fields
* Upgrade Dev Health

Avoid:

* Your first ten minutes
* Getting familiar with Dev Health
* Everything you need to know about reports
* Evidence-led operations manual
* Investment View Product Specification

`/get-started/` is not permission to write onboarding theatre. It is a provisional task router with newly authored minimal concepts and prerequisites.

## Procedures

1. State the outcome.
2. Name prerequisites and required role.
3. Give the shortest supported path.
4. Show the expected result.
5. Link the likely failure path.
6. Link exact reference only when needed.

Use numbered steps only for sequence. Use bullets for options or facts. Put irreversible warnings before the action, not after it.

## Product and UI language

* Use current product labels exactly when telling a reader what to select.
* Use code identifiers only in reference or contributor content.
* Verify routes, permissions, availability, and prerequisites against current sources.
* Do not document planned or removed behavior as current.
* When a label is likely to change, title the page after the stable task and mention the current label in the procedure.

## Analytics and AI-derived signals

* Distinguish observed facts, derived measures, model-assisted estimates, generated narrative, and unavailable data.
* Use calibrated verbs such as **appears**, **suggests**, or **is associated with** only when uncertainty exists.
* Do not dilute exact facts with unnecessary hedging.
* Never convert a team/workflow signal into a conclusion about an individual.
* State what the reader should not conclude.
* Link to evidence, coverage, confidence, and exact calculation reference.

## State distinctions

These are never interchangeable:

* `0` — measured zero;
* `null` or unavailable — no supported value;
* incomplete — some required input is missing;
* stale — a value exists but is older than expected;
* delayed — processing has not completed;
* estimated or derived — calculated from available evidence;
* unsupported — the product does not provide the behavior;
* no detected association — analysis found no supported association in the selected scope.

## Code, commands, and examples

* Use placeholders that are visibly placeholders.
* Never include real credentials, customer data, hostnames, or tokens.
* Show the command separately from representative output.
* State the required working directory and environment when not obvious.
* Include expected exit or success behavior for high-risk commands.
* Avoid unbounded destructive examples.
* Validate syntax or contract shape when feasible.

## Links

* Link text names the destination or task.
* Do not use “click here,” “learn more,” or repeated generic “see also.”
* Link tasks to the exact concept, reference, and troubleshooting required.
* Avoid reciprocal-link noise.
* A contextual link does not create another canonical navigation placement.

## Screenshots and diagrams

Use a visual only when it reduces reader effort.

Every published visual has:

* a reader purpose;
* meaningful alt text or a text equivalent;
* source environment and product revision;
* sanitization record;
* owner and review trigger.

Do not fabricate UI screenshots. Do not use image hashes as a quality signal. Crop to the task, not the whole application. Avoid instructions based only on color or visual position.

## Accessibility

* Use one H1 and a logical heading hierarchy.
* Write meaningful link text.
* Give tables header cells and context.
* Provide text alternatives for diagrams.
* Do not communicate state by color alone.
* Do not require hover, drag, or pointer precision.
* Describe controls by accessible name, not only “the button on the right.”
* Keep keyboard commands supplemental to an ordinary path.
* Use admonitions sparingly and label the reason: prerequisite, caution, destructive action, security, or deprecation.

## Review checklist

A reviewer should be able to answer yes to all applicable questions:

* Is the reader task clear from the title and opening?
* Is the page in its one canonical location?
* Is the current product path accurate?
* Are prerequisites, role, scope, and applicability visible?
* Are exact facts sourced or generated?
* Are null, zero, stale, incomplete, and unsupported states distinct?
* Are failure, verification, and escalation present for high-risk work?
* Is the page shorter or more direct than the source material it replaces?
* Are visuals necessary, accessible, and sanitized?
* Does the page avoid internal project language and duplicated reference?
