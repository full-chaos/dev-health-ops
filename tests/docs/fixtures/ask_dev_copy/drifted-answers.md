---
page_id: fixture-ask-dev-answers-drifted
summary: RED fixture -- a deliberately drifted copy of the Ask Dev answers page.
---

# Fixture: drifted Ask Dev answer copy

This file is NOT published. It exists so the published-copy drift guard's failure
path stays proven in CI rather than proven once, by hand, on the day it was written.

Two strings below are deliberately wrong against the runtime constants:

* the `refused` display label says "cannot do" where the source says "can do";
* the `denied` canonical sentence says "permission" where the source says "access".

Everything else matches, so a test asserting exactly two errors also proves the guard
does not fire on the strings that are correct.

<!-- BEGIN ASK-DEV OUTCOME LABELS -->
| Outcome | Shown as | What it means |
| --- | --- | --- |
| `answered` | Answered | authored explanation, not checked |
| `answered_with_gaps` | Answered with some gaps | authored explanation, not checked |
| `needs_clarification` | Needs clarification | authored explanation, not checked |
| `not_found` | Not found | authored explanation, not checked |
| `temporarily_unavailable` | Temporarily unavailable | authored explanation, not checked |
| `unsupported` | Not supported yet | authored explanation, not checked |
| `denied` | Not permitted | authored explanation, not checked |
| `refused` | Not something Ask Dev cannot do | authored explanation, not checked |
| `failed` | Something went wrong | authored explanation, not checked |
<!-- END ASK-DEV OUTCOME LABELS -->

<!-- BEGIN ASK-DEV REFUSAL COPY -->
| You asked it to | Outcome | What you see | Suggested next step |
| --- | --- | --- | --- |
| Run a command | `refused` | *Ask Dev can only read and summarize your data; it can't run commands or make changes.* | *Ask a read-only question about your data instead.* |
| Fetch a URL | `unsupported` | *This question is not supported yet.* | *Try a status, health, or metric question instead.* |
<!-- END ASK-DEV REFUSAL COPY -->

<!-- BEGIN ASK-DEV NO-ANSWER COPY -->
| Outcome | You see | Suggested next step |
| --- | --- | --- |
| `not_found` | *No matching subject was found for this question.* | *Check the name and try again.* |
| `temporarily_unavailable` | *This answer is temporarily unavailable. Please try again shortly.* | *Try the question again in a few minutes.* |
| `denied` | *You do not have permission to ask about this.* | *Ask an administrator for access to this area.* |
| `failed` | *Something went wrong while preparing this answer.* | *Try the question again.* |
<!-- END ASK-DEV NO-ANSWER COPY -->

Withheld: <!-- BEGIN ASK-DEV WITHHELD COPY -->*This part of the answer could not be shown.*<!-- END ASK-DEV WITHHELD COPY -->
