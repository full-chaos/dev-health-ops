# AI Operating Leverage demo narrative

AI Operating Leverage is the buyer-facing explanation for whether AI-assisted engineering work appears to create system lift or move cost elsewhere. It is intentionally decomposable: the product shows the signed components that contribute to the net readout instead of presenting a black-box productivity score.

Use this narrative for demos, sales walkthroughs, and product copy tied to AI Workflow Intelligence.

## Positioning

> Adopt AI coding agents with evidence, controls, and system-health feedback loops — without per-author AI scoring, raw prompt/session capture, or individual reviewer ranking.

The operating question is:

> Are AI-assisted workflows improving delivery, or are they shifting cost into review, rework, quality risk, and governance gaps?

The walkthrough should follow one path:

```text
Home -> AI Impact -> Review Load / Risk -> Governance gaps -> Evidence -> Recommended intervention
```

## Component copy

AI Operating Leverage should be explained as a component breakdown:

| Component            | Buyer-facing meaning                                                                                  | What to say                                                                                      |
| -------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Delivery lift        | AI-attributed work appears to improve throughput or cycle time against the comparable human baseline. | "AI may be helping flow here, but we still inspect the drag components before calling it a win." |
| Review amplification | AI-attributed PRs require more review comments, rounds, or reviewer attention.                        | "The system may be moving effort from coding into review."                                       |
| Rework drag          | AI-attributed work shows higher post-review changes, reopens, or churn.                               | "The first draft may be cheaper, but iteration cost is rising."                                  |
| Defect / revert drag | AI-attributed work correlates with test gaps, reverts, or quality regressions.                        | "Delivery speed is not enough if confidence drops."                                              |
| Incident drag        | AI-attributed work overlaps with incident or change-failure signals.                                  | "Operational cost may be showing up after merge."                                                |
| Unknown attribution  | Attribution stayed unresolved because evidence was insufficient.                                      | "This is a trust signal. We show the coverage gap instead of guessing."                          |
| Governance coverage  | Declaration, review, security, license, and tool-policy controls are present or missing.              | "Controls are visible as system coverage, not as a person-level compliance score."               |

Do not call the net value an "AI productivity score." The net value is a routing signal for investigation, and the components are the product.

## Demo scenarios

### 1. Positive lift

**Setup:** AI-assisted share rises while cycle time improves and drag components stay flat.

**Narrative:** "This team appears to be getting delivery lift without visible review or quality cost. The next step is to open the evidence and identify which workflows are safe candidates for broader adoption."

**Intervention:** Expand the successful pattern to similar repos, then watch review amplification and unknown attribution over the next operating review.

### 2. Hidden review amplification

**Setup:** AI-assisted PR volume rises, but reviews per PR and review latency rise faster than the human baseline.

**Narrative:** "AI may be accelerating draft creation while concentrating work on reviewers. The bottleneck moved; it did not disappear."

**Intervention:** Add review guidelines for AI-attributed PRs, tighten PR sizing, and rotate reviewers before increasing agent usage.

### 3. Rework drag

**Setup:** Cycle time looks better at first, but post-review pushes, churn, or reopen rates rise.

**Narrative:** "The first pass is faster, but the system is paying through iteration. We should inspect the evidence before treating this as delivery improvement."

**Intervention:** Add acceptance-check prompts, require test evidence, or constrain agent usage to lower-risk work types.

### 4. Incident drag

**Setup:** AI-attributed changes overlap with reverts, incidents, or change-failure signals.

**Narrative:** "The delivery gain is being offset by operational risk. AI usage may need stronger release or ownership controls in this scope."

**Intervention:** Route AI-attributed changes in affected services through additional review, ownership checks, or staged rollout policies.

### 5. Unknown attribution as a trust signal

**Setup:** The unknown bucket is high or rising.

**Narrative:** "We are not going to guess. Unknown attribution means labels, trailers, bot identities, or CI annotations are incomplete. The dashboard stays honest by preserving that gap."

**Intervention:** Fix detection coverage first: standardize labels, register bot identities, and add commit trailer guidance.

### 6. Governance violations

**Setup:** AI-attributed work has missing human review, missing security/license checks, or out-of-policy model/tool usage.

**Narrative:** "The issue is not who used AI. The issue is whether the system has enough controls for AI-attributed work."

**Intervention:** Close the specific policy gap, then verify the next operating review shows improved governance coverage.

## Guardrails for demos

- Never show per-author AI adoption, per-person productivity, or reviewer rankings.
- Never imply raw prompts, IDE sessions, or private tool usage are captured.
- Keep language directional: "appears," "suggests," "may indicate." Avoid verdict language.
- Treat every recommendation as a testable intervention with a follow-up metric.

## Recommended talk track

1. Start on Home and select **AI Workflow Intelligence** as the market-entry path.
2. Open **AI Impact** to compare delivery lift with review, rework, quality, incident, unknown-attribution, and governance components.
3. If lift is positive, validate that drag and governance gaps are not hiding the cost.
4. If drag is rising, open **Review Load** or **AI Risk** to localize the pressure.
5. Treat **unknown attribution** as a product trust moment: Dev Health preserves uncertainty rather than guessing.
6. End in the evidence trail and operating review with one recommended intervention and one follow-up measure.
