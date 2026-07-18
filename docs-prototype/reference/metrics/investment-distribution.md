# Investment distribution

**Type:** Derived metric distribution  
**Applicability:** Current supported Investment workflow  
**Reader:** Product users who need the exact calculation; API and contributor readers who need the contract

## Meaning

An Investment result is a probability distribution over supported categories aggregated with an effort value. It is not the percentage of tickets and is not a manual tag.

## Weighting

For each WorkUnit and category:

```text
weighted category contribution = category probability × effort value
```

The displayed distribution normalizes the weighted contributions in the selected scope and period.

## Effort value

The canonical implementation determines the available effort signal according to the supported source precedence. Exact runtime precedence and fallback behavior must be generated or checked from the current code before this page becomes canonical.

## Missing and zero states

- A measured effort value of `0` is different from unavailable input.
- Missing scope, missing evidence, and incomplete processing must not be represented as a measured zero.
- A neutral or fallback distribution must be labeled with its evidence and confidence limitations.

## Related task

Use [Investigate where effort appears to be going](../../use/investment/investigate-effort.md) for the product workflow.
