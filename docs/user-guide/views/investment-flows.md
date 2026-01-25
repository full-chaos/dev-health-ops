# Investment Flows (Sankey)

## What it answers
- How does effort concentrate or fragment across themes and scopes?
- Where is the shift between delivery and reactive work?

## Requirements
- Sankey must support time slicing or baseline deltas, otherwise it becomes decorative.
- Scope node (repo/team) is optional and depends on mapping availability.

## Backing surfaces
- GraphQL `analytics.sankey`
- SQL compiler `compile_sankey`
