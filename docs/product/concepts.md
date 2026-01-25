# Concepts

## WorkUnits
A WorkUnit is an evidence container (PR, issue, incident, etc.). It is never a category.

## Evidence quality
Every categorization emits an evidence quality value and band. Low evidence quality is a data-quality signal.

## Probability distributions
Categorization is a distribution across canonical subcategories, rolled up deterministically to themes.

## Inspectability
Every computed output should map back to evidence:
- raw objects (PRs, issues, commits)
- derived relationships (work graph edges)
- materialized aggregates
