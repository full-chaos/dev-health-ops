# Persisted queries

File: `api/graphql/persisted_queries.json`

## Purpose
Persisted queries allow the UI to reference stable query identifiers instead of sending ad-hoc query text.

## Operational benefits
- Enables server-side caching keyed by query id + variables
- Simplifies authorization gates and cost enforcement
- Reduces attack surface (fewer dynamic query shapes)

## Invalidation
Invalidation hooks live in:
- `api/graphql/cache_invalidation.py`
- `api/graphql/pubsub.py`
