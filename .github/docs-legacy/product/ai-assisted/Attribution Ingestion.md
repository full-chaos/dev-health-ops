# Implement assisted-work attribution ingestion

## Goal

Detect AI-assisted and agent-created engineering work from provider data.

## P0 sources

- PR labels such as `ai-assisted`, `agent-created`, and `ai-review`
- Bot or app authors
- Commit trailers such as `AI-Assisted-By`
- Branch naming conventions
- PR descriptions
- CI annotations

## Requirements

- Explicit labels override weaker heuristics
- Manual attribution must be preserved
- Unknown work remains unknown rather than guessed
- Every inferred attribution includes confidence and source
- Attribution can run during sync or backfill

## Acceptance criteria

- Attribution runs during provider sync or metrics backfill
- AI-assisted PRs can be detected from labels and author metadata
- Agent-created PRs can be detected from bot/app metadata
- Commit trailer parsing works for AI attribution
- Test fixtures cover each P0 source type
