# Add attribution storage for AI-assisted work

## Goal

Add canonical storage for AI-assisted and agentic engineering work.

## Scope

Create or extend storage for:

- AI workflow run records
- AI PR attribution records
- AI review outcome records
- AI policy event records
- AI recommendation records

## Requirements

Attribution records must preserve:

- source of attribution
- confidence level
- related organization
- related team where available
- related repository
- related PR or issue where available
- timestamps
- provider metadata

## Acceptance criteria

- Database migrations are present
- Storage read and write paths are implemented
- Attribution source and confidence are preserved
- Existing metrics jobs can read attribution without breaking non-AI repositories
- Fixtures cover assisted work and agent-created work
