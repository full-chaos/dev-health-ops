# Web GraphQL client

Source: `dev-health-web/docs/graphql-client.md`

## Library stack
- urql for operations + normalized caching
- React hooks for fetching
- Subscriptions via WebSocket
- Zod validation for runtime checking

## Environment switch
`NEXT_PUBLIC_USE_GRAPHQL_ANALYTICS=false` disables GraphQL analytics.

## Key implementation files
- `src/lib/graphql/provider.tsx`
- `src/lib/graphql/hooks.ts`
- `src/lib/graphql/client.ts`
