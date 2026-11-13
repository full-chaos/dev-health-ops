package audit

// EventLegacyLinkTokensPurged records one run of the retired legacy public
// link-token purge (CHAOS-11506): the counts and the run id, nothing else.
// The verb is gone; the type stays so the rows it wrote stay valid.
const EventLegacyLinkTokensPurged = "auth.legacy_link_tokens.purged"

// publicPurgeEventTypes are the events whose state change was a delete in the
// ops (public) schema. Commit cannot write them: its changed-row count only
// sees guarded auth tables, so it refuses the mutation as a no-op.
var publicPurgeEventTypes = map[string]bool{EventLegacyLinkTokensPurged: true}
