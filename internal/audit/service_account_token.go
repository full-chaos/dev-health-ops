package audit

// EventServiceAccountTokenIssued is a service account token the exchange
// issued for a service account key. The token is stateless, so no security
// state changes.
const EventServiceAccountTokenIssued = "auth.service_account.token_issued"

func init() { appendOnlyEventTypes[EventServiceAccountTokenIssued] = true }
