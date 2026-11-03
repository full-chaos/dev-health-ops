package audit

// SourceOperatorCLI is the audit source of a change an operator made with an
// auth-migrate verb, as the migration role: the row has no actor principal.
const SourceOperatorCLI = "operator-cli"

// EventPlatformOwnerGrantRefused and EventPlatformOwnerRevokeRefused are an API
// grant or revoke of platform_owner that the platform role store refused:
// nothing changed. The role is granted and revoked only by auth-migrate.
const (
	EventPlatformOwnerGrantRefused  = "auth.platform_owner.grant_refused"
	EventPlatformOwnerRevokeRefused = "auth.platform_owner.revoke_refused"
)

func init() {
	appendOnlyEventTypes[EventPlatformOwnerGrantRefused] = true
	appendOnlyEventTypes[EventPlatformOwnerRevokeRefused] = true
}
