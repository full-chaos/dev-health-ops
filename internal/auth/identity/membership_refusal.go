package identity

// MembershipRequiredMessage is the message of the 403 membership_required answer. go-api (internal/api/sso)
// and auth-service (identity/oidc, identity/saml) answer {"detail":{"message":<this>,"reason":"membership_required"}}.
const MembershipRequiredMessage = "A membership in this organization is required"
