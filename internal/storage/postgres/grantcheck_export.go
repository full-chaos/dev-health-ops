package postgres

// This file exists solely to give the CHAOS-3033 grant-surface-deriver
// checker (internal/domaingrants) a read-only, in-process way to obtain the
// EXACT text of the required_table_privileges CTE embedded in
// domainAuthorizationQuery (domain_authorization.go), without duplicating or
// re-parsing that logic from source text. It does not modify
// domain_authorization.go and does not change production behavior.

// DomainAuthorizationQueryForCheck returns the verbatim SQL text of
// domainAuthorizationQuery, so the checker can parse the
// required_table_privileges VALUES rows out of the real, currently-shipping
// query rather than a restatement of it.
func DomainAuthorizationQueryForCheck() string {
	return domainAuthorizationQuery
}
