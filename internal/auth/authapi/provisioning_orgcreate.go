package authapi

const (
	// PatternOrgOnboard is the route of a user's first organization.
	PatternOrgOnboard = "/internal/v1/organizations/onboard"
	// PatternOrgCreate is the route of an organization a superuser creates.
	PatternOrgCreate = "/internal/v1/organizations/create"
)

const (
	opOrgOnboard      = "organization_onboard"
	opOrgCreate       = "organization_create"
	reasonNameInvalid = "name_invalid"
)
