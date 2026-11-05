package policy

// shadowClassTable is the reviewed mismatch class table (see LoadShadowClasses): a header line, then
// one tab-separated row per line. It is Go source so a change to it is a change to a Go file.
var shadowClassTable = []byte(`legacy	pdp	reason	namespace	class	note
allow	deny	delegation_invalid	*	intentional	The PDP refuses a delegated session whose delegation no longer holds: an actor that lost admin.impersonate or is deactivated, an empty actor chain (internal/auth/authapi/authorize.go delegated); the legacy guard does not read the actor.
allow	deny	repository_allowlist_too_large	*	intentional	The PDP denies a principal with more readable repositories than an allowlist holds (256) instead of truncating the list (internal/auth/authorization/scope.go repositoryScope); the legacy guard has no repository bound.
`)
