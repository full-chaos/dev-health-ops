package policy

// shadowClassTable is the reviewed mismatch class table (see LoadShadowClasses): a header line, then
// one tab-separated row per line. It is Go source so a change to it is a change to a Go file.
var shadowClassTable = []byte(`legacy	pdp	reason	namespace	class	note
allow	deny	delegation_invalid	*	intentional	The PDP refuses every delegated session until its delegation rules are wired (internal/auth/authapi/authorize.go delegated); the legacy guard serves an impersonating caller.
`)
