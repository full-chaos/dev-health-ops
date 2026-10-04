// Package syncdispatchv1 embeds this directory's sync-dispatch route policy
// into any Go binary that imports it.
//
// The River migrator (internal/rivermigrate) moves a sync-dispatch route row
// that still holds the retired Celery seed onto the route this policy names
// (CHAOS-8600). It also runs in an image that carries no copy of this
// directory, and go:embed cannot reach outside the directory holding the .go
// file that declares it, so this package exists here, alongside
// transport-routes.json, to make the policy the binary was built from the
// policy it applies (the same shape as contracts/jobs/v1).
package syncdispatchv1

import _ "embed"

// TransportRoutes is transport-routes.json's exact byte content, embedded at
// compile time.
//
//go:embed transport-routes.json
var TransportRoutes []byte
