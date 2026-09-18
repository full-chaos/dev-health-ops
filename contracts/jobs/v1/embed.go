// Package jobsv1 embeds this directory's job migration policy into any Go
// binary that imports it.
//
// dev-health-worker-migrate seeds a route row for every kind the policy runs
// on River with no rollback route. It runs in an image that carries no copy
// of this directory, and go:embed cannot reach outside the directory holding
// the .go file that declares it, so this package exists here, alongside
// migration-state.json, to make the policy the binary was built from the
// policy it applies.
package jobsv1

import _ "embed"

// MigrationState is migration-state.json's exact byte content, embedded at
// compile time.
//
//go:embed migration-state.json
var MigrationState []byte
