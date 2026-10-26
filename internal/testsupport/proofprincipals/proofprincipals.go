// Package proofprincipals names the proof service principals of the users table for the corpus comparisons that leave
// them out: by identity, never by the shape of the row.
package proofprincipals

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

// IDs are the fixed users.id of the proof service principals (edgetokenmint), the ids CHAOS-11523's placeholder
// predicate names.
var IDs = []string{edgetokenmint.ProvePrincipalID, edgetokenmint.AdminProofPrincipalID}

// ComparedSQL is a predicate over the public.users row aliased alias: true for every row except the proof principals.
// A row of provider service with the proof principals' shape and another id is compared, never dropped.
func ComparedSQL(alias string) string {
	return fmt.Sprintf("%s.auth_provider IS DISTINCT FROM 'service'", alias)
}
