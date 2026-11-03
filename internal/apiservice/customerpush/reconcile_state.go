package customerpush

import (
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
)

// ExpectedState is the state the projection must hold for a stored token. Stub.
func ExpectedState(t tokenRow) pushreconcile.Token { return pushreconcile.Token{} }
