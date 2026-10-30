package audit

import "github.com/google/uuid"

// Caller is the credential an internal route was called with: a service
// credential row, or a workload principal and its verified token's jti. The
// zero Caller names nothing.
type Caller struct {
	CredentialID string
	Workload     *uuid.UUID
	JTI          string
}

// Annotate writes the caller into an audit row's attributes: credential_id,
// or caller_principal_id and workload_jti. The row's actor stays the human
// the call acts for.
func (c Caller) Annotate(attributes map[string]any) {
	switch {
	case c.Workload != nil:
		attributes["caller_principal_id"] = c.Workload.String()
		attributes["workload_jti"] = c.JTI
	case c.CredentialID != "":
		attributes["credential_id"] = c.CredentialID
	}
}
