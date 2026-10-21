package saml

import "github.com/full-chaos/dev-health-ops/internal/auth/requestid"

// auditRequestID is the server-minted request id for audit rows (nil without
// the requestid middleware). The client's X-Request-ID is never used.
var auditRequestID = requestid.Audit
