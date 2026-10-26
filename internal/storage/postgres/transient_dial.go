package postgres

import "github.com/full-chaos/dev-health-ops/internal/platform/dialclass"

// ErrDialUnreachable is the bounded cause a sanitizer keeps when it drops a
// transient dial error's text (see dialclass.ErrUnreachable).
var ErrDialUnreachable = dialclass.ErrUnreachable

// IsTransientDialError classifies err by type and code only, never by text.
// Auth (28P01, 28000), permission (42501), other server answers, TLS and
// configuration failures are final.
func IsTransientDialError(err error) bool { return dialclass.IsTransient(err) }
