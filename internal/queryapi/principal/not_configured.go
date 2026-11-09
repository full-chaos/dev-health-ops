package principal

import "errors"

// errNotConfigured is a nil *Verifier's answer: the process has no envelope key.
var errNotConfigured = errors.New("principal: no envelope key configured")
