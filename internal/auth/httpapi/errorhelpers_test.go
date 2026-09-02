package httpapi

import "errors"

// errorAs and errorIs keep the test file's assertions readable without
// importing errors at every call site.
func errorAs(err error, target any) bool { return errors.As(err, target) }

func errorIs(err, target error) bool { return errors.Is(err, target) }
