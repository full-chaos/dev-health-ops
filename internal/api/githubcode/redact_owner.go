package githubcode

import (
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// RedactOwner returns err with the owner's login replaced by {login} in its text. A failure of an owner route names
// the request URL (restcore puts it in a not-found text) and the Python-parity text of the organization route names
// the owner in its operation; a client may read that text, and a login is personal data (the class ops #4161
// closed for the team routes). A non-Error is returned as it is.
func RedactOwner(err error, owner string) error {
	typed, ok := err.(*Error)
	if !ok || owner == "" {
		return err
	}
	message := typed.Message
	for _, form := range []string{owner, pythonparity.Quote(owner, ""), url.PathEscape(owner)} {
		message = strings.ReplaceAll(message, form, "{login}")
	}
	return &Error{Class: typed.Class, Message: message}
}
