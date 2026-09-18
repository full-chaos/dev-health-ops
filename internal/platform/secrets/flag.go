package secrets

import (
	"flag"
	"fmt"
	"os"
)

// BindFlag registers a string flag whose value may fall back to an
// environment variable, without ever letting that variable's VALUE reach
// flag's usage text.
//
// `flag` prints every registered flag's DEFAULT VALUE in its usage output
// (a mistyped flag, a missing value, -h). A flag registered as
// set.StringVar(p, name, os.Getenv(envVar), ...) therefore makes the
// env var's value -- a DSN or URI may carry a password in its userinfo --
// the flag's default, and `flag` prints it in full on any of those paths.
// BindFlag instead registers an EMPTY default and names the environment
// variable in the usage text; ResolveFlag applies the fallback AFTER
// Parse, entirely outside flag's own printing path.
func BindFlag(set *flag.FlagSet, p *string, name, envVar, usage string) {
	set.StringVar(p, name, "", usage+" (falls back to the "+envVar+" environment variable, whose VALUE is never printed)")
}

// ResolveFlag applies the environment fallback after set.Parse: a flag the
// operator EXPLICITLY set always wins, even if they set it to the empty
// string -- set.Visit reports only flags actually passed on the command
// line, which is the one way to tell "-name=" (empty ON PURPOSE) apart
// from "nothing passed" once BindFlag's own default is unconditionally
// "". Only when nothing was passed does envVar apply, taken VERBATIM
// (no trimming): the original code this replaces used envVar as the
// flag's raw default with no transformation, and preserving that exactly
// is the point -- a DSN whose only valid form happens to need whatever
// bytes the operator set is never silently rewritten. Call once per
// bound flag, after Parse, before the value is used.
func ResolveFlag(set *flag.FlagSet, p *string, name, envVar string) {
	explicit := false
	set.Visit(func(f *flag.Flag) {
		if f.Name == name {
			explicit = true
		}
	})
	if explicit {
		return
	}
	*p = os.Getenv(envVar)
}

// RedactedConnectError reports a failed attempt to open a Postgres
// connection or pool for a DSN sourced from a BindFlag-registered flag,
// WITHOUT echoing the underlying driver error.
//
// A malformed DSN's parse failure embeds the DSN itself in the driver's
// own error message, and the driver's redaction covers only the
// userinfo component (`user:password@`) -- a credential placed anywhere
// else in the DSN, such as a `options=-c password=<value>` query
// parameter, is not redacted by the driver at all. Nothing about the
// driver's error is safe to print, so this reports a fixed message
// naming the operation and the two places the DSN could have come from
// instead of wrapping err.
func RedactedConnectError(operation, flagName, envVar string) error {
	return fmt.Errorf("%s: failed to open a connection -- check the DSN passed via -%s (or %s) and network reachability", operation, flagName, envVar)
}
