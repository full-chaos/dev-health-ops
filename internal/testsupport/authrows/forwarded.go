// Package authrows is the row side of the auth-flow corpus on the ACP target. When go-api forwards an aggregate
// to auth-service, auth-service writes the auth schema, not the public rows the frozen golden holds. This
// package reads the auth-schema equivalents into the golden's row shape (Query) and compares them with the
// golden under a named, exact quirk table (Ledger): an unclassified difference fails, a quirk that covers
// nothing fails, and no comparison is skipped.
package authrows

import (
	"fmt"
	"os"
	"strings"
)

// EnvForwarded is the environment variable ci/auth_corpus_external.sh sets to the aggregates go-api forwarded to
// auth-service, comma separated and empty for none.
const EnvForwarded = "DEVHEALTH_AUTH_ORACLE_FORWARDED"

// Aggregate is one AUTH_PROXY_* aggregate of go-api.
type Aggregate string

const (
	Sessions    Aggregate = "sessions"
	LocalLogin  Aggregate = "local_login"
	Credentials Aggregate = "credentials"
	Social      Aggregate = "social"
	OIDC        Aggregate = "oidc"
	SAML        Aggregate = "saml"
)

// Aggregates is every aggregate, in the order of ci/auth_corpus_external.sh's AGGREGATES.
var Aggregates = []Aggregate{Sessions, LocalLogin, Credentials, Social, OIDC, SAML}

// Forwarded is the set of aggregates the target under test forwarded to auth-service. The zero value is none.
type Forwarded struct{ set map[Aggregate]bool }

// ParseForwarded reads a comma separated list of aggregate names. Empty text is none; an unknown or repeated
// name is an error.
func ParseForwarded(text string) (Forwarded, error) {
	out := Forwarded{set: map[Aggregate]bool{}}
	if strings.TrimSpace(text) == "" {
		return out, nil
	}
	for _, part := range strings.Split(text, ",") {
		name := Aggregate(strings.TrimSpace(part))
		known := false
		for _, a := range Aggregates {
			known = known || a == name
		}
		if !known {
			return Forwarded{}, fmt.Errorf("authrows: %q is not an aggregate (want a comma separated list of %v)", part, Aggregates)
		}
		if out.set[name] {
			return Forwarded{}, fmt.Errorf("authrows: aggregate %q is listed twice", name)
		}
		out.set[name] = true
	}
	return out, nil
}

// FromEnv reads EnvForwarded. An unset variable is an error: an external target's run script must say what it
// forwarded (empty for none), because the rows are read from the stores that forwarding decides.
func FromEnv() (Forwarded, error) {
	text, ok := os.LookupEnv(EnvForwarded)
	if !ok {
		return Forwarded{}, fmt.Errorf("authrows: %s is not set; the run script exports the forwarded aggregates (empty for none)", EnvForwarded)
	}
	return ParseForwarded(text)
}

// Has reports whether a is forwarded.
func (f Forwarded) Has(a Aggregate) bool { return f.set[a] }

// Any reports whether any aggregate is forwarded.
func (f Forwarded) Any() bool { return len(f.set) > 0 }

// String is the forwarded aggregates in Aggregates order, comma separated.
func (f Forwarded) String() string {
	var names []string
	for _, a := range Aggregates {
		if f.set[a] {
			names = append(names, string(a))
		}
	}
	return strings.Join(names, ",")
}
