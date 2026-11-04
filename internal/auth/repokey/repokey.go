// Package repokey is the one definition of the canonical repository key of the
// Auth Control Plane: "*" for every repository of the organization, or a
// lowercase path of 2-20 segments in at most 255 bytes (a GitLab path nests 20
// groups deep). It is a dependency-free leaf, so the grant store, the decision
// point, the request decoder and the published schemas hold one rule.
package repokey

import "regexp"

// MaxSegments and MaxBytes bound a whole repository key.
const (
	MaxSegments = 20
	MaxBytes    = 255
)

// SchemaPattern is the pattern every published schema holds for a repository
// key (with maxLength MaxBytes), as an RE2 and ECMA-262 pattern. Every segment
// but the last is a group name: 1-100 characters, an alphanumeric first and
// last character, no leading '.'. The last is a repository name, which may
// start with '.' (.github), so '.' and '..' stay out. The group repeat is
// unrolled because RE2 refuses a bounded repeat nested in a repeat past 1000
// copies.
var SchemaPattern = schemaPattern()

var pattern = regexp.MustCompile(SchemaPattern)

func schemaPattern() string {
	const group = `[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?`
	const last = `(?:[a-z0-9]|[a-z0-9.][a-z0-9._-]{0,98}[a-z0-9])`
	nest := ""
	for i := 0; i < MaxSegments-2; i++ {
		nest = "(?:/" + group + nest + ")?"
	}
	return `^(?:` + group + nest + `/` + last + `|\*)$`
}

// Valid reports whether key is a canonical repository key: "*" or a lowercase
// path of 2-20 segments in at most 255 bytes.
func Valid(key string) bool { return len(key) <= MaxBytes && pattern.MatchString(key) }

// Matcher is the compiled SchemaPattern, for a decoder that reports a refusal
// by its own name. The caller holds the MaxBytes bound.
func Matcher() *regexp.Regexp { return pattern }
