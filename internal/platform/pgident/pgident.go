// Package pgident decides whether a name may be used as a bare PostgreSQL
// schema or role identifier by the migrators. It is a leaf: it imports nothing
// from this module, so the ops migrator and the auth migrator can share one
// definition without either entering the other's dependency tree.
package pgident

import "regexp"

// Problem says why a name is refused, or OK.
type Problem int

const (
	// OK means the name may be emitted as a bare identifier.
	OK Problem = iota
	// BadShape means the name is not [a-z][a-z0-9_]* of at most 63 characters.
	BadShape
	// ReservedKeyword means the name is a PostgreSQL reserved word.
	ReservedKeyword
)

// shape bounds every identifier the migrators interpolate.
//
// Interpolation is unavoidable: PostgreSQL has no bind placeholder for a schema
// or role name. Nothing reaches SQL text without matching this pattern first,
// and the pattern excludes quotes, whitespace, semicolons and backslashes by
// construction rather than by escaping them afterwards.
var shape = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// reserved are the PostgreSQL reserved words that cannot appear as a bare
// identifier. The list is deliberately short and covers the words a human
// might plausibly choose for a schema or role; it is a usability guard against
// a confusing runtime syntax error, not a security control -- the security
// comes from the shape excluding quotes and whitespace, and from the migrators
// double-quoting whatever survives.
var reserved = map[string]struct{}{
	"all": {}, "analyse": {}, "analyze": {}, "and": {}, "any": {}, "array": {},
	"as": {}, "asc": {}, "authorization": {}, "between": {}, "both": {},
	"case": {}, "cast": {}, "check": {}, "collate": {}, "column": {},
	"constraint": {}, "create": {}, "current_date": {}, "current_role": {},
	"current_time": {}, "current_timestamp": {}, "current_user": {},
	"default": {}, "deferrable": {}, "desc": {}, "distinct": {}, "do": {},
	"else": {}, "end": {}, "except": {}, "false": {}, "for": {}, "foreign": {},
	"from": {}, "grant": {}, "group": {}, "having": {}, "in": {}, "initially": {},
	"intersect": {}, "into": {}, "leading": {}, "limit": {}, "localtime": {},
	"localtimestamp": {}, "new": {}, "not": {}, "null": {}, "off": {},
	"offset": {}, "old": {}, "on": {}, "only": {}, "or": {}, "order": {},
	"placing": {}, "primary": {}, "references": {}, "returning": {},
	"select": {}, "session_user": {}, "some": {}, "symmetric": {}, "table": {},
	"then": {}, "to": {}, "trailing": {}, "true": {}, "union": {}, "unique": {},
	"user": {}, "using": {}, "when": {}, "where": {}, "with": {},
}

// Check classifies name.
func Check(name string) Problem {
	if !shape.MatchString(name) {
		return BadShape
	}
	if _, isReserved := reserved[name]; isReserved {
		return ReservedKeyword
	}
	return OK
}
