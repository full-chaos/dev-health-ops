package authschema

import "fmt"

// ValidatedIdentifier is a schema, role, table or sequence name that has passed
// ValidateIdentifier and may therefore be emitted into SQL as an identifier.
//
// WHY A TYPE RATHER THAN A COMMENT. PostgreSQL cannot bind an IDENTIFIER --
// `CREATE SCHEMA $1` is not valid SQL -- so this package genuinely does
// interpolate names into statements, and Semgrep's pgx-sqli rule flags every
// one of those sites. The justification for suppressing it used to be an
// ORDERING: validation happens before the DDL on the Apply path. That was true
// and unenforced. Nothing stopped a new call site from interpolating an
// unvalidated string, all the injection-shape tests kept passing, and the
// package already contained one caller reaching a DDL helper without
// validating (a test fixture with a compile-time literal schema).
//
// Making the identifier a TYPE converts the claim from one a reader has to
// check into one the compiler checks: quoteIdentifier accepts only this type,
// the only way to obtain one is NewValidatedIdentifier, and the only way to
// build that is to pass the allowlist.
//
// THE SCOPE OF THAT GUARANTEE, STATED PRECISELY, because a first version of
// this comment overstated it and a review round caught the overstatement:
// every site that renders an identifier THROUGH quoteIdentifier is covered,
// and the compiler enumerates those. A site that builds its own SQL with
// fmt.Sprintf and %q never calls quoteIdentifier, so changing this signature
// cannot reach it and the compiler says nothing. fixture_integration_test.go
// does exactly that for role and database names. Those inputs are generated
// test identifiers, so it is not a live injection path -- but "a new DDL path
// cannot skip validation" is FALSE as stated, and the true claim is narrower:
// a new path that uses this package's identifier RENDERER cannot skip it.
//
// Worth knowing if that fixture is ever fed a name it did not generate: Go's
// %q is Go string-literal quoting, not PostgreSQL identifier quoting. It
// escapes an embedded quote as \" where PostgreSQL expects it doubled, so %q
// is not a substitute for this function even where it looks like one.
//
// KNOWN LIMIT, stated because a guarantee whose boundary is unstated is a
// worse guarantee: inside THIS package a caller can still write
// ValidatedIdentifier{name: raw} and bypass the constructor. The struct
// literal is unavailable to every other package because the field is
// unexported, so the property is airtight across the package boundary and
// disciplined within it. That is strictly stronger than a comment, and it is
// not absolute. `go vet -vettool composites` and review are the remaining
// controls for in-package construction.
type ValidatedIdentifier struct{ name string }

// NewValidatedIdentifier validates name and returns it as an identifier that
// may be interpolated into SQL.
func NewValidatedIdentifier(name string) (ValidatedIdentifier, error) {
	if err := ValidateIdentifier(name); err != nil {
		return ValidatedIdentifier{}, err
	}
	return ValidatedIdentifier{name: name}, nil
}

// String returns the unquoted identifier.
func (v ValidatedIdentifier) String() string { return v.name }

// quoteIdentifier renders a validated identifier for interpolation.
//
// It takes ValidatedIdentifier rather than string, and that signature IS the
// security control: an unvalidated name cannot reach a SQL statement through
// this function because it cannot reach this function at all.
//
// The zero value is rejected loudly rather than rendered. ValidatedIdentifier{}
// carries an empty name, which is the one way to hold the type without having
// passed the constructor; emitting `""` would produce a syntax error at the
// server, far from the mistake. A panic here is correct because the zero value
// can only arrive from a programming error inside this package -- it cannot be
// produced by any input, valid or hostile.
func quoteIdentifier(v ValidatedIdentifier) string {
	if v.name == "" {
		panic(fmt.Sprintf("authschema: zero-value ValidatedIdentifier reached quoteIdentifier; " +
			"identifiers must come from NewValidatedIdentifier"))
	}
	return `"` + v.name + `"`
}

// Quote renders a validated identifier for interpolation by ANOTHER package.
//
// It exists so a package that must name this schema in its own SQL --
// internal/audit writes the outbox and audit tables inside a caller's
// transaction -- uses THIS renderer instead of writing a second one. A second
// renderer is the defect CHAOS-4918 is about: fmt.Sprintf with %q looks like
// identifier quoting, is Go string-literal quoting, and escapes an embedded
// quote as \" where PostgreSQL wants it doubled.
//
// Exported deliberately and narrowly: the TYPE is still the control, so a
// caller cannot reach this without having passed the allowlist first.
func Quote(v ValidatedIdentifier) string { return quoteIdentifier(v) }

// mustValidatedIdentifier is for identifiers this package itself defines as
// compile-time constants, where a failure is a build-time authoring error
// rather than anything an operator or an attacker can influence. It is
// deliberately NOT exported and must never be reachable from an input.
func mustValidatedIdentifier(name string) ValidatedIdentifier {
	v, err := NewValidatedIdentifier(name)
	if err != nil {
		panic(fmt.Sprintf("authschema: constant identifier %q is invalid: %v", name, err))
	}
	return v
}

// versionTableIdentifier is this lineage's bookkeeping table, defined by this
// package rather than supplied by anyone.
var versionTableIdentifier = mustValidatedIdentifier(versionTable)
