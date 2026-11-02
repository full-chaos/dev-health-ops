package session

// ValidAMR reports whether amr is a non-empty set of session.v1's amr values.
func ValidAMR(amr []string) bool { return validAMR(amr) }

// AssuranceOf is the principal.v1 assurance level of a session's amr: aal1 for
// a non-empty set of enum values, "" otherwise. No higher level is derived
// from amr alone; the writer of an MFA factor owns the higher rows.
func AssuranceOf(amr []string) string {
	if !validAMR(amr) {
		return ""
	}
	return "aal1"
}
