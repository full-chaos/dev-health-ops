package session

// ValidAMR reports whether amr is a non-empty set of session.v1's amr values.
func ValidAMR(amr []string) bool { return validAMR(amr) }
