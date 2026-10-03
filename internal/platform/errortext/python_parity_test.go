package errortext

// Sanitize is the Python-parity function alone (sanitize_error_text for a string: the patterns, then Python's default cap of 4000).
// TEST-ONLY (CHAOS-7947): it has no production caller, because on the sync writers' path it hides less than the function of the same
// name did on main (no ASCII reading); the entry points are SanitizeHardened and SanitizeHardenedShapesFirst. The clause corpus and
// the characterization tests pin the engine through it.
func Sanitize(text string) string { return Truncate(Redact(text), defaultMaxErrorTextLength) }
