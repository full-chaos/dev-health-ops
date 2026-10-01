package emailvalidator

// The oracle tests live in package emailvalidator_test, because the
// frozen-answer harness imports this package. These are the unexported pieces
// they compare with Python's.
var (
	CheckDotAtom = checkDotAtom
	ContainsRune = containsRune
	EqualRunes   = equalRunes
)
