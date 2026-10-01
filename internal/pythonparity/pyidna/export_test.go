package pyidna

// The oracle tests live in package pyidna_test, because the frozen-answer
// harness imports this package. These are the unexported pieces they compare
// with Python's.
var (
	AsciiToRunes  = asciiToRunes
	CheckBidi     = checkBidi
	JoiningType   = joiningType
	ValidContextJ = validContextJ
	ValidContextO = validContextO
)
