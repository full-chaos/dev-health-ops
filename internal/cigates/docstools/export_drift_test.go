package docstools

// SetThemeOfForTest replaces the subcategory -> theme map for one test and returns the restore func.
func SetThemeOfForTest(f func(string) string) (restore func()) {
	old := invTxThemeOf
	invTxThemeOf = f
	return func() { invTxThemeOf = old }
}

const (
	CodePrereqUnicodeGoVersion = codePrereqUnicodeGoVersion
	CodePrereqUnicodeGoOnly    = codePrereqUnicodeGoOnly
)

var CodePrereqGoOnlyWord = codePrereqGoOnlyWord
