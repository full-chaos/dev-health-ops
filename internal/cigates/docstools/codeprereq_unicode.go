package docstools

// Code points that are word characters (letter, number or underscore) in Go's Unicode tables but not in Python's \w.
// The Python \b of the credential pattern treats them as non-word. Generated once, by hand, outside CI (see
// testdata/README.md, "Unicode word table"); no test runs Python. A Go release with another unicode.Version fails
// TestCodePrerequisitesUnicodeTableMatchesGo until the table is generated again.
const (
	codePrereqUnicodeGoVersion = "17.0.0"
	codePrereqUnicodePyVersion = "16.0.0"
	codePrereqUnicodeGoOnly    = 4657
)

// codePrereqGoOnlyWord lists inclusive code point ranges; a Python-only word character does not exist at these versions.
var codePrereqGoOnlyWord = [][2]rune{
	{0x88F, 0x88F},
	{0xC5C, 0xC5C},
	{0xCDC, 0xCDC},
	{0xA7CE, 0xA7CF},
	{0xA7D2, 0xA7D2},
	{0xA7D4, 0xA7D4},
	{0xA7F1, 0xA7F1},
	{0x10940, 0x10959},
	{0x10EC5, 0x10EC7},
	{0x11DB0, 0x11DDB},
	{0x11DE0, 0x11DE9},
	{0x16EA0, 0x16EB8},
	{0x16EBB, 0x16ED3},
	{0x16FF2, 0x16FF6},
	{0x187F8, 0x187FF},
	{0x18D09, 0x18D1E},
	{0x18D80, 0x18DF2},
	{0x1E6C0, 0x1E6DE},
	{0x1E6E0, 0x1E6E2},
	{0x1E6E4, 0x1E6E5},
	{0x1E6E7, 0x1E6ED},
	{0x1E6F0, 0x1E6F4},
	{0x1E6FE, 0x1E6FF},
	{0x2B73A, 0x2B73F},
	{0x2CEA2, 0x2CEAD},
	{0x323B0, 0x33479},
}
