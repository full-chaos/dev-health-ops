package integrationshards_test

// The python guards these tests port used `re` on str, where \s and \b are Unicode-aware; Go's are ASCII.
const (
	pySpace   = `\s\x{1C}-\x{1F}\x{85}\x{A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}`
	pyS       = `[` + pySpace + `]`
	pyWordSet = `\p{L}\p{N}_`
	pyBefore  = `(?:^|[^` + pyWordSet + `])`
	pyAfter   = `(?:[^` + pyWordSet + `]|$)`
)
