// Code generated from gitleaks v8.24.3 config/gitleaks.toml (sha256 8f393899dbdc67e4eae580909e496acab9cf3a75aacfb6fac56b6eb16bb9e051); the expressions are copied byte for byte. DO NOT EDIT by hand:
// a new scanner version is a new generation (see CHAOS-8180 and testdata/scanner_vectors.tsv).

package goldenscan

import "regexp"

// scannerVersion is the gitleaks release this port follows.
const scannerVersion = "8.24.3"

// generic is the generic-api-key expression.
var generic = regexp.MustCompile(`(?i)[\w.-]{0,50}?(?:access|auth|(?-i:[Aa]pi|API)|credential|creds|key|passw(?:or)?d|secret|token)(?:[ \t\w.-]{0,20})[\s'"]{0,3}(?:=|>|:{1,3}=|\|\||:|=>|\?=|,)[\x60'"\s=]{0,5}([\w.=-]{10,150}|[a-z0-9][a-z0-9+/]{11,}={0,3})(?:[\x60'"\s;]|\\[nr]|$)`)

// globalAllowRegexes are the regexes of the global allowlist (applied to the secret).
var globalAllowRegexes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^true|false|null$`),
	regexp.MustCompile(`^(?i:a+|b+|c+|d+|e+|f+|g+|h+|i+|j+|k+|l+|m+|n+|o+|p+|q+|r+|s+|t+|u+|v+|w+|x+|y+|z+|\*+|\.+)$`),
	regexp.MustCompile(`^\$(?:\d+|{\d+})$`),
	regexp.MustCompile(`^\$(?:[A-Z_]+|[a-z_]+)$`),
	regexp.MustCompile(`^\${(?:[A-Z_]+|[a-z_]+)}$`),
	regexp.MustCompile(`^\{\{[ \t]*[\w ().|]+[ \t]*}}$`),
	regexp.MustCompile(`^\$\{\{[ \t]*(?:(?:env|github|secrets|vars)(?:\.[A-Za-z]\w+)+[\w "'&./=|]*)[ \t]*}}$`),
	regexp.MustCompile(`^%(?:[A-Z_]+|[a-z_]+)%$`),
	regexp.MustCompile(`^%[+\-# 0]?[bcdeEfFgGoOpqstTUvxX]$`),
	regexp.MustCompile(`^\{\d{0,2}}$`),
	regexp.MustCompile(`^@(?:[A-Z_]+|[a-z_]+)@$`),
	regexp.MustCompile(`^/Users/(?i)[a-z0-9]+/[\w .-/]+$`),
	regexp.MustCompile(`^/(?:bin|etc|home|opt|tmp|usr|var)/[\w ./-]+$`),
}

// globalStopwords are the stopwords of the global allowlist.
var globalStopwords = []string{"abcdefghijklmnopqrstuvwxyz", "014df517-39d1-4453-b7b3-9930c563627c"}

// ruleAllowSecret is the first rule allowlist: a regex on the secret (letters, underscores, dots and dashes only).
var ruleAllowSecret = []*regexp.Regexp{
	regexp.MustCompile(`^[a-zA-Z_.-]+$`),
}

// ruleAllowMatch is the second rule allowlist: a regex on the whole match, or a stopword in the secret.
var ruleAllowMatch = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(?:access(?:ibility|or)|access[_.-]?id|random[_.-]?access|api[_.-]?(?:id|name|version)|rapid|capital|[a-z0-9-]*?api[a-z0-9-]*?:jar:|author|X-MS-Exchange-Organization-Auth|Authentication-Results|(?:credentials?[_.-]?id|withCredentials)|(?:bucket|foreign|hot|idx|natural|primary|pub(?:lic)?|schema|sequence)[_.-]?key|(?:turkey)|key[_.-]?(?:alias|board|code|frame|id|length|mesh|name|pair|press(?:ed)?|ring|selector|signature|size|stone|storetype|word|up|down|left|right)|key[_.-]?vault[_.-]?(?:id|name)|keyVaultToStoreSecrets|key(?:store|tab)[_.-]?(?:file|path)|issuerkeyhash|(?-i:[DdMm]onkey|[DM]ONKEY)|keying|(?:secret)[_.-]?(?:length|name|size)|UserSecretsId|(?:csrf)[_.-]?token|(?:io\.jsonwebtoken[ \t]?:[ \t]?[\w-]+)|(?:api|credentials|token)[_.-]?(?:endpoint|ur[il])|public[_.-]?token|(?:key|token)[_.-]?file|(?-i:(?:[A-Z_]+=\n[A-Z_]+=|[a-z_]+=\n[a-z_]+=)(?:\n|\z))|(?-i:(?:[A-Z.]+=\n[A-Z.]+=|[a-z.]+=\n[a-z.]+=)(?:\n|\z)))`),
}

// ruleAllowLine is the third rule allowlist: a regex on the line of the match.
var ruleAllowLine = []*regexp.Regexp{
	regexp.MustCompile(`--mount=type=secret,`),
	regexp.MustCompile(`import[ \t]+{[ \t\w,]+}[ \t]+from[ \t]+['"][^'"]+['"]`),
}
