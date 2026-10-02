// Package credentialshapes is the ONE list of credential shapes and sentence contexts every text redactor of the system is
// proven on (CHAOS-7937): logging.RedactText (the log handler), logging.RedactCredentialShapes, pythonparity's
// SanitizeErrorTextHardened and the sync sanitizer. Every value is a plain literal written out in full: a documented prefix
// followed by a low-entropy body (one repeated character per class of the alphabet), so it matches OUR patterns and gives a
// secret scanner nothing to flag. Generated once from the shape specification and then maintained by hand.
package credentialshapes

import "strings"

// Shape is one credential shape. Values: one value per character CLASS of the shape's body alphabet (lower, upper, digit and
// each allowed symbol), each at the minimum length or above. Short is the same shape one byte under the minimum: NOT redacted.
type Shape struct {
	ID     string
	Opaque bool
	Values []string
	Short  string
}

// Value is the first value of the shape.
func (s Shape) Value() string { return s.Values[0] }

// ShapesWithoutPositiveFixture are the alternations of the vendor pattern that have NO positive fixture here: GitHub push protection
// rejects any literal of the Stripe and LaunchDarkly key shapes, whatever its entropy, so a fixture cannot be committed (CHAOS-7937, D4320).
// Each is named as NOT covered by a positive fixture; the production pattern still matches them and the SHORT and negative
// sentences below still pin their lower bound.
func ShapesWithoutPositiveFixture() []string {
	return []string{`(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}`, `whsec_[A-Za-z0-9]{16,}`, `(?:api|sdk|mob)-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`}
}

// Shapes returns the list.
func Shapes() []Shape {
	return []Shape{
		{ID: "openai sk-", Values: []string{"sk-aaaaaaaaaaaaaaaaaaaaaaaa", "sk-AAAAAAAAAAAAAAAAAAAAAAAA", "sk-111111111111111111111111", "sk-aaaaaaaaaaaaaaaaaaaa"}, Short: "sk-aaaaaaaaaaaaaaaaaaa"},
		{ID: "openai sk-proj-", Values: []string{"sk-proj-aaaaaaaaaaaaaaaaaaaaaaaa", "sk-proj-AAAAAAAAAAAAAAAAAAAAAAAA", "sk-proj-111111111111111111111111", "sk-proj-________________________", "sk-proj-------------------------", "sk-proj-aaaaaaaaaaaaaaaaaaaa"}, Short: "sk-proj-aaaaaaaaaaaaaaaaaaa"},
		{ID: "anthropic sk-ant-", Values: []string{"sk-ant-api03-aaaaaaaaaaaaaaaaaaaaaaaa", "sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAA", "sk-ant-api03-111111111111111111111111", "sk-ant-api03-________________________", "sk-ant-api03-------------------------", "sk-ant-aaaaaaaaaaaaaaaaaaaa"}, Short: "sk-ant-aaaaaaaaaaaaaaaaaaa"},
		{ID: "openrouter sk-or-", Values: []string{"sk-or-v1-aaaaaaaaaaaaaaaaaaaaaaaa", "sk-or-v1-AAAAAAAAAAAAAAAAAAAAAAAA", "sk-or-v1-111111111111111111111111", "sk-or-v1-________________________", "sk-or-v1-------------------------", "sk-or-aaaaaaaaaaaaaaaaaaaa"}, Short: "sk-or-aaaaaaaaaaaaaaaaaaa"},
		{ID: "sk-svcacct", Values: []string{"sk-svcacct-aaaaaaaaaaaaaaaaaaaaaaaa", "sk-svcacct-AAAAAAAAAAAAAAAAAAAAAAAA", "sk-svcacct-111111111111111111111111", "sk-svcacct-________________________", "sk-svcacct-------------------------", "sk-svcacct-aaaaaaaaaaaaaaaaaaaa"}, Short: "sk-svcacct-aaaaaaaaaaaaaaaaaaa"},
		{ID: "sk-admin", Values: []string{"sk-admin-aaaaaaaaaaaaaaaaaaaaaaaa", "sk-admin-AAAAAAAAAAAAAAAAAAAAAAAA", "sk-admin-111111111111111111111111", "sk-admin-________________________", "sk-admin-------------------------", "sk-admin-aaaaaaaaaaaaaaaaaaaa"}, Short: "sk-admin-aaaaaaaaaaaaaaaaaaa"},
		{ID: "google AIza", Values: []string{"AIzaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "AIzaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "AIza111111111111111111111111111111", "AIza______________________________", "AIza------------------------------", "AIzaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, Short: "AIzaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{ID: "slack xoxa", Values: []string{"xoxa-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxa-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxa-111111111111111111111111", "xoxa-------------------------", "xoxa-aaaaaaaaaa"}, Short: "xoxa-aaaaaaaaa"},
		{ID: "slack xoxb", Values: []string{"xoxb-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxb-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxb-111111111111111111111111", "xoxb-------------------------", "xoxb-aaaaaaaaaa"}, Short: "xoxb-aaaaaaaaa"},
		{ID: "slack xoxc", Values: []string{"xoxc-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxc-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxc-111111111111111111111111", "xoxc-------------------------", "xoxc-aaaaaaaaaa"}, Short: "xoxc-aaaaaaaaa"},
		{ID: "slack xoxd", Values: []string{"xoxd-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxd-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxd-111111111111111111111111", "xoxd-------------------------", "xoxd-aaaaaaaaaa"}, Short: "xoxd-aaaaaaaaa"},
		{ID: "slack xoxe", Values: []string{"xoxe-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxe-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxe-111111111111111111111111", "xoxe-------------------------", "xoxe-aaaaaaaaaa"}, Short: "xoxe-aaaaaaaaa"},
		{ID: "slack xoxp", Values: []string{"xoxp-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxp-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxp-111111111111111111111111", "xoxp-------------------------", "xoxp-aaaaaaaaaa"}, Short: "xoxp-aaaaaaaaa"},
		{ID: "slack xoxr", Values: []string{"xoxr-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxr-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxr-111111111111111111111111", "xoxr-------------------------", "xoxr-aaaaaaaaaa"}, Short: "xoxr-aaaaaaaaa"},
		{ID: "slack xoxs", Values: []string{"xoxs-aaaaaaaaaaaaaaaaaaaaaaaa", "xoxs-AAAAAAAAAAAAAAAAAAAAAAAA", "xoxs-111111111111111111111111", "xoxs-------------------------", "xoxs-aaaaaaaaaa"}, Short: "xoxs-aaaaaaaaa"},
		{ID: "github ghp", Values: []string{"ghp_aaaaaaaaaaaaaaaaaaaaaaaa", "ghp_AAAAAAAAAAAAAAAAAAAAAAAA", "ghp_111111111111111111111111", "ghp_aaaaaaaaaaaaaaaa"}, Short: "ghp_aaaaaaaaaaaaaaa"},
		{ID: "github gho", Values: []string{"gho_aaaaaaaaaaaaaaaaaaaaaaaa", "gho_AAAAAAAAAAAAAAAAAAAAAAAA", "gho_111111111111111111111111", "gho_aaaaaaaaaaaaaaaa"}, Short: "gho_aaaaaaaaaaaaaaa"},
		{ID: "github ghu", Values: []string{"ghu_aaaaaaaaaaaaaaaaaaaaaaaa", "ghu_AAAAAAAAAAAAAAAAAAAAAAAA", "ghu_111111111111111111111111", "ghu_aaaaaaaaaaaaaaaa"}, Short: "ghu_aaaaaaaaaaaaaaa"},
		{ID: "github ghs", Values: []string{"ghs_aaaaaaaaaaaaaaaaaaaaaaaa", "ghs_AAAAAAAAAAAAAAAAAAAAAAAA", "ghs_111111111111111111111111", "ghs_aaaaaaaaaaaaaaaa"}, Short: "ghs_aaaaaaaaaaaaaaa"},
		{ID: "github ghr", Values: []string{"ghr_aaaaaaaaaaaaaaaaaaaaaaaa", "ghr_AAAAAAAAAAAAAAAAAAAAAAAA", "ghr_111111111111111111111111", "ghr_aaaaaaaaaaaaaaaa"}, Short: "ghr_aaaaaaaaaaaaaaa"},
		{ID: "github_pat_", Values: []string{"github_pat_aaaaaaaaaaaaaaaaaaaaaaaa", "github_pat_AAAAAAAAAAAAAAAAAAAAAAAA", "github_pat_111111111111111111111111", "github_pat_________________________", "github_pat_aaaaaaaaaaaaaaaa"}, Short: "github_pat_aaaaaaaaaaaaaaa"},
		{ID: "gitlab glpat", Values: []string{"glpat-aaaaaaaaaaaaaaaaaaaaaaaa", "glpat-AAAAAAAAAAAAAAAAAAAAAAAA", "glpat-111111111111111111111111", "glpat-________________________", "glpat-------------------------", "glpat-aaaaaaaaaaaaaaaa"}, Short: "glpat-aaaaaaaaaaaaaaa"},
		{ID: "gitlab gloas", Values: []string{"gloas-aaaaaaaaaaaaaaaaaaaaaaaa", "gloas-AAAAAAAAAAAAAAAAAAAAAAAA", "gloas-111111111111111111111111", "gloas-________________________", "gloas-------------------------", "gloas-aaaaaaaaaaaaaaaa"}, Short: "gloas-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glrt", Values: []string{"glrt-aaaaaaaaaaaaaaaaaaaaaaaa", "glrt-AAAAAAAAAAAAAAAAAAAAAAAA", "glrt-111111111111111111111111", "glrt-________________________", "glrt-------------------------", "glrt-aaaaaaaaaaaaaaaa"}, Short: "glrt-aaaaaaaaaaaaaaa"},
		{ID: "gitlab gldt", Values: []string{"gldt-aaaaaaaaaaaaaaaaaaaaaaaa", "gldt-AAAAAAAAAAAAAAAAAAAAAAAA", "gldt-111111111111111111111111", "gldt-________________________", "gldt-------------------------", "gldt-aaaaaaaaaaaaaaaa"}, Short: "gldt-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glptt", Values: []string{"glptt-aaaaaaaaaaaaaaaaaaaaaaaa", "glptt-AAAAAAAAAAAAAAAAAAAAAAAA", "glptt-111111111111111111111111", "glptt-________________________", "glptt-------------------------", "glptt-aaaaaaaaaaaaaaaa"}, Short: "glptt-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glsoat", Values: []string{"glsoat-aaaaaaaaaaaaaaaaaaaaaaaa", "glsoat-AAAAAAAAAAAAAAAAAAAAAAAA", "glsoat-111111111111111111111111", "glsoat-________________________", "glsoat-------------------------", "glsoat-aaaaaaaaaaaaaaaa"}, Short: "glsoat-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glft", Values: []string{"glft-aaaaaaaaaaaaaaaaaaaaaaaa", "glft-AAAAAAAAAAAAAAAAAAAAAAAA", "glft-111111111111111111111111", "glft-________________________", "glft-------------------------", "glft-aaaaaaaaaaaaaaaa"}, Short: "glft-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glimt", Values: []string{"glimt-aaaaaaaaaaaaaaaaaaaaaaaa", "glimt-AAAAAAAAAAAAAAAAAAAAAAAA", "glimt-111111111111111111111111", "glimt-________________________", "glimt-------------------------", "glimt-aaaaaaaaaaaaaaaa"}, Short: "glimt-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glagent", Values: []string{"glagent-aaaaaaaaaaaaaaaaaaaaaaaa", "glagent-AAAAAAAAAAAAAAAAAAAAAAAA", "glagent-111111111111111111111111", "glagent-________________________", "glagent-------------------------", "glagent-aaaaaaaaaaaaaaaa"}, Short: "glagent-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glcbt", Values: []string{"glcbt-aaaaaaaaaaaaaaaaaaaaaaaa", "glcbt-AAAAAAAAAAAAAAAAAAAAAAAA", "glcbt-111111111111111111111111", "glcbt-________________________", "glcbt-------------------------", "glcbt-aaaaaaaaaaaaaaaa"}, Short: "glcbt-aaaaaaaaaaaaaaa"},
		{ID: "gitlab glffct", Values: []string{"glffct-aaaaaaaaaaaaaaaaaaaaaaaa", "glffct-AAAAAAAAAAAAAAAAAAAAAAAA", "glffct-111111111111111111111111", "glffct-________________________", "glffct-------------------------", "glffct-aaaaaaaaaaaaaaaa"}, Short: "glffct-aaaaaaaaaaaaaaa"},
		{ID: "linear lin_api_", Values: []string{"lin_api_aaaaaaaaaaaaaaaaaaaaaaaa", "lin_api_AAAAAAAAAAAAAAAAAAAAAAAA", "lin_api_111111111111111111111111", "lin_api_aaaaaaaaaaaaaaaa"}, Short: "lin_api_aaaaaaaaaaaaaaa"},
		{ID: "linear lin_oauth_", Values: []string{"lin_oauth_aaaaaaaaaaaaaaaaaaaaaaaa", "lin_oauth_AAAAAAAAAAAAAAAAAAAAAAAA", "lin_oauth_111111111111111111111111", "lin_oauth_aaaaaaaaaaaaaaaa"}, Short: "lin_oauth_aaaaaaaaaaaaaaa"},
		{ID: "atlassian ATATT", Values: []string{"ATATTaaaaaaaaaaaaaaaaaaaaaaaa", "ATATTAAAAAAAAAAAAAAAAAAAAAAAA", "ATATT111111111111111111111111", "ATATT________________________", "ATATT------------------------", "ATATT========================", "ATATTaaaaaaaaaaaaaaaa"}, Short: "ATATTaaaaaaaaaaaaaaa"},
		{ID: "atlassian ATCTT", Values: []string{"ATCTTaaaaaaaaaaaaaaaaaaaaaaaa", "ATCTTAAAAAAAAAAAAAAAAAAAAAAAA", "ATCTT111111111111111111111111", "ATCTT________________________", "ATCTT------------------------", "ATCTT========================", "ATCTTaaaaaaaaaaaaaaaa"}, Short: "ATCTTaaaaaaaaaaaaaaa"},
		{ID: "pagerduty pdus+_", Values: []string{"pdus+_aaaaaaaaaaaaaaaaaaaaaaaa", "pdus+_AAAAAAAAAAAAAAAAAAAAAAAA", "pdus+_111111111111111111111111", "pdus+_________________________", "pdus+_------------------------", "pdus+_++++++++++++++++++++++++", "pdus+_////////////////////////", "pdus+_========================", "pdus+_aaaaaaaaaaaaaaaa"}, Short: "pdus+_aaaaaaaaaaaaaaa"},
		{ID: "jwt", Values: []string{"eyJaaaaaaaaaa.eyJaaaaaaaaaa.aaaaaaaaaaaaaaaa", "eyJAAAAAAAAAA.eyJAAAAAAAAAA.AAAAAAAAAAAAAAAA", "eyJ1111111111.eyJ1111111111.1111111111111111", "eyJ__________.eyJ__________.________________", "eyJ----------.eyJ----------.----------------", "eyJaaaaaaaa.aaaaaaaa.aaaaaaaa"}, Short: "eyJaaaaaaa.eyJaaaaaaaaaa.aaaaaaaaaaaaaaaa"},
		{ID: "opaque exactly 20 bytes", Opaque: true, Values: []string{"a1a1a1a1a1a1a1a1a1a1", "a1a1a1a1a1a1a1a1a1aa"}, Short: "a1a1a1a1a1a1a1a1a1a"},
		{ID: "opaque hex", Opaque: true, Values: []string{"a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0"}},
		{ID: "opaque mixed", Opaque: true, Values: []string{"a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"}},
		{ID: "opaque base64url", Opaque: true, Values: []string{"a1_a1-a1_a1-a1_a1-a1_a1-a1_a1-a1"}},
	}
}

// Context is a sentence a value appears in. NeedsWord: the sentence has a credential word before the value, so an Opaque shape
// is expected to be redacted in it.
type Context struct {
	Template  string
	NeedsWord bool
}

// Contexts returns the sentence forms: a bare value, provider rejection wordings, a Go transport error, header and prose forms,
// and the forms a JSON body or an escaped string puts directly behind a word character.
func Contexts() []Context {
	return []Context{
		{"%s", false},
		{"Incorrect API key provided: %s. You can find your API key at the dashboard", true},
		{"401 {\"error\":{\"message\":\"invalid x-api-key\"}} for %s", false},
		{"Authorization failed for %s", true},
		{"request failed: key %s rejected by provider", true},
		{"Get \"https://api.example.test/v1/models?x=1\": rejected %s (status 401)", false},
		{"the secret is %s", true},
		{"password %s was rejected", true},
		{"invalid token %s from provider", true},
		{"bearer %s expired", true},
		{"the password is %s", true},
		{"credential rejected: %s", true},
		{"token provided: %s", true},
		{"authorization is %s", true},
		{"api key is %s", true},
		{"secret was %s", true},
		{"access_token %s was refused", true},
		{"passwd rejected %s", true},
		{"key\t%s", true},
		{"secret:%s", true},
		{"key=%s", true},
		{"key(%s)", true},
		{"key[%s]", true},
		{"key\"%s\"", true},
		{"key'%s'", true},
		{"key\\t%s", true},
		{"key\\n%s", true},
		{"key\\r%s", true},
		{"key\\\"%s", true},
		{"key\\'%s", true},
		{"key\\u0022%s", true},
		{"key\\u0027%s", true},
		{"passwd %s", true},
		{"key invalid %s", true},
		{"key incorrect %s", true},
		{"token expired %s", true},
		{"token revoked %s", true},
		{"key unknown %s", true},
		{"key bad %s", true},
		{"the secret of the %s", true},
		{"a key of a %s", true},
		{"key is the invalid %s", true},
		{"private token %s was refused", true},
		{"the key \"%s\" was rejected", true},
		{"the secret ('%s') is wrong", true},
		{"token: [%s]", true},
		{"bearer \\\"%s\\\" expired", true},
		{"key \\u0022%s\\u0022", true},
		{"password: '%s'", true},
		// directly behind a word character: an escaped tab or newline of a JSON body, an underscore, a digit, a letter
		{"{\"message\":\"bad credential:\\n%s\"}", false},
		{"{\"message\":\"bad credential:\\t%s\"}", false},
		{"{\"message\":\"bad credential:\\u0022%s\"}", false},
		{"id_%s", false},
		{"9%s", false},
		{"x%s", false},
		{"{\"error\":{\"message\":\"Incorrect API key provided:\\n%s\"}}", false},
		{"detail:\\t%s end", false},
		{"{\\\"key\\\":\\\"%s\\\"}", false},
		{"value \\u0022%s\\u0022", false},
		{"prefix_%s", false},
		{"id7%s", false},
		{"abc%s", false},
		{"/v1/keys/%s/rotate", false},
		{"https://api.example.test/v1?q=%s&x=1", false},
		{"\t%s", false},
		{"[%s] (%s) <%s>", false},
		{"key:%s", false},
		{"k=%s", false},
		{"x-%s", false},
		{"v1.%s", false},
		{"caf\u00e9%s", false},
		{"line one\n%s\nline three", false},
		{"value%%3D%%22%s%%22", false},
		{"%sandmoretext", false},
		{"%s and again %s", false},
	}
}

// AcceptedCost is one CLASS of text a redactor changes ON PURPOSE although it is not a credential, with one example. A
// redactor fails safe: a false positive costs readable text, a false negative costs a credential (D4201). A later widening of a
// class shows as a diff of this list.
type AcceptedCost struct {
	Class   string
	Example string
}

// AcceptedCosts are the classes redacted on purpose; the tests pin that each example IS redacted.
func AcceptedCosts() []AcceptedCost {
	return []AcceptedCost{
		{"a word that ends in a documented short prefix, followed by a long run of the key alphabet (sk-)", "task-aaaaaaaaaaaaaaaaaaaaaaaa"},
		{"a lower-case hyphenated phrase that starts with a documented sub-prefix (sk-admin-, sk-svcacct-) after any letter", "task-admin-console-deployment-production-01"},
		{"a word that ends in a GitLab prefix (glpat-) followed by 16+ key characters", "xglpat-aaaaaaaaaaaaaaaaaaaaaaaa"},
		{"a word that ends in lin_api_ followed by 16+ alphanumerics", "berlin_api_aaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"a word that ends in gh?_ followed by 16+ alphanumerics", "weighs_aaaaaaaaaaaaaaaaaaaaaaaa"},
		{"a UUID or any digit-and-letter value of 20+ bytes right behind a credential word", "key 123e4567-e89b-12d3-a456-426614174000"},
	}
}

// Negatives are ordinary sentences and near-misses that every redactor must leave unchanged.
func Negatives() []string {
	return []string{
		"ask-the-team-about-the-quarterly-planning-review", "flask-app-deployment-production-cluster-01",
		"commit 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b", "id 123e4567-e89b-12d3-a456-426614174000",
		"cursor eyJwYWdlIjoyLCJwZXJfcGFnZSI6MTAwfQ next", "a pk_live_ publishable key id",
		"task xoxo-team-building-event-2026 planned", "the AIzaSyncJob finished in 3 seconds", "bucket rk_test_fixtures is empty",
		"mask-sensitive-fields-before-export-step", "sk-learn-pipeline-v2",
		"the task-force-assignment-with-a-long-hyphenated-name failed",
		"sk-short", "AIzaShort", "whsec_short", "eyJ.notatoken", "eyJabc.def.ghi", "eyJaaaaaaa.aaaaaaaa.aaaaaaaa", "eyJaaaaaaaa.aaaaaaa.aaaaaaaa", "eyJaaaaaaaa.aaaaaaaa.aaaaaaa", "eyJaaaaaaaaaa.b.c", "eyJaaaaaaaaaa.eyJaaaaaaaaaa.c", "sk_live_ is a prefix",
		"disk-usage-high-on-node-number-12345-of-the-cluster-ok",
		"risk-assessment-for-the-quarterly-planning-review-of-teams",
		"eu-sk-bratislava-datacenter-rack-12-row-4",
		"node sk-north-saskatchewan-region-01 is ready",
		"/orgs/42/work-graph/edges/xox-unit",
		"the key was missing",
		"the key a1b2c3d4e5f6g7h8i9j is only nineteen bytes",
		"api key not found for the organization",
		"invalid key rotation-is-scheduled-for-next-quarter-window",
		"secret is not configured for this integration",
		"password expired",
		"the holder of a ticket is a bearer",
		"key 1234567890123456789012345 is not an api key (digits only)",
		"authorization failed for the request",
		"token is invalid",
		"credential for org 42 was rotated",
	}
}

// Expand puts value into every "%s" of the template ("%%" is one percent sign).
func Expand(template, value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(template, "%s", value), "%%", "%")
}

// Tail is the last 12 bytes of a value: a redactor that kept part of the credential keeps that.
func Tail(value string) string { return value[len(value)-12:] }
