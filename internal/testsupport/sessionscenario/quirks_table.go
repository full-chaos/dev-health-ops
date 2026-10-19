package sessionscenario

// Quirks classes every recorded behaviour of the corpus goldens that differs from a clean contract
// (CHAOS-10157). A reimplementation (auth-service) keeps Contractual entries, may answer Accidental ones
// cleanly, and must not keep Unsafe ones. TestQuirkTable fails on a golden quirk with no entry here.
var Quirks = []Quirk{
	{
		ID: "validation-echo", Class: Unsafe, Detector: "validation-echo",
		Reason: "FastAPI 422 bodies echo the submitted input and ctx, which can be a password or the whole request; the Go routes never write them",
	},
	{
		ID: "lone-surrogate-500", Class: Accidental,
		Steps:  []Step{{"login: password with lone surrogate", 500}, {"validate: lone surrogate", 500}, {"burst: login surrogate password 1", 500}, {"burst: login surrogate password 2", 500}, {"burst: login surrogate password 3", 500}, {"burst: login surrogate password past the limit", 500}, {"register: password lone surrogate", 500}, {"register: full name with a lone surrogate", 500}},
		Reason: "Python raised while encoding a lone surrogate and answered 500; a clean 4xx refusal breaks no client",
	},
	{
		ID: "origin-urlparse-500", Class: Accidental,
		Steps:  []Step{{"register: origin urlparse error", 500}, {"register: referer urlparse error", 500}},
		Reason: "urlparse raised on a malformed Origin or Referer and Python answered 500; a clean 403 is the intent",
	},
	{
		ID: "non-ascii-signature-500", Class: Accidental,
		Steps:  []Step{{"verify: non-ascii signature", 500}, {"reset: non-ascii signature", 500}, {"accept: non-ascii signature", 500}},
		Reason: "a link token with a non-ASCII signature raised in the comparison and Python answered 500; invalid-token 400 is the intent",
	},
	{
		ID: "forged-claim-type-500", Class: Accidental,
		Steps:  []Step{{"refresh: forged, int org", 500}, {"refresh: forged, int sub", 500}},
		Reason: "a correctly signed refresh token with a non-string org or sub claim raised and Python answered 500; a 401 is the intent",
	},
	{
		ID: "unknown-org-500", Class: Accidental,
		Steps:  []Step{{"login: bob unknown org", 500}},
		Reason: "a superuser login naming an org that does not exist raised and Python answered 500; no client relies on it",
	},
	{
		ID: "password-over-72-bytes-register-500", Class: Accidental,
		Steps:  []Step{{"register: password over 72 bytes", 500}},
		Reason: "bcrypt refuses a password over 72 bytes and Python answered 500 on register (login answers 401); a policy refusal is the intent",
	},
	{
		ID: "provider-payload-shape-500", Class: Accidental,
		Steps:  []Step{{"social: github emails not objects", 500}, {"social: github no id", 500}, {"social: github not json", 500}, {"social: gitlab username taken", 500}},
		Reason: "an unexpected provider payload shape, or a taken username on create, raised and Python answered 500; a refusal naming the provider failure is the intent",
	},
	{
		ID: "provider-not-configured-503", Class: Contractual,
		Steps:  []Step{{"social: google not configured", 503}},
		Reason: "a provider without client credentials answers 503 with a message; Web shows it",
	},
	{
		ID: "social-link-by-email", Class: Unsafe,
		Steps:  []Step{{"social: github email of a local user", 200}, {"social: github email of a github user with another id", 200}, {"social: github email of a github user without an id", 200}},
		Reason: "replaced by issuer+subject identity binding in auth-service (CHAOS-3275 C4/C5)",
	},
	{
		ID: "refresh-grace-window", Class: Contractual,
		Steps:  []Step{{"refresh: original again inside the grace window", 200}, {"refresh: rotation race, original inside the window", 401}, {"refresh: original outside the grace window (reuse)", 401}},
		Reason: "a rotated refresh token is accepted again inside the grace window and answers reuse detection after it; concurrent browser tabs depend on it",
	},
	{
		ID: "display-name-email-login", Class: Accidental,
		Steps:  []Step{{"login: display-name form, upper case", 200}},
		Reason: "the email parser accepts the \"Name <addr>\" form and login lower-cases it; no client sends it",
	},
}
