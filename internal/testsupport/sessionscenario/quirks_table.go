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
		Names:  []string{"login: password with lone surrogate", "validate: lone surrogate", "burst: login surrogate password", "register: password lone surrogate", "register: full name with a lone surrogate"},
		Reason: "Python raised while encoding a lone surrogate and answered 500; a clean 4xx refusal breaks no client",
	},
	{
		ID: "origin-urlparse-500", Class: Accidental,
		Names:  []string{"register: origin urlparse error", "register: referer urlparse error"},
		Reason: "urlparse raised on a malformed Origin or Referer and Python answered 500; a clean 403 is the intent",
	},
	{
		ID: "non-ascii-signature-500", Class: Accidental,
		Names:  []string{"verify: non-ascii signature", "reset: non-ascii signature", "accept: non-ascii signature"},
		Reason: "a link token with a non-ASCII signature raised in the comparison and Python answered 500; invalid-token 400 is the intent",
	},
	{
		ID: "forged-claim-type-500", Class: Accidental,
		Names:  []string{"refresh: forged, int org", "refresh: forged, int sub"},
		Reason: "a correctly signed refresh token with a non-string org or sub claim raised and Python answered 500; a 401 is the intent",
	},
	{
		ID: "unknown-org-500", Class: Accidental,
		Names:  []string{"login: bob unknown org"},
		Reason: "a superuser login naming an org that does not exist raised and Python answered 500; no client relies on it",
	},
	{
		ID: "password-over-72-bytes-register-500", Class: Accidental,
		Names:  []string{"register: password over 72 bytes"},
		Reason: "bcrypt refuses a password over 72 bytes and Python answered 500 on register (login answers 401); a policy refusal is the intent",
	},
	{
		ID: "provider-payload-shape-500", Class: Accidental,
		Names:  []string{"social: github emails not objects", "social: github no id", "social: github not json", "social: gitlab username taken"},
		Reason: "an unexpected provider payload shape, or a taken username on create, raised and Python answered 500; a refusal naming the provider failure is the intent",
	},
	{
		ID: "provider-not-configured-503", Class: Contractual,
		Names:  []string{"social: google not configured"},
		Reason: "a provider without client credentials answers 503 with a message; Web shows it",
	},
	{
		ID: "social-link-by-email", Class: Unsafe,
		Names:  []string{"social: github email of a local user", "social: github email of a github user with another id", "social: github email of a github user without an id"},
		Reason: "replaced by issuer+subject identity binding in auth-service (CHAOS-3275 C4/C5)",
	},
	{
		ID: "refresh-grace-window", Class: Contractual,
		Names:  []string{"refresh: original again inside the grace window", "refresh: rotation race, original inside the window", "refresh: original outside the grace window (reuse)"},
		Reason: "a rotated refresh token is accepted again inside the grace window and answers reuse detection after it; concurrent browser tabs depend on it",
	},
	{
		ID: "display-name-email-login", Class: Accidental,
		Names:  []string{"login: display-name form, upper case"},
		Reason: "the email parser accepts the \"Name <addr>\" form and login lower-cases it; no client sends it",
	},
}
