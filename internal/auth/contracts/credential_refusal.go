package contracts

// Refusal stages of a credential-carrying surface's parser.
const (
	StageDuplicateMember = "duplicate_member"
	StageSyntax          = "syntax"
	StageSchema          = "schema"
	StageDecode          = "decode"
	StageClientRule      = "client_rule"
	StageUnclassified    = "unclassified"
)

// credentialRefusalPhrase closes every refusal of a credential-carrying surface.
const credentialRefusalPhrase = "(text withheld: this surface carries credentials)"

// CredentialRefusal is the only error a credential-carrying surface's parser
// returns: the surface, the stage that refused and, for a client rule, its
// rule code. It holds nothing from the input, because callers log refusals and
// the input carries credentials.
type CredentialRefusal struct {
	Surface string
	Stage   string
	Rule    string
}

func (e *CredentialRefusal) Error() string {
	stage := e.Stage
	if e.Rule != "" {
		stage += " " + e.Rule
	}
	return e.Surface + ": refused at " + stage + " " + credentialRefusalPhrase
}
