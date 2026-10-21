package contracts

import "errors"

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

// refuseCredential is the single exit of a credential-carrying parser: any
// error becomes a CredentialRefusal of that surface, so no stage, present or
// later, can return input text.
func refuseCredential(surface string, err error) error {
	var refusal *CredentialRefusal
	if errors.As(err, &refusal) {
		return &CredentialRefusal{Surface: surface, Stage: refusal.Stage, Rule: refusal.Rule}
	}
	return &CredentialRefusal{Surface: surface, Stage: StageUnclassified}
}
