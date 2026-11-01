package sessionissue

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func stateDocument(extra string) string {
	return `{"principal_id":"` + uuid.New().String() + `","organization_id":null,"revoked":false,"expired":false,"legacy_token_version":7` + extra + `}`
}

func TestParseStateReadsTheActorOfADelegatedSession(t *testing.T) {
	actor := uuid.New()
	state, err := parseState([]byte(stateDocument(`,"actor_principal_id":"` + actor.String() + `"`)))
	if err != nil {
		t.Fatal(err)
	}
	if state.ActorPrincipalID == nil || *state.ActorPrincipalID != actor {
		t.Fatalf("actor = %v, want %s", state.ActorPrincipalID, actor)
	}
}

func TestParseStateKeepsTheFiveMemberAnswer(t *testing.T) {
	state, err := parseState([]byte(stateDocument("")))
	if err != nil {
		t.Fatal(err)
	}
	if state.ActorPrincipalID != nil {
		t.Fatalf("actor = %v on a plain session", state.ActorPrincipalID)
	}
}

func TestParseStateRefusesAnUnknownSixthMember(t *testing.T) {
	for _, member := range []string{`,"client_class":"web"`, `,"actor":"x"`, `,"actor_principal_id2":null`} {
		if _, err := parseState([]byte(stateDocument(member))); err == nil {
			t.Fatalf("member %s was accepted", member)
		}
	}
}

func TestParseStateRefusesABadActor(t *testing.T) {
	upper := strings.ToUpper(uuid.New().String())
	for _, value := range []string{`null`, `7`, `""`, `"not-a-uuid"`, `"` + upper + `"`, `"prn_abc"`} {
		if _, err := parseState([]byte(stateDocument(`,"actor_principal_id":` + value))); err == nil {
			t.Fatalf("actor %s was accepted", value)
		}
	}
}

func TestParseStateRefusesTheActorInPlaceOfARequiredMember(t *testing.T) {
	doc := strings.Replace(stateDocument(`,"actor_principal_id":"`+uuid.New().String()+`"`), `"expired":false,`, ``, 1)
	if _, err := parseState([]byte(doc)); err == nil {
		t.Fatal("an answer without expired was accepted")
	}
}
