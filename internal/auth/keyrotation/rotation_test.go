package keyrotation

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

var t0 = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func publicKey(t *testing.T) ed25519.PublicKey {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return public
}

// fixture is a table: A active, P pending since t0, R retiring since t0,
// D retired, X compromised.
type fixture struct {
	rows    []Row
	publics map[string]ed25519.PublicKey
}

func newFixture(t *testing.T) fixture {
	f := fixture{publics: map[string]ed25519.PublicKey{}}
	for _, row := range []struct {
		kid   string
		state token.State
	}{{"A", token.StateActive}, {"P", token.StatePending}, {"R", token.StateRetiring}, {"D", token.StateRetired}, {"X", token.StateCompromised}} {
		f.publics[row.kid] = publicKey(t)
		f.rows = append(f.rows, Row{KeyID: row.kid, State: row.state, PublicKey: f.publics[row.kid], StateSince: t0})
	}
	return f
}

func without(rows []Row, kid string) []Row {
	var out []Row
	for _, row := range rows {
		if row.KeyID != kid {
			out = append(out, row)
		}
	}
	return out
}

func move(kid string, from, to token.State) Move { return Move{KeyID: kid, From: from, To: to} }

const (
	pending     = token.StatePending
	active      = token.StateActive
	retiring    = token.StateRetiring
	retired     = token.StateRetired
	compromised = token.StateCompromised
)

// TestDecideTransitions: every legal move and every refusal, one row each.
// The row count is pinned so a transition cannot be added or dropped
// silently.
func TestDecideTransitions(t *testing.T) {
	f := newFixture(t)
	afterPending := t0.Add(PendingFloor)
	afterRetiring := t0.Add(RetiringFloor)
	newKey := publicKey(t)
	custody := []token.Key{{KeyID: "N", State: pending, PublicKey: newKey}}
	cases := []struct {
		name    string
		request Request
		rows    []Row
		now     time.Time
		want    []Move
		refused Reason
	}{
		// legal
		{name: "register a custodied key as pending", request: Request{Verb: VerbRegister, KeyID: "N", Custody: custody},
			rows: f.rows, now: t0, want: []Move{{KeyID: "N", To: pending, PublicKey: newKey}}},
		{name: "activate pending at the floor; the active key retires", request: Request{Verb: VerbActivate, KeyID: "P"},
			rows: f.rows, now: afterPending, want: []Move{move("A", active, retiring), move("P", pending, active)}},
		{name: "activate pending with no active key", request: Request{Verb: VerbActivate, KeyID: "P"},
			rows: without(f.rows, "A"), now: afterPending, want: []Move{move("P", pending, active)}},
		{name: "retire retiring at the floor", request: Request{Verb: VerbRetire, KeyID: "R"},
			rows: f.rows, now: afterRetiring, want: []Move{move("R", retiring, retired)}},
		{name: "rollback retiring to active; the active key retires", request: Request{Verb: VerbRollback, KeyID: "R"},
			rows: f.rows, now: t0, want: []Move{move("A", active, retiring), move("R", retiring, active)}},
		{name: "rollback retired to pending", request: Request{Verb: VerbRollback, KeyID: "D"},
			rows: f.rows, now: t0, want: []Move{move("D", retired, pending)}},
		{name: "compromise active", request: Request{Verb: VerbCompromise, KeyID: "A"},
			rows: f.rows, now: t0, want: []Move{move("A", active, compromised)}},
		{name: "compromise active with a ready successor", request: Request{Verb: VerbCompromise, KeyID: "A", Successor: "P"},
			rows: f.rows, now: afterPending, want: []Move{move("A", active, compromised), move("P", pending, active)}},
		{name: "compromise pending", request: Request{Verb: VerbCompromise, KeyID: "P"},
			rows: f.rows, now: t0, want: []Move{move("P", pending, compromised)}},
		{name: "compromise retiring", request: Request{Verb: VerbCompromise, KeyID: "R"},
			rows: f.rows, now: t0, want: []Move{move("R", retiring, compromised)}},
		{name: "compromise retired", request: Request{Verb: VerbCompromise, KeyID: "D"},
			rows: f.rows, now: t0, want: []Move{move("D", retired, compromised)}},
		{name: "adopt custody into an empty table", request: Request{Verb: VerbAdopt, Custody: []token.Key{
			{KeyID: "a", State: active, PublicKey: f.publics["A"]}, {KeyID: "x", State: compromised, PublicKey: f.publics["X"]},
		}}, rows: nil, now: t0, want: []Move{{KeyID: "a", To: active, PublicKey: f.publics["A"]}, {KeyID: "x", To: compromised, PublicKey: f.publics["X"]}}},

		// refused
		{name: "activate before the publish floor", request: Request{Verb: VerbActivate, KeyID: "P"},
			rows: f.rows, now: afterPending.Add(-time.Second), refused: ReasonFloorNotReached},
		{name: "activate a retiring key", request: Request{Verb: VerbActivate, KeyID: "R"},
			rows: f.rows, now: afterRetiring, refused: ReasonWrongState},
		{name: "activate the active key", request: Request{Verb: VerbActivate, KeyID: "A"},
			rows: f.rows, now: afterPending, refused: ReasonWrongState},
		{name: "retire before the expiry floor", request: Request{Verb: VerbRetire, KeyID: "R"},
			rows: f.rows, now: afterRetiring.Add(-time.Second), refused: ReasonFloorNotReached},
		{name: "retire the active key", request: Request{Verb: VerbRetire, KeyID: "A"},
			rows: f.rows, now: afterRetiring, refused: ReasonWrongState},
		{name: "retire a pending key", request: Request{Verb: VerbRetire, KeyID: "P"},
			rows: f.rows, now: afterRetiring, refused: ReasonWrongState},
		{name: "rollback the active key", request: Request{Verb: VerbRollback, KeyID: "A"},
			rows: f.rows, now: t0, refused: ReasonWrongState},
		{name: "rollback a pending key", request: Request{Verb: VerbRollback, KeyID: "P"},
			rows: f.rows, now: t0, refused: ReasonWrongState},
		{name: "rollback a compromised key", request: Request{Verb: VerbRollback, KeyID: "X"},
			rows: f.rows, now: t0, refused: ReasonCompromisedTerminal},
		{name: "activate a compromised key", request: Request{Verb: VerbActivate, KeyID: "X"},
			rows: f.rows, now: afterPending, refused: ReasonCompromisedTerminal},
		{name: "retire a compromised key", request: Request{Verb: VerbRetire, KeyID: "X"},
			rows: f.rows, now: afterRetiring, refused: ReasonCompromisedTerminal},
		{name: "compromise a compromised key", request: Request{Verb: VerbCompromise, KeyID: "X"},
			rows: f.rows, now: t0, refused: ReasonCompromisedTerminal},
		{name: "unknown kid", request: Request{Verb: VerbActivate, KeyID: "nope"},
			rows: f.rows, now: afterPending, refused: ReasonKeyUnknown},
		{name: "verb on an empty table", request: Request{Verb: VerbActivate, KeyID: "P"},
			rows: nil, now: afterPending, refused: ReasonTableEmpty},
		{name: "unknown verb", request: Request{Verb: "promote", KeyID: "P"},
			rows: f.rows, now: afterPending, refused: ReasonUnknownVerb},
		{name: "successor on a planned verb", request: Request{Verb: VerbActivate, KeyID: "P", Successor: "R"},
			rows: f.rows, now: afterPending, refused: ReasonSuccessorIneligible},
		{name: "successor before its publish floor", request: Request{Verb: VerbCompromise, KeyID: "A", Successor: "P"},
			rows: f.rows, now: afterPending.Add(-time.Second), refused: ReasonSuccessorIneligible},
		{name: "successor not pending", request: Request{Verb: VerbCompromise, KeyID: "A", Successor: "R"},
			rows: f.rows, now: afterPending, refused: ReasonSuccessorIneligible},
		{name: "successor is the compromised key", request: Request{Verb: VerbCompromise, KeyID: "P", Successor: "P"},
			rows: without(f.rows, "A"), now: afterPending, refused: ReasonSuccessorIneligible},
		{name: "successor unknown", request: Request{Verb: VerbCompromise, KeyID: "A", Successor: "nope"},
			rows: f.rows, now: afterPending, refused: ReasonSuccessorIneligible},
		{name: "successor while another key stays active", request: Request{Verb: VerbCompromise, KeyID: "R", Successor: "P"},
			rows: f.rows, now: afterPending, refused: ReasonSuccessorIneligible},
		{name: "register a kid already in the table", request: Request{Verb: VerbRegister, KeyID: "N", Custody: custody},
			rows: append(append([]Row(nil), f.rows...), Row{KeyID: "N", State: retired, PublicKey: publicKey(t), StateSince: t0}),
			now:  t0, refused: ReasonKeyRegistered},
		{name: "register a compromised public key under a new kid", request: Request{Verb: VerbRegister, KeyID: "N",
			Custody: []token.Key{{KeyID: "N", State: pending, PublicKey: f.publics["X"]}}},
			rows: f.rows, now: t0, refused: ReasonKeyRegistered},
		{name: "register a kid custody does not hold", request: Request{Verb: VerbRegister, KeyID: "M", Custody: custody},
			rows: f.rows, now: t0, refused: ReasonKeyNotInCustody},
		{name: "register a key custody marks compromised", request: Request{Verb: VerbRegister, KeyID: "N",
			Custody: []token.Key{{KeyID: "N", State: compromised, PublicKey: newKey}}},
			rows: f.rows, now: t0, refused: ReasonWrongState},
		{name: "register on an empty table", request: Request{Verb: VerbRegister, KeyID: "N", Custody: custody},
			rows: nil, now: t0, refused: ReasonTableEmpty},
		{name: "register with a successor", request: Request{Verb: VerbRegister, KeyID: "N", Successor: "P", Custody: custody},
			rows: f.rows, now: t0, refused: ReasonSuccessorIneligible},
		{name: "adopt into a table that has rows", request: Request{Verb: VerbAdopt, Custody: custody},
			rows: f.rows, now: t0, refused: ReasonTableNotEmpty},
		{name: "adopt custody with no active key", request: Request{Verb: VerbAdopt, Custody: custody},
			rows: nil, now: t0, refused: ReasonCustodyNotOneActive},
		{name: "adopt custody with two active keys", request: Request{Verb: VerbAdopt, Custody: []token.Key{
			{KeyID: "a", State: active, PublicKey: f.publics["A"]}, {KeyID: "b", State: active, PublicKey: f.publics["P"]},
		}}, rows: nil, now: t0, refused: ReasonCustodyNotOneActive},
	}
	const pinned = 42
	if len(cases) != pinned {
		t.Fatalf("%d transition rows, pinned %d: a row was added or removed; update the pin with the review", len(cases), pinned)
	}
	refusalsSeen := map[Reason]bool{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			moves, err := Decide(c.request, c.rows, c.now)
			if c.refused != "" {
				var refusal *Refusal
				if !errors.As(err, &refusal) || refusal.Reason != c.refused || moves != nil {
					t.Fatalf("Decide = %v, %v; want refusal %s and no moves", moves, err, c.refused)
				}
				refusalsSeen[c.refused] = true
				return
			}
			if err != nil {
				t.Fatalf("Decide refused: %v", err)
			}
			if !reflect.DeepEqual(moves, c.want) {
				t.Fatalf("moves = %v, want %v", moves, c.want)
			}
		})
	}
	for _, reason := range Reasons {
		if !refusalsSeen[reason] {
			t.Errorf("refusal %s has no row", reason)
		}
	}
}

// TestCompromisedIsTerminalForEveryVerb: no verb moves a compromised key, at
// any time.
func TestCompromisedIsTerminalForEveryVerb(t *testing.T) {
	f := newFixture(t)
	for _, verb := range []Verb{VerbActivate, VerbRetire, VerbRollback, VerbCompromise} {
		for _, now := range []time.Time{t0, t0.Add(RetiringFloor * 10)} {
			_, err := Decide(Request{Verb: verb, KeyID: "X"}, f.rows, now)
			var refusal *Refusal
			if !errors.As(err, &refusal) || refusal.Reason != ReasonCompromisedTerminal {
				t.Errorf("%s on a compromised key at %s: %v", verb, now, err)
			}
		}
	}
}

func TestFloors(t *testing.T) {
	if PendingFloor != 5*time.Minute {
		t.Errorf("PendingFloor = %s, want the 300 s JWKS cache TTL", PendingFloor)
	}
	if RetiringFloor != 70*time.Minute {
		t.Errorf("RetiringFloor = %s, want access TTL 3600 s + skew 300 s + cache TTL 300 s", RetiringFloor)
	}
	if RevokeWindow != 65*time.Minute {
		t.Errorf("RevokeWindow = %s, want access TTL 3600 s + skew 300 s", RevokeWindow)
	}
}

// TestAuditVocabularyMatchesTheStateMachine: the audit allowlist's
// rotation_verb and rotation_refusal are exactly this package's verbs and
// reasons, and every audit row a verb writes passes the allowlist.
func TestAuditVocabularyMatchesTheStateMachine(t *testing.T) {
	vocabulary := audit.AttributeVocabulary()
	var verbs, reasons []string
	for _, verb := range Verbs {
		verbs = append(verbs, string(verb))
	}
	for _, reason := range append(append([]Reason(nil), Reasons...), ReasonStateChanged) {
		reasons = append(reasons, string(reason))
	}
	for name, pair := range map[string][2][]string{
		"rotation_verb":    {vocabulary["rotation_verb"], verbs},
		"rotation_refusal": {vocabulary["rotation_refusal"], reasons},
	} {
		got, want := append([]string(nil), pair[0]...), append([]string(nil), pair[1]...)
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: audit vocabulary %v, state machine %v", name, got, want)
		}
	}

	f := newFixture(t)
	requests := map[Verb]struct {
		request Request
		rows    []Row
		now     time.Time
	}{
		VerbAdopt:      {Request{Verb: VerbAdopt, Custody: []token.Key{{KeyID: "a", State: active, PublicKey: f.publics["A"]}}}, nil, t0},
		VerbRegister:   {Request{Verb: VerbRegister, KeyID: "N", Custody: []token.Key{{KeyID: "N", State: pending, PublicKey: publicKey(t)}}}, f.rows, t0},
		VerbActivate:   {Request{Verb: VerbActivate, KeyID: "P"}, f.rows, t0.Add(PendingFloor)},
		VerbRetire:     {Request{Verb: VerbRetire, KeyID: "R"}, f.rows, t0.Add(RetiringFloor)},
		VerbRollback:   {Request{Verb: VerbRollback, KeyID: "R"}, f.rows, t0},
		VerbCompromise: {Request{Verb: VerbCompromise, KeyID: "A", Successor: "P"}, f.rows, t0.Add(PendingFloor)},
	}
	for _, verb := range Verbs {
		c := requests[verb]
		moves, err := Decide(c.request, c.rows, c.now)
		if err != nil {
			t.Fatalf("%s: %v", verb, err)
		}
		event := appliedEvent(c.request, moves)
		if err := audit.ValidateAttributes(event.Attributes); err != nil {
			t.Errorf("%s audit row refused: %v", verb, err)
		}
		if event.Attributes["key_id"] == nil || event.Attributes["new_status"] == nil {
			t.Errorf("%s audit row names no key or state: %v", verb, event.Attributes)
		}
	}
	if got := appliedEvent(requests[VerbActivate].request, []Move{move("A", active, retiring), move("P", pending, active)}).Attributes; got["key_id"] != "P" || got["related_key_id"] != "A" || got["previous_status"] != "pending" {
		t.Errorf("activate audit attributes = %v", got)
	}
	for _, reason := range append(append([]Reason(nil), Reasons...), ReasonStateChanged) {
		for _, kid := range []string{"P", "", "not a kid"} {
			event := refusedEvent(Request{Verb: VerbActivate, KeyID: kid}, reason)
			if err := audit.ValidateAttributes(event.Attributes); err != nil {
				t.Errorf("refused row (%s, %q) refused: %v", reason, kid, err)
			}
		}
	}
}

// keyMaterialForms is every printable form of a key's bytes an attribute could
// carry.
func keyMaterialForms(raw []byte) []string {
	return []string{
		string(raw), hex.EncodeToString(raw), base64.StdEncoding.EncodeToString(raw),
		base64.RawStdEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw), base64.URLEncoding.EncodeToString(raw),
	}
}

// TestNoKeyMaterialReachesAnAuditAttribute: the audit rows of every verb and
// every refusal carry kids and states only, never a key's bytes in any
// encoding.
func TestNoKeyMaterialReachesAnAuditAttribute(t *testing.T) {
	f := newFixture(t)
	newKey := publicKey(t)
	var forms []string
	for _, public := range f.publics {
		forms = append(forms, keyMaterialForms(public)...)
	}
	forms = append(forms, keyMaterialForms(newKey)...)
	custody := []token.Key{{KeyID: "N", State: pending, PublicKey: newKey}}
	adopt := []token.Key{{KeyID: "a", State: active, PublicKey: f.publics["A"]}, {KeyID: "x", State: compromised, PublicKey: f.publics["X"]}}
	var events []audit.AuditEvent
	for _, c := range []struct {
		request Request
		rows    []Row
		now     time.Time
	}{
		{Request{Verb: VerbAdopt, Custody: adopt}, nil, t0},
		{Request{Verb: VerbRegister, KeyID: "N", Custody: custody}, f.rows, t0},
		{Request{Verb: VerbActivate, KeyID: "P"}, f.rows, t0.Add(PendingFloor)},
		{Request{Verb: VerbRetire, KeyID: "R"}, f.rows, t0.Add(RetiringFloor)},
		{Request{Verb: VerbRollback, KeyID: "D"}, f.rows, t0},
		{Request{Verb: VerbCompromise, KeyID: "A", Successor: "P"}, f.rows, t0.Add(PendingFloor)},
	} {
		moves, err := Decide(c.request, c.rows, c.now)
		if err != nil {
			t.Fatalf("%s: %v", c.request.Verb, err)
		}
		events = append(events, appliedEvent(c.request, moves))
	}
	for _, reason := range append(append([]Reason(nil), Reasons...), ReasonStateChanged) {
		events = append(events, refusedEvent(Request{Verb: VerbRegister, KeyID: "N", Custody: custody}, reason))
	}
	for _, event := range events {
		encoded, err := json.Marshal(event.Attributes)
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range event.Attributes {
			if _, ok := value.(string); !ok {
				t.Errorf("%s attribute %s is %T; only strings are written", event.EventType, key, value)
			}
		}
		for _, form := range forms {
			if strings.Contains(string(encoded), form) {
				t.Fatalf("%s attributes carry key material: %s", event.EventType, encoded)
			}
		}
		if strings.Contains(string(encoded), "PRIVATE") || strings.Contains(string(encoded), "-----") {
			t.Fatalf("%s attributes carry PEM text: %s", event.EventType, encoded)
		}
	}
}

type fakeRow struct {
	kid, state string
	x          *string
	scanErr    bool
}

// fakeRowSet is a signing_keys result set; scanErr rows fail Scan.
type fakeRowSet struct {
	rows []fakeRow
	i    int
}

func (f *fakeRowSet) Next() bool { f.i++; return f.i <= len(f.rows) }

func (f *fakeRowSet) Scan(dest ...any) error {
	row := f.rows[f.i-1]
	if row.scanErr {
		return errors.New("scan failed")
	}
	*dest[0].(*string), *dest[1].(*string), *dest[2].(**string) = row.kid, row.state, row.x
	return nil
}

func (f *fakeRowSet) Err() error { return nil }
func (f *fakeRowSet) Close()     {}

// A row the reader cannot read fails the read, but every row is scanned and
// the readable rows come back with the error, so their compromises latch: a
// compromised row with an unreadable public key keeps its kid.
func TestReadKeyStatesScansPastAnUnreadableRow(t *testing.T) {
	good := func(public ed25519.PublicKey) *string {
		x := base64.RawURLEncoding.EncodeToString(public)
		return &x
	}
	bad := "not base64 !"
	pb, pd := publicKey(t), publicKey(t)
	set := &fakeRowSet{rows: []fakeRow{
		{kid: "a", state: "active", x: &bad},
		{scanErr: true},
		{kid: "b", state: "compromised", x: good(pb)},
		{kid: "c", state: "compromised", x: nil},
		{kid: "d", state: "active", x: good(pd)},
	}}
	rows, err := readKeyStates(set)
	if err == nil {
		t.Fatal("a read with unreadable rows succeeded")
	}
	got := map[string]token.KeyStateRow{}
	for _, row := range rows {
		got[row.KeyID] = row
	}
	if len(rows) != 3 || !got["b"].PublicKey.Equal(pb) || got["b"].State != compromised ||
		got["c"].State != compromised || got["c"].PublicKey != nil || !got["d"].PublicKey.Equal(pd) {
		t.Fatalf("rows = %+v; want b and d whole and c by kid", rows)
	}
}
