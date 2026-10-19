package session

import (
	"bytes"
	"encoding/hex"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func testKey(t *testing.T) (contracts.SecretString, *[successorKeyBytes]byte) {
	t.Helper()
	raw, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	encoded := strings.TrimPrefix(raw.Reveal(), credentialPrefix)
	key, err := decodeSuccessorKey(contracts.NewSecretString(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return contracts.NewSecretString(encoded), key
}

func TestCredentialIsARefreshRequestV1Value(t *testing.T) {
	root := repoRoot(t)
	_, key := testKey(t)
	for i := 0; i < 64; i++ {
		credential, err := newCredential()
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []contracts.SecretString{credential, deriveSuccessor(key, uuid.New(), credential)} {
			if !wellFormed(value) {
				t.Fatalf("a minted credential is not well formed (length %d)", len(value.Reveal()))
			}
			doc, _ := json.Marshal(map[string]string{"schema_version": contracts.RefreshRequestSurface, "refresh_credential": value.Reveal()})
			parsed, err := contracts.ParseRefreshRequest(root, doc)
			if err != nil {
				t.Fatalf("refresh-request.v1 refused a minted credential: %v", err)
			}
			if parsed.RefreshCredential.Reveal() != value.Reveal() {
				t.Fatal("refresh-request.v1 round trip changed the credential")
			}
		}
	}
}

func TestWellFormedRefusesOtherShapes(t *testing.T) {
	good, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	v := good.Reveal()
	for name, value := range map[string]string{
		"empty":           "",
		"jwt":             "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.c2ln",
		"other prefix":    "dhr2_" + v[len(credentialPrefix):],
		"no prefix":       v[len(credentialPrefix):],
		"short":           v[:len(v)-1],
		"long":            v + "A",
		"padded":          v[:len(v)-1] + "=",
		"standard base64": v[:len(v)-1] + "+",
	} {
		if wellFormed(contracts.NewSecretString(value)) {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSuccessorIsDeterministicAndBoundToKeyChildAndParent(t *testing.T) {
	_, key := testKey(t)
	_, other := testKey(t)
	parent, _ := newCredential()
	otherParent, _ := newCredential()
	child := uuid.New()
	first := deriveSuccessor(key, child, parent).Reveal()
	if again := deriveSuccessor(key, child, parent).Reveal(); again != first {
		t.Fatal("the same inputs derived two successors; a grace replay could not return the same one")
	}
	for name, got := range map[string]string{
		"other key":    deriveSuccessor(other, child, parent).Reveal(),
		"other child":  deriveSuccessor(key, uuid.New(), parent).Reveal(),
		"other parent": deriveSuccessor(key, child, otherParent).Reveal(),
	} {
		if got == first {
			t.Errorf("%s derived the same successor", name)
		}
	}
	if first == parent.Reveal() {
		t.Error("the successor equals its parent")
	}
}

func TestSuccessorKeyMustBe32Bytes(t *testing.T) {
	for name, value := range map[string]string{
		"empty":    "",
		"16 bytes": encoding.EncodeToString(make([]byte, 16)),
		"33 bytes": encoding.EncodeToString(make([]byte, 33)),
		"padded":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
		"not b64":  strings.Repeat("!", 43),
	} {
		if _, err := decodeSuccessorKey(contracts.NewSecretString(value)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := decodeSuccessorKey(contracts.NewSecretString(encoding.EncodeToString(make([]byte, 32)))); err != nil {
		t.Errorf("a 32-byte key was refused: %v", err)
	}
}

func TestNewRefusesANegativeSessionLifetime(t *testing.T) {
	keyText, _ := testKey(t)
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Pool: pool, Schema: schema, SuccessorKey: keyText, SessionLifetime: -time.Second}); err == nil {
		t.Fatal("a negative session lifetime was accepted")
	}
	store, err := New(Config{Pool: pool, Schema: schema, SuccessorKey: keyText})
	if err != nil || store.sessionLifetime != 0 || store.refreshTTL != defaultRefreshTTL {
		t.Fatalf("defaults = lifetime %v, refresh %v, %v; want no cap and %v", store.sessionLifetime, store.refreshTTL, err, defaultRefreshTTL)
	}
}

// The store's amr set is session.v1's enum, read from the schema itself, so
// a value one side accepts and the other refuses cannot ship.
func TestAMRSetIsTheSessionV1Enum(t *testing.T) {
	raw, err := os.ReadFile(contracts.SchemaPath(repoRoot(t), contracts.SessionSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			AMR struct {
				Items struct {
					Enum []string `json:"enum"`
				} `json:"items"`
			} `json:"amr"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	enum := schema.Properties.AMR.Items.Enum
	if len(enum) != len(amrValues) {
		t.Fatalf("schema enum %v, store set %v", enum, amrValues)
	}
	for _, v := range enum {
		if !amrValues[v] {
			t.Errorf("session.v1 allows %q, the store refuses it", v)
		}
	}
	for _, ok := range [][]string{{"fed"}, {"fed", "mfa"}, {"pwd"}} {
		if !validAMR(ok) {
			t.Errorf("validAMR(%v) refused", ok)
		}
	}
	for _, bad := range [][]string{nil, {}, {"sms"}, {"FED"}, {"federated"}, {"fed", "fed"}, {"pwd", ""}} {
		if validAMR(bad) {
			t.Errorf("validAMR(%v) accepted", bad)
		}
	}
}

func TestReasonsAreAuthOutcomeCodes(t *testing.T) {
	known := map[string]bool{}
	for _, code := range contracts.AuthOutcomeReasonCodes {
		known[code] = true
	}
	success := map[string]bool{}
	for _, code := range contracts.AuthOutcomeSuccessReasons {
		success[code] = true
	}
	for _, reason := range allReasons {
		if !known[string(reason)] {
			t.Errorf("reason %q is not an auth-outcome.v1 reason code", reason)
		}
		if reason.Success() != success[string(reason)] {
			t.Errorf("reason %q: Success() = %v, contract says %v", reason, reason.Success(), success[string(reason)])
		}
	}
}

func TestWireIDsRoundTripAndMatchSessionV1(t *testing.T) {
	id := uuid.New()
	wire := WireSessionID(id)
	back, err := ParseWireSessionID(wire)
	if err != nil || back != id {
		t.Fatalf("ParseWireSessionID(%q) = %v, %v; want %v", wire, back, err, id)
	}
	for _, bad := range []string{"", "ses_", "prn_" + wire[4:], wire + "A", "ses_" + strings.Repeat("A", 21)} {
		if _, err := ParseWireSessionID(bad); err == nil {
			t.Errorf("ParseWireSessionID(%q) accepted", bad)
		}
	}

	created := time.Date(2026, 10, 19, 9, 0, 0, 123456789, time.UTC)
	revoked := created.Add(time.Hour)
	for _, r := range []*time.Time{nil, &revoked} {
		record := Record{ID: id, PrincipalID: uuid.New(), Revision: 3, AuthTime: created, AMR: []string{"pwd"},
			CreatedAt: created, ExpiresAt: created.Add(30 * 24 * time.Hour), RevokedAt: r}
		doc, err := json.Marshal(record.Wire())
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := contracts.ParseSession(repoRoot(t), doc)
		if err != nil {
			t.Fatalf("session.v1 refused a record's wire form: %v\n%s", err, doc)
		}
		if parsed.SessionID != wire || int64(parsed.Revision) != 3 || (parsed.RevokedAt == nil) != (r == nil) {
			t.Errorf("wire form lost a field: %s", doc)
		}
	}
}

// Every way the package's values can be printed must hide the credential and
// the successor key: fmt verbs, slog text and JSON, json.Marshal.
func TestCredentialsAndKeyAreNeverPrinted(t *testing.T) {
	keyText, key := testKey(t)
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(Config{Pool: pool, Schema: schema, SuccessorKey: keyText})
	if err != nil {
		t.Fatal(err)
	}
	parent, _ := newCredential()
	successor := deriveSuccessor(key, uuid.New(), parent)
	result := Result{Reason: ReasonAccepted, Issued: Issued{Credential: successor}}
	cfg := Config{SuccessorKey: keyText}

	var buf bytes.Buffer
	for _, value := range []any{result, &result, result.Issued, store, *store, cfg} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%b", "%o", "%O", "%c", "%U", "%e", "%f", "%g", "%t", "%p", "%10d", "%-12x", "% x", "%08b"} {
			fmt.Fprintf(&buf, verb+"\n", value)
		}
		if doc, err := json.Marshal(value); err == nil {
			buf.Write(doc)
		}
		slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "v", value)
		slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", "v", value)
	}
	out := buf.String()
	for name, secret := range map[string]string{
		"successor": successor.Reveal(), "successor body": successor.Reveal()[len(credentialPrefix):],
		"key": keyText.Reveal(), "key bytes": fmt.Sprint(key[:]),
		"successor hex": hex.EncodeToString([]byte(successor.Reveal())), "key hex": hex.EncodeToString([]byte(keyText.Reveal())),
	} {
		if strings.Contains(out, secret) {
			t.Errorf("the %s was printed", name)
		}
	}
	if !strings.Contains(out, "[redacted]") {
		t.Error("no redaction marker printed; the probe printed nothing useful")
	}
}
