// Package usertokens is test support for the negative proof that a user access token reaches nothing
// but the routes that accept it: it mints every kind of user credential the platform has (the legacy
// HS256 access token, the legacy refresh token, an EdDSA access token from the real issuer code path,
// and every accept vector of the access-token.v1 golden corpus), and a Postgres pool that counts
// connection attempts so "no store lookup happened" is a measured zero, not an assumption.
package usertokens

import (
	"context"
	"encoding/base64"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/tokencorpus"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	// Secret is the HS256 key of the legacy tokens minted here: a test value, never a real one.
	Secret = "usertokens-test-key-0123456789abcdef-0123"
	// LegacyIssuer and Audience are the legacy tokens' iss and aud.
	LegacyIssuer = "dev-health-ops"
	Audience     = "dev-health-api"
)

// Token is one user credential: Kind names how it was made, Value is the compact token.
type Token struct {
	Kind  string
	Value string
}

// Signature is the token's third segment, the part a leak check also looks for.
func (t Token) Signature() string {
	parts := strings.Split(t.Value, ".")
	if len(parts) == 3 {
		return parts[2]
	}
	return ""
}

// All returns the user credentials this package can mint without authapi: the legacy HS256 access token,
// the legacy refresh token and each accept vector of the corpus. Package withissuer adds the EdDSA access
// token of authapi's real issuer (it imports authapi, which authapi's own tests cannot).
func All(t testing.TB) []Token {
	t.Helper()
	out := []Token{
		{Kind: "hs256-access", Value: hs256Access(t)},
		{Kind: "legacy-refresh", Value: legacyRefresh(t)},
	}
	return append(out, corpusAccepts(t)...)
}

func hs256Access(t testing.TB) string {
	t.Helper()
	signer, err := edgetoken.NewSigner(Secret, LegacyIssuer, Audience)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.NewString()
	compact, err := signer.Access(edgetoken.AccessClaims{
		UserID: uuid.NewString(), Email: "ada@example.com", OrgID: uuid.NewString(), Role: "admin", TokenVersion: 1,
		SessionID: &sessionID,
	}, time.Now().UTC(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

func legacyRefresh(t testing.TB) string {
	t.Helper()
	signer, err := edgetoken.NewSigner(Secret, LegacyIssuer, Audience)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := signer.Refresh(edgetoken.RefreshClaims{
		UserID: uuid.NewString(), OrgID: uuid.NewString(), FamilyID: uuid.NewString(),
	}, time.Now().UTC(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

func corpusAccepts(t testing.TB) []Token {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := tokencorpus.Load(filepath.Join(root, tokencorpus.Dir))
	if err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding
	var out []Token
	for _, entry := range corpus.Manifest.Vectors {
		if entry.Expect != tokencorpus.Accept {
			continue
		}
		vector := corpus.Vectors[entry.File]
		signature, err := tokencorpus.DecodeSignature(vector)
		if err != nil {
			t.Fatalf("corpus vector %s: %v", entry.File, err)
		}
		compact := enc.EncodeToString([]byte(vector.HeaderJSON)) + "." + enc.EncodeToString([]byte(vector.PayloadJSON)) + "." + enc.EncodeToString(signature)
		out = append(out, Token{Kind: "eddsa-corpus:" + entry.File, Value: compact})
	}
	if len(out) == 0 {
		t.Fatal("the corpus has no accept vector")
	}
	return out
}

// NoLeak fails when any token (or its signature segment) appears in text: a refusal must not echo the
// credential it refused, in a body or a log.
func NoLeak(t testing.TB, where, text string, tokens []Token) {
	t.Helper()
	for _, tok := range tokens {
		if strings.Contains(text, tok.Value) || (tok.Signature() != "" && strings.Contains(text, tok.Signature())) {
			t.Errorf("%s carries the %s token", where, tok.Kind)
		}
	}
}

// Counter counts connection attempts of a CountingPool.
type Counter struct{ n atomic.Int64 }

// Count is the number of connection attempts so far.
func (c *Counter) Count() int64 { return c.n.Load() }

var errBlocked = errors.New("usertokens: connection blocked")

// CountingPool is a pgx pool no query can leave: every connection attempt is counted and refused. A code
// path that reaches the database through it shows Count() > 0 (and its query fails); a path that is
// refused before any lookup leaves Count() at zero.
func CountingPool(t testing.TB) (*pgxpool.Pool, *Counter) {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://refused:refused@127.0.0.1:1/refused?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	counter := &Counter{}
	config.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		counter.n.Add(1)
		return errBlocked
	}
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, counter
}
