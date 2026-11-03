package sso

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

const subjectMarker = "MARKER77"

// unusableSubjects are the bad forms of a provider subject; each carries the marker so a leak is visible.
func unusableSubjects() map[string]string {
	return map[string]string{
		"empty":           "",
		"nul":             subjectMarker + "\x00tail",
		"control":         subjectMarker + "\x01tail",
		"invalid utf8":    subjectMarker + "\xff\xfe",
		"lone surrogate":  subjectMarker + "\xed\xa0\x80",
		"edge whitespace": " " + subjectMarker,
	}
}

// countingPool is a pool whose every connection attempt is counted and refused: a statement needs a connection,
// so the counter is the number of database statements the code under test tried to run.
func countingPool(t *testing.T) (*pgxpool.Pool, *atomic.Int64) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	var dials atomic.Int64
	cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("no database in this test")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, &dials
}

func subjectGuardHandlers(t *testing.T) (handlers, *atomic.Int64, *bytes.Buffer) {
	t.Helper()
	pool, dials := countingPool(t)
	var logs bytes.Buffer
	return handlers{Deps{Pool: pool, Logger: slog.New(slog.NewTextHandler(&logs, nil))}}, dials, &logs
}

func assertRefusedBeforeAnyStatement(t *testing.T, err error, dials *atomic.Int64, logs *bytes.Buffer) {
	t.Helper()
	if !errors.Is(err, identity.ErrInvalidProviderID) {
		t.Fatalf("err = %v, want ErrInvalidProviderID", err)
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("%d database statements ran before the refusal", n)
	}
	if strings.Contains(err.Error(), subjectMarker) || strings.Contains(logs.String(), subjectMarker) {
		t.Fatal("the subject value is in the error or the logs")
	}
}

func TestProvisionOrGetUserRefusesAnUnusableSubjectBeforeAnyStatement(t *testing.T) {
	for _, protocol := range []string{"oidc", "saml"} {
		for name, subject := range unusableSubjects() {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				h, dials, logs := subjectGuardHandlers(t)
				row := &providerRow{OrgID: uuid.NewString(), Protocol: protocol, AutoProvision: true}
				_, _, err := h.provisionOrGetUser(context.Background(), row, "a@example.test", "A", uuid.New(), subject, nil)
				assertRefusedBeforeAnyStatement(t, err, dials, logs)
			})
		}
	}
}

func TestOAuthProvisionUserRefusesAnUnusableSubjectBeforeAnyStatement(t *testing.T) {
	for _, providerType := range []string{"github", "gitlab", "google"} {
		for name, subject := range unusableSubjects() {
			t.Run(providerType+"/"+name, func(t *testing.T) {
				h, dials, logs := subjectGuardHandlers(t)
				row := &providerRow{OrgID: uuid.NewString(), Protocol: "oauth", AutoProvision: true}
				_, _, err := h.oauthProvisionUser(context.Background(), row, providerType,
					oauthClaims{email: "a@example.test", fullName: "A", externalID: subject})
				assertRefusedBeforeAnyStatement(t, err, dials, logs)
			})
		}
	}
}

// A usable subject does reach the database (control: the counter really counts).
func TestAUsableSubjectReachesTheDatabase(t *testing.T) {
	h, dials, _ := subjectGuardHandlers(t)
	row := &providerRow{OrgID: uuid.NewString(), Protocol: "oidc", AutoProvision: true}
	_, _, _ = h.provisionOrGetUser(context.Background(), row, "a@example.test", "A", uuid.New(), "12345", nil)
	if dials.Load() == 0 {
		t.Fatal("a valid subject ran no statement; the counter proves nothing")
	}
}

// The create and lookup paths take the subject as given, because every auth provider value they receive refuses an
// empty subject first. RefuseInvalidProviderID waves "", "local" and the service provider through, so a provider value
// outside this list would need the empty-subject branches back.
func TestEveryAuthProviderTheSignInPathsUseRefusesAnEmptySubject(t *testing.T) {
	providers := []string{"oidc", "saml"}
	for _, protocol := range []string{"oauth_github", "oauth_gitlab", "oauth_google"} {
		providerType, ok := oauthTypeForProtocol(protocol)
		if !ok {
			t.Fatalf("no OAuth type for %s", protocol)
		}
		providers = append(providers, oauthAuthProviderValue(providerType))
	}
	for _, provider := range providers {
		if err := refuseUnusableSubject(provider, ""); !errors.Is(err, identity.ErrInvalidProviderID) {
			t.Errorf("%s accepts an empty subject: %v", provider, err)
		}
	}
}
