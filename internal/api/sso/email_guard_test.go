package sso

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// unusableEmails are the forms the shared email rule refuses; each carries the marker so a leak is visible.
func unusableEmails() map[string]string {
	return map[string]string{
		"replacement char": subjectMarker + "�@example.test",
		"nul":              subjectMarker + "\x00@example.test",
		"control":          subjectMarker + "\x01@example.test",
		"del":              subjectMarker + "\x7f@example.test",
		"c1":               subjectMarker + "\u0085x@example.test",
		"invalid utf8":     subjectMarker + "\xff@example.test",
		"lone surrogate":   subjectMarker + "\xed\xa0\x80@example.test",
	}
}

func assertEmailRefusedBeforeAnyStatement(t *testing.T, err error, dialCount int64, logs string) {
	t.Helper()
	if !errors.Is(err, errInvalidProviderEmail) {
		t.Fatalf("err = %v, want errInvalidProviderEmail", err)
	}
	if dialCount != 0 {
		t.Fatalf("%d database statements ran before the refusal", dialCount)
	}
	if strings.Contains(err.Error(), subjectMarker) || strings.Contains(logs, subjectMarker) {
		t.Fatal("the email value is in the error or the logs")
	}
}

func TestProvisionOrGetUserRefusesAnUnusableEmailBeforeAnyStatement(t *testing.T) {
	for _, protocol := range []string{"oidc", "saml"} {
		for name, email := range unusableEmails() {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				h, dials, logs := subjectGuardHandlers(t)
				row := &providerRow{OrgID: uuid.NewString(), Protocol: protocol, AutoProvision: true}
				_, _, err := h.provisionOrGetUser(context.Background(), row, email, "A", uuid.New(), "12345", nil)
				assertEmailRefusedBeforeAnyStatement(t, err, dials.Load(), logs.String())
			})
		}
	}
}

// A usable address reaches the database (control), including one whose edge characters the address validator trims.
func TestAUsableEmailReachesTheDatabase(t *testing.T) {
	for name, email := range map[string]string{"plain": "a@example.test", "edge tab": "\ta@example.test", "unicode local part": "jürgen@example.test"} {
		t.Run(name, func(t *testing.T) {
			h, dials, _ := subjectGuardHandlers(t)
			row := &providerRow{OrgID: uuid.NewString(), Protocol: "oidc", AutoProvision: true}
			_, _, _ = h.provisionOrGetUser(context.Background(), row, email, "A", uuid.New(), "12345", nil)
			if dials.Load() == 0 {
				t.Fatal("a usable address ran no statement")
			}
		})
	}
}
