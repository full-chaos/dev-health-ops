//go:build integration

package sso_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

const (
	persistentFormat = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"
	nameIDSP         = "https://sp.test/saml/metadata"
)

func samlSignInWithNameID(t *testing.T, st stack, idp *samlIdP, providerID uuid.UUID, email, nameID, format, qualifier string) int {
	t.Helper()
	response := idp.signedSAMLResponse(t, samlResponseOpts{
		spEntityID: nameIDSP, acsURL: samlACSOverrideFor(providerID), email: email, fullName: "Person",
		nameID: nameID, nameIDFormat: format, spNameQualifier: qualifier,
	})
	status, _ := postJSON(t, st.server, "/api/v1/auth/saml/"+providerID.String()+"/acs", map[string]any{"SAMLResponse": response})
	return status
}

type storedParts struct {
	value           string
	format, spnq    *string
	provider        string
	ssoProviderBind bool
}

func readStoredParts(t *testing.T, ctx context.Context, st stack, email string) storedParts {
	t.Helper()
	var p storedParts
	if err := st.pool.QueryRow(ctx, `SELECT auth_provider_id, saml_nameid_format, saml_sp_name_qualifier, auth_provider, sso_provider_id IS NOT NULL
FROM users WHERE email = $1`, email).Scan(&p.value, &p.format, &p.spnq, &p.provider, &p.ssoProviderBind); err != nil {
		t.Fatalf("read stored parts: %v", err)
	}
	return p
}

// A new account and an existing bound account both get the NameID format and SPNameQualifier at sign-in; the
// identity columns stay as they were.
func TestSAMLSignInStoresTheNameIDParts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, true)

	const jit = "new.person@allowed.example"
	if status := samlSignInWithNameID(t, st, idp, providerID, jit, "name-new", persistentFormat, "sp-q"); status != http.StatusOK {
		t.Fatalf("sign-up sign-in: %d", status)
	}
	got := readStoredParts(t, ctx, st, jit)
	if got.value != "name-new" || got.format == nil || *got.format != persistentFormat || got.spnq == nil || *got.spnq != "sp-q" {
		t.Fatalf("new account parts = %+v", got)
	}

	const bound = "bound.person@allowed.example"
	seedBound(t, ctx, st.pool, providerID, "saml", bound, "name-bound")
	if before := readStoredParts(t, ctx, st, bound); before.format != nil || before.spnq != nil {
		t.Fatalf("seeded row must start with NULL parts: %+v", before)
	}
	if status := samlSignInWithNameID(t, st, idp, providerID, bound, "name-bound", persistentFormat, ""); status != http.StatusOK {
		t.Fatalf("existing sign-in: %d", status)
	}
	got = readStoredParts(t, ctx, st, bound)
	if got.value != "name-bound" || got.provider != "saml" || !got.ssoProviderBind ||
		got.format == nil || *got.format != persistentFormat || got.spnq == nil || *got.spnq != nameIDSP {
		t.Fatalf("existing account parts = %+v (the assertion maker sets the SPNameQualifier to the SP entity id)", got)
	}
}

// Seeded set: every bound SAML user who signed in once after the fix can be keyed with the runtime rule; one who
// never signed in stays unkeyed (NULL parts), and a transient NameID stays refused.
func TestSAMLSignInMakesBoundUsersKeyableAndNeverSignedInStayUnkeyed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, false)

	signed := map[string]string{"a.person@allowed.example": "name-a", "b.person@allowed.example": "name-b"}
	for email, nameID := range signed {
		seedBound(t, ctx, st.pool, providerID, "saml", email, nameID)
		if status := samlSignInWithNameID(t, st, idp, providerID, email, nameID, persistentFormat, "sp-q"); status != http.StatusOK {
			t.Fatalf("sign-in %s: %d", email, status)
		}
		reactivate(t, ctx, st, providerID)
	}
	const never = "never.person@allowed.example"
	seedBound(t, ctx, st.pool, providerID, "saml", never, "name-never")

	keyable, unkeyed := 0, 0
	rows, err := st.pool.Query(ctx, `SELECT auth_provider_id, saml_nameid_format, saml_sp_name_qualifier, email FROM users WHERE auth_provider = 'saml'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var value, email string
		var format, spnq *string
		if err := rows.Scan(&value, &format, &spnq, &email); err != nil {
			t.Fatal(err)
		}
		_, err := saml.SubjectFromStored(value, format, spnq)
		switch {
		case err == nil:
			keyable++
		case errors.Is(err, saml.ErrNameIDNotCaptured) && email == never:
			unkeyed++
		default:
			t.Fatalf("%s: unexpected %v", email, err)
		}
	}
	if keyable != len(signed) || unkeyed != 1 {
		t.Fatalf("keyable=%d unkeyed=%d, want %d and 1", keyable, unkeyed, len(signed))
	}
}

// A transient NameID is stored as asserted and stays refused by the shared rule.
func TestSAMLSignInTransientNameIDStaysRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := startStack(t, ctx)
	idp := newSAMLIdP(t)
	providerID := seedSAMLOrgProviderAuto(t, ctx, st, idp, nameIDSP, false)
	const email = "transient.person@allowed.example"
	seedBound(t, ctx, st.pool, providerID, "saml", email, "name-t")
	if status := samlSignInWithNameID(t, st, idp, providerID, email, "name-t", saml.NameIDFormatTransient, ""); status != http.StatusOK {
		t.Fatalf("sign-in: %d", status)
	}
	got := readStoredParts(t, ctx, st, email)
	if _, err := saml.SubjectFromStored(got.value, got.format, got.spnq); !errors.Is(err, saml.ErrSubject) {
		t.Fatalf("transient: %v, want ErrSubject", err)
	}
}
