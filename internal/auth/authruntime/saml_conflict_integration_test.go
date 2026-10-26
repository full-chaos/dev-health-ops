//go:build integration

package authruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beevik/etree"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const (
	samlConflictBase      = "https://sp.example.test/api/v1/auth"
	samlConflictIdPEntity = "https://idp.example.test/metadata"
	samlConflictDetail    = "More than one account carries this sign-in. Contact your organization administrator."
)

// samlConflictIdP signs assertions for one provider row with a key made at run time.
type samlConflictIdP struct {
	tlsCert tls.Certificate
	certB64 string
}

func newSAMLConflictIdP(t *testing.T) samlConflictIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test idp"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return samlConflictIdP{tlsCert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, certB64: base64.StdEncoding.EncodeToString(der)}
}

// response is an unsolicited, assertion-signed response for the provider row's SP carrying a persistent NameID.
func (idp samlConflictIdP) response(t *testing.T, p saml.Provider, nameID string) string {
	t.Helper()
	now := time.Now().UTC()
	ts := func(v time.Time) string { return v.Format(time.RFC3339) }
	acs, audience := p.ACSURL(samlConflictBase), p.SPEntityID(samlConflictBase)
	doc := etree.NewDocument()
	r := doc.CreateElement("samlp:Response")
	r.CreateAttr("xmlns:samlp", "urn:oasis:names:tc:SAML:2.0:protocol")
	r.CreateAttr("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion")
	r.CreateAttr("ID", "_r"+uuid.NewString())
	r.CreateAttr("Version", "2.0")
	r.CreateAttr("IssueInstant", ts(now))
	r.CreateAttr("Destination", acs)
	r.CreateElement("saml:Issuer").SetText(samlConflictIdPEntity)
	r.CreateElement("samlp:Status").CreateElement("samlp:StatusCode").CreateAttr("Value", "urn:oasis:names:tc:SAML:2.0:status:Success")
	a := r.CreateElement("saml:Assertion")
	a.CreateAttr("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion")
	a.CreateAttr("ID", "_a"+uuid.NewString())
	a.CreateAttr("Version", "2.0")
	a.CreateAttr("IssueInstant", ts(now))
	a.CreateElement("saml:Issuer").SetText(samlConflictIdPEntity)
	subject := a.CreateElement("saml:Subject")
	name := subject.CreateElement("saml:NameID")
	name.CreateAttr("Format", "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent")
	name.SetText(nameID)
	confirmation := subject.CreateElement("saml:SubjectConfirmation")
	confirmation.CreateAttr("Method", "urn:oasis:names:tc:SAML:2.0:cm:bearer")
	data := confirmation.CreateElement("saml:SubjectConfirmationData")
	data.CreateAttr("NotOnOrAfter", ts(now.Add(5*time.Minute)))
	data.CreateAttr("Recipient", acs)
	conditions := a.CreateElement("saml:Conditions")
	conditions.CreateAttr("NotBefore", ts(now.Add(-time.Minute)))
	conditions.CreateAttr("NotOnOrAfter", ts(now.Add(5*time.Minute)))
	conditions.CreateElement("saml:AudienceRestriction").CreateElement("saml:Audience").SetText(audience)
	authn := a.CreateElement("saml:AuthnStatement")
	authn.CreateAttr("AuthnInstant", ts(now))
	authn.CreateElement("saml:AuthnContext").CreateElement("saml:AuthnContextClassRef").SetText("urn:oasis:names:tc:SAML:2.0:ac:classes:Password")
	attr := a.CreateElement("saml:AttributeStatement").CreateElement("saml:Attribute")
	attr.CreateAttr("Name", "email")
	attr.CreateElement("saml:AttributeValue").SetText(nameID + "@example.com")
	signing := dsig.NewDefaultSigningContext(dsig.TLSCertKeyStore(idp.tlsCert))
	signing.Canonicalizer = dsig.MakeC14N10ExclusiveCanonicalizerWithPrefixList("")
	if err := signing.SetSignatureMethod(dsig.RSASHA256SignatureMethod); err != nil {
		t.Fatal(err)
	}
	signed, err := signing.SignEnveloped(a)
	if err != nil {
		t.Fatal(err)
	}
	r.RemoveChild(a)
	r.AddChild(signed)
	raw, err := doc.WriteToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

type samlConflictSessions struct{}

func (samlConflictSessions) IssueFederatedSession(w http.ResponseWriter, _ *http.Request, _ saml.SignIn) {
	w.WriteHeader(http.StatusOK)
}

func (samlConflictSessions) Preflight(context.Context) error { return nil }

type samlConflictEnv struct {
	admin, runtime *pgxpool.Pool
	schema         authschema.ValidatedIdentifier
	idp            samlConflictIdP
	org            uuid.UUID
	provider       saml.Provider
}

func newSAMLConflictEnv(t *testing.T) *samlConflictEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("saml_conflict", instance)
	if err != nil {
		t.Fatal(err)
	}
	password := testRolePassword(t)
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	e := &samlConflictEnv{admin: admin, runtime: runtime, schema: schema, idp: newSAMLConflictIdP(t), org: uuid.New()}
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier) VALUES ($1, $2, 'saml-conflict', 'enterprise')`,
		e.org, "org-"+e.org.String()); err != nil {
		t.Fatal(err)
	}
	row := uuid.New()
	config := fmt.Sprintf(`{"entity_id": %q, "sso_url": "https://idp.example.test/sso", "certificate": %q}`, samlConflictIdPEntity, e.idp.certB64)
	if _, err := admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, allow_idp_initiated, config)
VALUES ($1, $2, $3, 'saml', 'active', true, $4::json)`, row, e.org, "idp-"+row.String(), config); err != nil {
		t.Fatal(err)
	}
	pg, err := saml.NewPGStore(runtime, schema)
	if err != nil {
		t.Fatal(err)
	}
	if e.provider, err = pg.Provider(ctx, row); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *samlConflictEnv) store(t *testing.T, owned bool) *identity.Store {
	t.Helper()
	store, err := identity.New(identity.Config{Pool: e.runtime, Schema: e.schema, UsersOwnedByAuth: owned})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// acs posts nameID's response to the ACS handler of the SAML service authruntime builds over store.
func (e *samlConflictEnv) acs(t *testing.T, store *identity.Store, nameID string) *httptest.ResponseRecorder {
	t.Helper()
	var cfg authconfig.Config
	cfg.SSO.AppBaseURL = samlConflictBase
	service, err := samlService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), e.runtime, e.schema, store)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]string{"SAMLResponse": e.idp.response(t, e.provider, nameID)})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, saml.ACSPath(e.provider, samlConflictBase), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("provider_id", e.provider.ID.String())
	rec := httptest.NewRecorder()
	saml.Handlers{Service: service, Sessions: samlConflictSessions{}}.ACS(rec, req)
	return rec
}

func (e *samlConflictEnv) member(t *testing.T, user uuid.UUID) {
	t.Helper()
	if _, err := e.admin.Exec(context.Background(), `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`,
		uuid.New(), user, e.org); err != nil {
		t.Fatal(err)
	}
}

func (e *samlConflictEnv) key(t *testing.T, nameID string) identity.ExternalKey {
	t.Helper()
	subject, err := saml.Subject(saml.NameID{Format: "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", Value: nameID})
	if err != nil {
		t.Fatal(err)
	}
	return identity.ExternalKey{Issuer: samlConflictIdPEntity, Provider: identity.Provider(e.provider.ID.String()), Subject: subject}
}

func wantSAMLConflictRefusal(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	var got struct {
		Detail string `json:"detail"`
	}
	if rec.Code != http.StatusConflict || json.Unmarshal(rec.Body.Bytes(), &got) != nil || got.Detail != samlConflictDetail {
		t.Fatalf("ACS = %d %s; want 409 %q", rec.Code, rec.Body.String(), samlConflictDetail)
	}
}

// A SAML account whose principal already holds another subject at the provider row is an identity conflict through
// the production adapter (samlIdentities) and the SAML service authruntime builds: after the users cutover (the
// owned re-link) and before it (the legacy projection). The ACS answer is the refusal, never a 500.
func TestProductionSAMLWiringRefusesAnAccountHoldingAnotherSubject(t *testing.T) {
	e := newSAMLConflictEnv(t)
	ctx := context.Background()

	t.Run("owned re-link", func(t *testing.T) {
		owned := e.store(t, true)
		held := "held-" + uuid.NewString()
		principal, err := owned.ProvisionFederated(ctx, identity.ProvisionRequest{Key: e.key(t, held), Email: held + "@example.com",
			Actor: identity.SelfSignUp(nil), LegacyProvider: "saml", LegacySubject: held})
		if err != nil {
			t.Fatalf("provision the account with its held subject: %v", err)
		}
		second := "second-" + uuid.NewString()
		if _, err := e.admin.Exec(ctx, `UPDATE public.users SET auth_provider_id = $2, sso_provider_id = $3 WHERE id = $1`,
			principal, second, e.provider.ID); err != nil {
			t.Fatal(err)
		}
		e.member(t, principal)

		org := e.org
		_, found, err := samlIdentities{owned}.IdentityBySubject(ctx,
			saml.Identity{Issuer: samlConflictIdPEntity, Provider: e.provider.ID.String(), Subject: e.key(t, second).Subject},
			&saml.LegacyMatch{AuthProvider: "saml", Subject: second, MembershipOrg: org})
		if found || !errors.Is(err, saml.ErrIdentityAmbiguous) {
			t.Fatalf("samlIdentities.IdentityBySubject = %v, %v; want saml.ErrIdentityAmbiguous", found, err)
		}
		wantSAMLConflictRefusal(t, e.acs(t, owned, second))
	})

	t.Run("switch off: legacy projection", func(t *testing.T) {
		legacy := e.store(t, false)
		first := "first-" + uuid.NewString()
		user := uuid.New()
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.users (id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified)
VALUES ($1, $2, 'saml', $3, $4, true, true)`, user, first+"@example.com", first, e.provider.ID); err != nil {
			t.Fatal(err)
		}
		e.member(t, user)
		if rec := e.acs(t, legacy, first); rec.Code != http.StatusOK {
			t.Fatalf("first sign-in = %d %s; want 200 (projected and linked)", rec.Code, rec.Body.String())
		}
		second := "second-" + uuid.NewString()
		if _, err := e.admin.Exec(ctx, `UPDATE public.users SET auth_provider_id = $2 WHERE id = $1`, user, second); err != nil {
			t.Fatal(err)
		}
		wantSAMLConflictRefusal(t, e.acs(t, legacy, second))
	})
}
