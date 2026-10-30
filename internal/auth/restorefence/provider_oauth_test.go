package restorefence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const oauthSection = "public.provider_oauth_credentials"

// oauthHeld pairs a restored provider credential with the source's: the same key, the same version and the same binding.
const oauthHeldWant = "x.org_id = t.org_id AND x.provider = t.provider AND x.credential_name = t.credential_name AND x.version = t.version AND x.binding_id IS NOT DISTINCT FROM t.binding_id"

// A provider token rotated (refresh, reconnect) or removed (disconnect) after T comes back with the restore: the old token
// may be live at the provider. The restored row is deleted unless the source holds the same version and binding, and
// named for the operator (requeue_provider_revocation). Only with a manifest: a blind margin deletes nothing.
func TestARotatedOrRemovedProviderCredentialIsDeletedAndNamed(t *testing.T) {
	def := mustSection(t, oauthSection)
	if def.kind != kindPresence || !def.legacy || def.key.name != "org_id" || !def.manifestOnly {
		t.Errorf("want a manifest-only legacy presence section keyed by org_id; got kind %v legacy %v key %s manifestOnly %v", def.kind, def.legacy, def.key.name, def.manifestOnly)
	}
	sql := def.applySQL(testSchema(t))
	if !strings.Contains(sql, "DELETE FROM public.provider_oauth_credentials t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE "+oauthHeldWant+")") {
		t.Errorf("the delete must keep only a row the source holds with the same version and binding: %s", sql)
	}
	if def.action != ActionRequeueProviderRevocation {
		t.Errorf("action %q, want requeue_provider_revocation", def.action)
	}
	ids := def.idsSQL(testSchema(t))
	if !strings.Contains(ids, "NOT EXISTS (SELECT 1 FROM m x WHERE "+oauthHeldWant+")") || !strings.Contains(ids, oauthIDWant) {
		t.Errorf("the action must name exactly the deleted rows, by the encoded composite id: %s", ids)
	}
	if sectionIndex("public.provider_oauth_revocations") > sectionIndex(oauthSection) {
		t.Error("the credential section runs after the revocation report of the same provider")
	}
}

// C1: the manifest carries exactly the key, the version and the binding id of a provider credential: never the token or
// the granted scopes. binding_id is a server-made id (pagerduty_callback.go: uuid hex), stored next to the token, not a
// credential.
func TestTheProviderCredentialManifestCarriesNoSecret(t *testing.T) {
	def := mustSection(t, oauthSection)
	if got := strings.Join(def.columnNames(), ","); got != "org_id,provider,credential_name,version,binding_id" {
		t.Errorf("the section exports %s, want exactly org_id,provider,credential_name,version,binding_id", got)
	}
	exp := def.exportSQL(testSchema(t))
	for _, bad := range []string{"token_encrypted", "granted_scopes", "has_refresh_token", "account_id"} {
		if strings.Contains(exp, bad) {
			t.Errorf("the export names %s: %s", bad, exp)
		}
	}
	ddl := oauthDDL(t)
	for _, want := range []string{"org_id text NOT NULL", "provider text NOT NULL", "credential_name text NOT NULL", "token_encrypted text NOT NULL", "version integer DEFAULT 1 NOT NULL", "binding_id text"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("the provider_oauth_credentials DDL no longer declares %q: re-check the fence section", want)
		}
	}
}

func oauthDDL(t *testing.T) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal", "pgmigrate", "baseline", "head.json"))
	if err != nil {
		t.Fatal(err)
	}
	var head struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?s)CREATE TABLE public\.provider_oauth_credentials \((.*?)\n\);`).FindStringSubmatch(head.Schema)
	if m == nil {
		t.Fatal("no CREATE TABLE public.provider_oauth_credentials in the baseline")
	}
	return m[1] + "\n" + strings.Join(regexp.MustCompile(`(?m)^.*provider_oauth_credentials.*CHECK.*$`).FindAllString(head.Schema, -1), "\n")
}

// oauthIDWant is the operator id of one provider credential: each part percent-encoded (%, /, comma, CR, LF), joined by
// "/". No CHECK limits the three text columns, so a raw "/" or "," could make two credentials print the same id, or
// split one id in the comma-joined action line.
const oauthIDWant = `replace(replace(replace(replace(replace(t.org_id, '%', '%25'), '/', '%2F'), ',', '%2C'), chr(10), '%0A'), chr(13), '%0D') || '/' || ` +
	`replace(replace(replace(replace(replace(t.provider, '%', '%25'), '/', '%2F'), ',', '%2C'), chr(10), '%0A'), chr(13), '%0D') || '/' || ` +
	`replace(replace(replace(replace(replace(t.credential_name, '%', '%25'), '/', '%2F'), ',', '%2C'), chr(10), '%0A'), chr(13), '%0D')`

// C2: the three key columns are plain text with no CHECK, so they can hold "/" and ","; the id encodes them.
func TestTheProviderCredentialIdIsUnambiguous(t *testing.T) {
	if strings.Contains(oauthDDL(t), "CHECK") {
		t.Log("a CHECK now constrains provider_oauth_credentials; the encoding stays (it is safe either way)")
	}
	encode := func(v string) string {
		return strings.NewReplacer("%", "%25", "/", "%2F", ",", "%2C", "\n", "%0A", "\r", "%0D").Replace(v)
	}
	id := func(org, provider, name string) string {
		return encode(org) + "/" + encode(provider) + "/" + encode(name)
	}
	seen := map[string][3]string{}
	for _, k := range [][3]string{{"a/b", "c", "d"}, {"a", "b/c", "d"}, {"a", "b", "c/d"}, {"a,b", "c", "d"}, {"a%2Fb", "c", "d"}, {"a", "b", "c\nd"}} {
		got := id(k[0], k[1], k[2])
		if prev, dup := seen[got]; dup {
			t.Errorf("%v and %v print the same id %q", prev, k, got)
		}
		if strings.Contains(got, ",") || strings.Count(got, "/") != 2 {
			t.Errorf("%v prints %q: a comma or an extra slash makes the action line ambiguous", k, got)
		}
		seen[got] = k
	}
}

// Q4: several provider credentials share the key org_id, so Canonical breaks a tie by the whole row: the digest does not
// depend on the row order. Sections with unique keys keep the digest the f762bf3e binary computed (C3).
func TestTheCanonicalDigestBreaksKeyTiesAndKeepsOldDigests(t *testing.T) {
	old := Manifest{Version: 2, SourceTimeline: 2, Floors: []Floor{{"organization", "org_b", 7}, {"organization", "org_a", 3}}}
	old.Sections = []Section{
		{Name: "auth.sessions", Rows: []Row{
			{"id": "00000000-0000-4000-8000-000000000009", "revoked_at": "2026-10-29T00:00:02+00:00", "revoked_reason": "logout"},
			{"id": "00000000-0000-4000-8000-000000000001", "revoked_at": "2026-10-29T00:00:01+00:00", "revoked_reason": "admin"}}},
		{Name: "public.users", Rows: []Row{
			{"id": "00000000-0000-4000-8000-000000000005", "is_active": false, "token_version": json.Number("4")},
			{"id": "00000000-0000-4000-8000-000000000003", "is_active": true, "token_version": json.Number("2")}}},
	}
	// Computed by Manifest.Digest at f762bf3e (logs/11891-canonical-pin-f762bf3e.log).
	if d, err := old.Digest(); err != nil || d != "7fe32ae9b924295ecdac0171d40897ad3deb8f1bf41a9e2dd90c2d92565a42d4" {
		t.Errorf("the digest of a single-key manifest changed: %s, %v", d, err)
	}
	a := Row{"org_id": "org-1", "provider": "pagerduty", "credential_name": "a", "version": json.Number("2"), "binding_id": "b1"}
	b := Row{"org_id": "org-1", "provider": "pagerduty", "credential_name": "b", "version": json.Number("1"), "binding_id": "b2"}
	one := Manifest{Version: ManifestVersion, SourceTimeline: 2, Floors: []Floor{}, Sections: []Section{{Name: oauthSection, Rows: []Row{a, b}}}}
	two := Manifest{Version: ManifestVersion, SourceTimeline: 2, Floors: []Floor{}, Sections: []Section{{Name: oauthSection, Rows: []Row{b, a}}}}
	d1, err1 := one.Digest()
	d2, err2 := two.Digest()
	if err1 != nil || err2 != nil || d1 != d2 {
		t.Errorf("two rows with one org_id give digests %s and %s by order (%v %v)", d1, d2, err1, err2)
	}
}

// C3: the sections added after version 2 (the provider section, then the password sections) are the only changes of the
// section set: every other section exports exactly what the version 2 manifest held.
func TestEveryOtherSectionExportsWhatVersionTwoHeld(t *testing.T) {
	if got := sectionSetDigest(t, oauthSection, authPasswordSection, legacyPasswordSection, identitySection); got != pinnedSectionSet[2] {
		t.Errorf("a section other than %s changed its export (digest %s, version 2 pinned %s)", oauthSection, got, pinnedSectionSet[2])
	}
}
