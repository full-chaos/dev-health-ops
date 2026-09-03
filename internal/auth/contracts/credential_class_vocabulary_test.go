package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// waveOnePlatformCredentialClasses are the credential classes the Auth
// Control Plane itself will mint, which therefore CANNOT appear in the Wave 0
// freeze -- that document inventories what exists today, and these do not
// exist yet. Each is named by TRD section 11.
//
// This list is the ONLY sanctioned way for principal.v1's credential-class
// enum to contain a value that credential-classes.json does not. It is
// deliberately explicit and deliberately short: the ACP-ADR-12 discussion of
// the endpoint-profile gate makes the point that "an escape hatch must be as
// specific as the thing it excuses, or it becomes an exemption". A wildcard,
// a prefix rule, or "anything ending in _token" would be an exemption.
//
// Note what is NOT here. Delegation reuses the frozen `impersonation_session`
// rather than inventing a parallel class, because the frozen vocabulary
// already has that concept and a second name for one thing is how a closed
// vocabulary stops being closed.
var waveOnePlatformCredentialClasses = map[string]string{
	"user_access_token":  "TRD section 11, User access token: EdDSA JWS replacing ops_access_token_hs256.",
	"refresh_credential": "TRD section 11, Refresh credential: opaque, hashed, single-use rotating, family-linked; replaces ops_refresh_token.",
	"workload_token":     "TRD section 11, Workload token: EdDSA JWS, exact audience, obtained by federation or bootstrap exchange.",
}

func frozenCredentialClassIDs(t *testing.T) map[string]bool {
	t.Helper()
	path := filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var document struct {
		Classes []struct {
			ClassID string `json:"class_id"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if len(document.Classes) == 0 {
		// Without this the set comparison below would "pass" against an empty
		// frozen set by reporting every enum value as an undocumented
		// addition -- a loud failure, but for the wrong reason. Fail here
		// with the real cause instead.
		t.Fatalf("%s declares no classes; the vocabulary check has nothing to compare against", path)
	}
	ids := map[string]bool{}
	for _, class := range document.Classes {
		ids[class.ClassID] = true
	}
	return ids
}

func principalCredentialClassEnum(t *testing.T) []string {
	t.Helper()
	path := SchemaPath(testRoot(t), PrincipalSurface)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var document struct {
		Properties struct {
			Credential struct {
				Properties struct {
					Class struct {
						Enum []string `json:"enum"`
					} `json:"class"`
				} `json:"properties"`
			} `json:"credential"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	enum := document.Properties.Credential.Properties.Class.Enum
	if len(enum) == 0 {
		t.Fatalf("principal.v1 declares no credential class enum -- either the schema lost it "+
			"or this test's path into the document is stale (%s)", path)
	}
	return enum
}

// TestTheCredentialClassEnumEqualsTheFrozenSetPlusDocumentedAdditions is the
// guard that keeps principal.v1's enum from becoming a SECOND source of truth
// for the credential vocabulary.
//
// JSON Schema cannot $ref an enum out of a data file, so the enum has to be
// written out in the schema. That makes it exactly the shape this lane's own
// review prompt names as an attack target: "an enum hardcoded instead of read
// from the file that owns it". The schema DECLARES; this test proves the
// declaration equals credential-classes.json plus the documented additions,
// in BOTH directions -- so a class added to the frozen file and forgotten
// here fails, and a class smuggled into the enum alone fails too.
func TestTheCredentialClassEnumEqualsTheFrozenSetPlusDocumentedAdditions(t *testing.T) {
	frozen := frozenCredentialClassIDs(t)
	enum := principalCredentialClassEnum(t)

	inEnum := map[string]bool{}
	var undocumented []string
	for _, class := range enum {
		if inEnum[class] {
			t.Errorf("credential class %q appears twice in the enum", class)
		}
		inEnum[class] = true
		if frozen[class] {
			continue
		}
		if _, ok := waveOnePlatformCredentialClasses[class]; ok {
			continue
		}
		undocumented = append(undocumented, class)
	}
	sort.Strings(undocumented)
	if len(undocumented) > 0 {
		t.Errorf("credential classes in principal.v1's enum that are neither in the Wave 0 "+
			"frozen set nor in waveOnePlatformCredentialClasses: %v. Add the class to "+
			"contracts/auth/v1/credential-classes.json if it exists today, or to the Wave 1 "+
			"list with its TRD citation if the control plane will mint it. Do not widen the "+
			"enum alone -- that is how a closed vocabulary stops being closed.", undocumented)
	}

	var missing []string
	for class := range frozen {
		if !inEnum[class] {
			missing = append(missing, class)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("credential classes frozen in credential-classes.json but absent from "+
			"principal.v1's enum: %v. A principal authenticated by one of these could not be "+
			"represented, so the omission is a gap in the wire contract, not a tightening.",
			missing)
	}

	for class := range waveOnePlatformCredentialClasses {
		if frozen[class] {
			t.Errorf("credential class %q is listed as a Wave 1 addition but ALREADY exists in "+
				"the Wave 0 frozen set. Remove it from waveOnePlatformCredentialClasses: an "+
				"escape hatch that covers something already covered hides the next real "+
				"addition behind a stale entry.", class)
		}
		if !inEnum[class] {
			t.Errorf("credential class %q is documented as a Wave 1 addition but is not in the "+
				"enum, so the documentation describes a class the contract cannot express", class)
		}
	}
}

// TestEveryWaveOneAdditionCarriesAReason keeps the escape hatch honest.
//
// A list of bare strings would drift into an allowlist nobody can audit. Each
// entry must say which TRD section defines the class, so a reviewer can check
// the claim rather than trust the name.
func TestEveryWaveOneAdditionCarriesAReason(t *testing.T) {
	if len(waveOnePlatformCredentialClasses) == 0 {
		t.Skip("no Wave 1 additions declared")
	}
	for class, reason := range waveOnePlatformCredentialClasses {
		if len(reason) < 20 {
			t.Errorf("Wave 1 credential class %q has no substantive reason (%q). Cite the TRD "+
				"section that defines it.", class, reason)
		}
	}
}
