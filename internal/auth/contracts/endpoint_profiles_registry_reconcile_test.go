package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// registryAnchors is, per credential class, the issuer and consumer anchors of
// credential-classes.json, keyed "path:line" (ops repo only).
type registryAnchors struct {
	issuers   map[string]map[string]bool
	consumers map[string]map[string]bool
}

type classRegistryAnchor struct {
	Repo string `json:"repo"`
	Path string `json:"path"`
	Line int    `json:"line"`
}

type classRegistryDocument struct {
	Classes []struct {
		ClassID string `json:"class_id"`
		Issuer  struct {
			Anchors []*classRegistryAnchor `json:"anchors"`
		} `json:"issuer"`
		Consumers []struct {
			Anchor *classRegistryAnchor `json:"anchor"`
		} `json:"consumers"`
	} `json:"classes"`
}

func anchorKey(path string, line int) string { return fmt.Sprintf("%s:%d", path, line) }

func indexRegistry(document classRegistryDocument) registryAnchors {
	out := registryAnchors{issuers: map[string]map[string]bool{}, consumers: map[string]map[string]bool{}}
	for _, class := range document.Classes {
		out.issuers[class.ClassID] = map[string]bool{}
		out.consumers[class.ClassID] = map[string]bool{}
		for _, anchor := range class.Issuer.Anchors {
			if anchor != nil && anchor.Repo == "ops" {
				out.issuers[class.ClassID][anchorKey(anchor.Path, anchor.Line)] = true
			}
		}
		for _, consumer := range class.Consumers {
			if consumer.Anchor != nil && consumer.Anchor.Repo == "ops" {
				out.consumers[class.ClassID][anchorKey(consumer.Anchor.Path, consumer.Anchor.Line)] = true
			}
		}
	}
	return out
}

// authHTTPLayer is the package whose issuer anchors and route registrations
// are auth-service's served surface: an issuer or consumer anchor there must
// be a profile's issued credential or accepted class.
const authHTTPLayer = "internal/auth/authapi/"

// reconcileProfilesWithRegistry checks both directions: every class a row
// accepts has a registry consumer anchored at the row's source line, and every
// class a row issues has a registry issuer anchored at the issued credential's
// line; and every registry consumer or issuer anchored in the auth-service
// HTTP layer is a row's accepted or issued class at that line.
func reconcileProfilesWithRegistry(rows []map[string]any, registry registryAnchors) []string {
	var problems []string
	accepted := map[string]bool{}
	issued := map[string]bool{}
	for _, row := range rows {
		id, _ := row["id"].(string)
		source, _ := row["source"].(map[string]any)
		file, _ := source["file"].(string)
		line, _ := source["line"].(float64)
		classes, _ := row["accepted_credential_classes"].([]any)
		for _, item := range classes {
			class, _ := item.(string)
			key := anchorKey(file, int(line))
			accepted[class+"@"+key] = true
			if !registry.consumers[class][key] {
				problems = append(problems, fmt.Sprintf("row %q accepts %s but credential-classes.json has no %s consumer anchored at %s", id, class, class, key))
			}
		}
		credentials, _ := row["issued_credential"].([]any)
		for _, item := range credentials {
			credential, _ := item.(map[string]any)
			class, _ := credential["class_id"].(string)
			anchor, _ := credential["anchor"].(map[string]any)
			path, _ := anchor["path"].(string)
			at, _ := anchor["line"].(float64)
			key := anchorKey(path, int(at))
			issued[class+"@"+key] = true
			if !registry.issuers[class][key] {
				problems = append(problems, fmt.Sprintf("row %q issues %s but credential-classes.json has no %s issuer anchored at %s", id, class, class, key))
			}
		}
	}
	for class, anchors := range registry.consumers {
		for key := range anchors {
			if strings.HasPrefix(key, authHTTPLayer) && !accepted[class+"@"+key] {
				problems = append(problems, fmt.Sprintf("credential-classes.json has a %s consumer anchored at %s, which no auth-service profile row accepts there", class, key))
			}
		}
	}
	for class, anchors := range registry.issuers {
		for key := range anchors {
			if strings.HasPrefix(key, authHTTPLayer) && !issued[class+"@"+key] {
				problems = append(problems, fmt.Sprintf("credential-classes.json has a %s issuer anchored at %s, which no auth-service profile row issues there", class, key))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func loadClassRegistryDocument(t *testing.T) classRegistryDocument {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document classRegistryDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func authServiceRows(t *testing.T) []map[string]any {
	t.Helper()
	var rows []map[string]any
	for _, item := range loadAuthServiceDocument(t)["rows"].([]any) {
		rows = append(rows, item.(map[string]any))
	}
	return rows
}

// TestAuthServiceProfilesReconcileWithTheCredentialRegistry: what each
// auth-service row accepts and issues is listed in credential-classes.json at
// the same file:line, and the registry lists no auth-service HTTP-layer
// producer or consumer that no row profiles.
func TestAuthServiceProfilesReconcileWithTheCredentialRegistry(t *testing.T) {
	problems := reconcileProfilesWithRegistry(authServiceRows(t), indexRegistry(loadClassRegistryDocument(t)))
	for _, problem := range problems {
		t.Error(problem)
	}
}

// TestTheReconcileGateRefusesPlantedMismatches: each kind of mismatch, planted
// in a copy of the real inventory and registry, is reported.
func TestTheReconcileGateRefusesPlantedMismatches(t *testing.T) {
	fresh := func() ([]map[string]any, registryAnchors) {
		raw, err := json.Marshal(authServiceRows(t))
		if err != nil {
			t.Fatal(err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(raw, &rows); err != nil {
			t.Fatal(err)
		}
		return rows, indexRegistry(loadClassRegistryDocument(t))
	}
	if rows, registry := fresh(); len(reconcileProfilesWithRegistry(rows, registry)) != 0 {
		t.Skip("the real inventory does not reconcile; TestAuthServiceProfilesReconcileWithTheCredentialRegistry reports why")
	}
	refreshRow := func(rows []map[string]any) map[string]any {
		for _, row := range rows {
			if row["route"] == "/v1/token/refresh" {
				return row
			}
		}
		t.Fatal("no refresh row")
		return nil
	}
	cases := map[string]func(rows []map[string]any, registry registryAnchors){
		"a row accepts a class the registry does not anchor at its source": func(rows []map[string]any, _ registryAnchors) {
			row := refreshRow(rows)
			row["accepted_credential_classes"] = append(row["accepted_credential_classes"].([]any), "ops_access_token_hs256")
		},
		"an issued credential's anchor moved": func(rows []map[string]any, _ registryAnchors) {
			credential := refreshRow(rows)["issued_credential"].([]any)[0].(map[string]any)
			anchor := credential["anchor"].(map[string]any)
			anchor["line"] = anchor["line"].(float64) + 1
		},
		"the registry loses the auth-service issuer of the access token": func(rows []map[string]any, registry registryAnchors) {
			for key := range registry.issuers["ops_access_token_hs256"] {
				if strings.HasPrefix(key, authHTTPLayer) {
					delete(registry.issuers["ops_access_token_hs256"], key)
				}
			}
		},
		"the registry names an auth-service consumer no row accepts": func(_ []map[string]any, registry registryAnchors) {
			registry.consumers["refresh_credential"][anchorKey(authHTTPLayer+"routes.go", 70)] = true
		},
		"the registry names an auth-service issuer no row issues": func(_ []map[string]any, registry registryAnchors) {
			registry.issuers["refresh_credential"][anchorKey(authHTTPLayer+"issuer.go", 1)] = true
		},
		"the issue row names a class the registry does not define": func(rows []map[string]any, _ registryAnchors) {
			for _, row := range rows {
				if row["route"] == "/internal/v1/sessions" {
					row["accepted_credential_classes"] = []any{"internal_svc_authsess_tokens"}
				}
			}
		},
		"the registry loses the issue route's service credential consumer": func(_ []map[string]any, registry registryAnchors) {
			delete(registry.consumers["internal_svc_authsess_token"], anchorKey(authHTTPLayer+"compat.go", 133))
		},
	}
	for name, plant := range cases {
		rows, registry := fresh()
		plant(rows, registry)
		if len(reconcileProfilesWithRegistry(rows, registry)) == 0 {
			t.Errorf("%s: the gate reported nothing", name)
		}
	}
}
