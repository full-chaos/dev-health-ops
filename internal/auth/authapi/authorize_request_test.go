package authapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const authorizeValidBody = `{"schema_version":"authorization_request.v1",` +
	`"principal_reference":"prf_EXAMPLE0000000000000001","action":"acr.context.read",` +
	`"resource":{"type":"repository","key":"full-chaos/dev-health-ops","organization_id":"org_EXAMPLE0000000000000001"},` +
	`"context":{"request_id":"req_EXAMPLE0000000000000000001","client_service":"dev-health-web"}}`

func authorizeExamplesDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(contracts.ContractsDir(repoRoot(t)), "examples", "authorization_request")
}

func TestDecodeAuthorizationRequestReachesTheManifestVerdictForEveryExample(t *testing.T) {
	dir := authorizeExamplesDir(t)
	var manifest struct {
		Accept []struct{ File string } `json:"accept"`
		Reject []struct{ File string } `json:"reject"`
	}
	readJSON(t, filepath.Join(dir, "manifest.json"), &manifest)
	if len(manifest.Accept) == 0 || len(manifest.Reject) == 0 {
		t.Fatal("the example manifest lists no accept or no reject file")
	}
	for _, e := range manifest.Accept {
		raw, err := os.ReadFile(filepath.Join(dir, e.File))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeAuthorizationRequest(raw); err != nil {
			t.Errorf("%s: refused an accept example: %v", e.File, err)
		}
	}
	for _, e := range manifest.Reject {
		raw, err := os.ReadFile(filepath.Join(dir, e.File))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeAuthorizationRequest(raw); !errors.Is(err, ErrAuthorizationRequestInvalid) {
			t.Errorf("%s: accepted a reject example (err %v)", e.File, err)
		}
	}
}

func TestDecodeAuthorizationRequestCarriesTheOrganizationOnlyAsTheResourceHint(t *testing.T) {
	q, err := DecodeAuthorizationRequest([]byte(authorizeValidBody))
	if err != nil {
		t.Fatal(err)
	}
	want := AuthorizationQuestion{
		PrincipalReference: "prf_EXAMPLE0000000000000001", Action: "acr.context.read",
		ResourceType: "repository", ResourceKey: "full-chaos/dev-health-ops", OrganizationID: "org_EXAMPLE0000000000000001",
		RequestID: "req_EXAMPLE0000000000000000001", ClientService: "dev-health-web",
	}
	if q != want {
		t.Fatalf("decoded %+v, want %+v", q, want)
	}
}

// authorizeSmuggled are bodies that assert identity or authority the contract
// does not let a caller assert, or that hide a member from a case-sensitive
// reader. Each must be refused.
func authorizeSmuggled() map[string]string {
	add := func(member string) string {
		return strings.Replace(authorizeValidBody, `{"schema_version"`, `{`+member+`,"schema_version"`, 1)
	}
	inResource := func(member string) string {
		return strings.Replace(authorizeValidBody, `"resource":{`, `"resource":{`+member+`,`, 1)
	}
	inContext := func(member string) string {
		return strings.Replace(authorizeValidBody, `"context":{`, `"context":{`+member+`,`, 1)
	}
	return map[string]string{
		"top principal_id":        add(`"principal_id":"prn_AAAAAAAAAAAAAAAAAAAAAQ"`),
		"top principal":           add(`"principal":{"id":"prn_AAAAAAAAAAAAAAAAAAAAAQ"}`),
		"top organization_id":     add(`"organization_id":"org_EXAMPLE0000000000000002"`),
		"top org_id":              add(`"org_id":"org_EXAMPLE0000000000000002"`),
		"top roles":               add(`"roles":["owner"]`),
		"top permissions":         add(`"permissions":["members.manage"]`),
		"top scopes":              add(`"scopes":["api:access"]`),
		"top is_superuser":        add(`"is_superuser":true`),
		"top actor":               add(`"actor":"prn_AAAAAAAAAAAAAAAAAAAAAQ"`),
		"case-variant reference":  add(`"Principal_Reference":"prf_EXAMPLE0000000000000002"`),
		"case-variant action":     add(`"ACTION":"members.manage"`),
		"repeated reference":      add(`"principal_reference":"prf_EXAMPLE0000000000000002"`),
		"repeated action":         add(`"action":"members.manage"`),
		"resource role":           inResource(`"role":"owner"`),
		"resource principal_id":   inResource(`"principal_id":"prn_AAAAAAAAAAAAAAAAAAAAAQ"`),
		"resource repeated org":   inResource(`"organization_id":"org_EXAMPLE0000000000000002"`),
		"resource case org":       inResource(`"Organization_ID":"org_EXAMPLE0000000000000002"`),
		"context principal_id":    inContext(`"principal_id":"prn_AAAAAAAAAAAAAAAAAAAAAQ"`),
		"context organization_id": inContext(`"organization_id":"org_EXAMPLE0000000000000002"`),
		"null organization":       strings.Replace(authorizeValidBody, `"org_EXAMPLE0000000000000001"`, `null`, 1),
		"empty organization":      strings.Replace(authorizeValidBody, `"org_EXAMPLE0000000000000001"`, `""`, 1),
		"numeric organization":    strings.Replace(authorizeValidBody, `"org_EXAMPLE0000000000000001"`, `1`, 1),
		"trailing object":         authorizeValidBody + `{}`,
		"array body":              `[` + authorizeValidBody + `]`,
		"wrong schema_version":    strings.Replace(authorizeValidBody, `authorization_request.v1`, `authorization_request.v2`, 1),
		"unknown client_service":  strings.Replace(authorizeValidBody, `dev-health-web`, `dev-health-other`, 1),
	}
}

func TestDecodeAuthorizationRequestRefusesCallerAssertedIdentity(t *testing.T) {
	for name, body := range authorizeSmuggled() {
		if q, err := DecodeAuthorizationRequest([]byte(body)); !errors.Is(err, ErrAuthorizationRequestInvalid) {
			t.Errorf("%s: decoded %+v (err %v), want a refusal", name, q, err)
		}
	}
}

func TestDecodeAuthorizationRequestRefusalNamesNoValue(t *testing.T) {
	body := strings.Replace(authorizeValidBody, `"prf_EXAMPLE0000000000000001"`, `"secret value with spaces"`, 1)
	_, err := DecodeAuthorizationRequest([]byte(body))
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err = %v, want a refusal that does not echo the value", err)
	}
}

// The decoder never accepts what the schema refuses. Bodies the schema can
// see (no repeated member) get the same verdict from both.
func TestDecodeAuthorizationRequestAgreesWithTheSchema(t *testing.T) {
	root := repoRoot(t)
	bodies := map[string]string{"valid": authorizeValidBody}
	for name, body := range authorizeSmuggled() {
		bodies[name] = body
	}
	for name, edit := range map[string][2]string{
		"bare action":         {`"acr.context.read"`, `"admin"`},
		"upper action":        {`"acr.context.read"`, `"Acr.context.read"`},
		"upper repo key":      {`"full-chaos/dev-health-ops"`, `"Full-Chaos/dev-health-ops"`},
		"star repo key":       {`"full-chaos/dev-health-ops"`, `"*"`},
		"short reference":     {`"prf_EXAMPLE0000000000000001"`, `"prf_short"`},
		"jwt reference":       {`"prf_EXAMPLE0000000000000001"`, `"eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ4In0.c2ln"`},
		"spaced reference":    {`"prf_EXAMPLE0000000000000001"`, `"prf_EXAMPLE 000000000000001"`},
		"bad request id":      {`"req_EXAMPLE0000000000000000001"`, `"req_short"`},
		"platform type":       {`"type":"repository"`, `"type":"platform"`},
		"unknown type":        {`"type":"repository"`, `"type":"widget"`},
		"no organization":     {`,"organization_id":"org_EXAMPLE0000000000000001"`, ``},
		"number reference":    {`"prf_EXAMPLE0000000000000001"`, `12345678901234567890`},
		"non-repo key on org": {`"type":"repository","key":"full-chaos/dev-health-ops"`, `"type":"organization","key":"acme"`},
		"star on org":         {`"type":"repository","key":"full-chaos/dev-health-ops"`, `"type":"organization","key":"*"`},
	} {
		bodies[name] = strings.Replace(authorizeValidBody, edit[0], edit[1], 1)
	}
	for name, body := range bodies {
		_, decodeErr := DecodeAuthorizationRequest([]byte(body))
		var document any
		if json.Unmarshal([]byte(body), &document) != nil {
			if decodeErr == nil {
				t.Errorf("%s: decoder accepted a body encoding/json refuses", name)
			}
			continue
		}
		schemaErr := contracts.Validate(root, "authorization_request.v1", document)
		switch {
		case decodeErr == nil && schemaErr != nil:
			t.Errorf("%s: decoder accepted what the schema refuses: %v", name, schemaErr)
		case decodeErr != nil && schemaErr == nil && !authorizeHasRepeatedMember(body):
			t.Errorf("%s: decoder refused what the schema accepts: %v", name, decodeErr)
		}
	}
}

// authorizeHasRepeatedMember reports a body whose objects repeat a member: the
// schema sees only the last one, the decoder refuses it.
func authorizeHasRepeatedMember(body string) bool {
	_, err := strictMembers([]byte(body))
	if err != nil && strings.Contains(err.Error(), "repeated") {
		return true
	}
	var top map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &top) != nil {
		return false
	}
	for _, name := range []string{"resource", "context"} {
		if _, err := strictMembers(top[name]); err != nil && strings.Contains(err.Error(), "repeated") {
			return true
		}
	}
	return false
}

func TestDecodeAuthorizationBatchRequest(t *testing.T) {
	batch := func(n int, item string) string {
		items := make([]string, n)
		for i := range items {
			items[i] = item
		}
		return `{"schema_version":"authorization_batch_request.v1","requests":[` + strings.Join(items, ",") + `]}`
	}
	got, err := DecodeAuthorizationBatchRequest([]byte(batch(2, authorizeValidBody)))
	if err != nil || len(got) != 2 {
		t.Fatalf("two items: %d, %v; want 2 decoded", len(got), err)
	}
	if got, err := DecodeAuthorizationBatchRequest([]byte(batch(maxAuthorizationBatchItems, authorizeValidBody))); err != nil || len(got) != maxAuthorizationBatchItems {
		t.Fatalf("%d items: %d, %v", maxAuthorizationBatchItems, len(got), err)
	}
	refused := map[string]string{
		"empty":              batch(0, authorizeValidBody),
		"too many":           batch(maxAuthorizationBatchItems+1, authorizeValidBody),
		"wrong version":      strings.Replace(batch(1, authorizeValidBody), "authorization_batch_request.v1", "authorization_request.v1", 1),
		"batch principal_id": strings.Replace(batch(1, authorizeValidBody), `{"schema_version":"authorization_batch_request.v1"`, `{"principal_id":"prn_AAAAAAAAAAAAAAAAAAAAAQ","schema_version":"authorization_batch_request.v1"`, 1),
		"requests object":    `{"schema_version":"authorization_batch_request.v1","requests":{}}`,
		"repeated requests":  `{"schema_version":"authorization_batch_request.v1","requests":[` + authorizeValidBody + `],"requests":[]}`,
	}
	for name, body := range authorizeSmuggled() {
		refused["item "+name] = batch(1, body)
	}
	for name, body := range refused {
		if got, err := DecodeAuthorizationBatchRequest([]byte(body)); !errors.Is(err, ErrAuthorizationRequestInvalid) {
			t.Errorf("%s: decoded %d items (err %v), want a refusal", name, len(got), err)
		}
	}
}

func FuzzDecodeAuthorizationRequest(f *testing.F) {
	f.Add([]byte(authorizeValidBody))
	for _, body := range authorizeSmuggled() {
		f.Add([]byte(body))
	}
	root, rootErr := contracts.RepoRoot(".")
	f.Fuzz(func(t *testing.T, body []byte) {
		q, err := DecodeAuthorizationRequest(body)
		if err != nil {
			if !errors.Is(err, ErrAuthorizationRequestInvalid) {
				t.Fatalf("refusal does not wrap ErrAuthorizationRequestInvalid: %v", err)
			}
			return
		}
		if rootErr != nil {
			t.Fatal(rootErr)
		}
		var document any
		if err := json.Unmarshal(body, &document); err != nil {
			t.Fatalf("decoder accepted a body encoding/json refuses: %v", err)
		}
		if err := contracts.Validate(root, "authorization_request.v1", document); err != nil {
			t.Fatalf("decoder accepted %s, which the schema refuses: %v", fmt.Sprintf("%+v", q), err)
		}
	})
}
