package ingressplanes

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// repoFile is the absolute path of a file named relative to the module root.
func repoFile(t *testing.T, relative string) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	return filepath.Join(root, filepath.FromSlash(relative))
}

// checkedInContract is the contract of the repository. A contract that does
// not load fails the test that asked for it.
func checkedInContract(t *testing.T) Contract {
	t.Helper()
	contract, err := Load(repoFile(t, ContractPath))
	if err != nil {
		t.Fatalf("the checked-in contract does not load: %v", err)
	}
	return contract
}

func rule(path, pathType, plane string) Rule {
	return Rule{Path: path, PathType: pathType, Plane: plane}
}

func anchored(path, plane string) Rule { return rule(path, PathTypeImplementationSpecific, plane) }

// validContract is the smallest table with every shape the contract accepts.
func validContract() Contract {
	return Contract{SchemaVersion: SchemaVersion, RegexMode: true, Rules: []Rule{
		rule("/", PathTypePrefix, PlaneGoAPI),
		anchored("/graphql$", PlaneQueryAPI),
		anchored("/api/v1/people/[^/]+/metric$", PlaneQueryAPI),
		anchored("/api/v1/auth/login$", PlaneGoAPI),
		anchored(`/a\.b$`, PlaneGoAPI),
	}}
}

// TestCheckedInContractIsTheShapeProdRoutes pins the facts of the first
// content that the README states: one default rule to the Go api, /graphql on
// query-api, every other rule anchored, both planes present.
func TestCheckedInContractIsTheShapeProdRoutes(t *testing.T) {
	contract := checkedInContract(t)
	if contract.DefaultPlane() != PlaneGoAPI {
		t.Errorf("the default plane is %q, want %s (prod: \"/\" is the Go api)", contract.DefaultPlane(), PlaneGoAPI)
	}
	planes := map[string]int{}
	graphql := ""
	for _, r := range contract.Rules {
		planes[r.Plane]++
		if r.Path == "/graphql$" {
			graphql = r.Plane
		}
	}
	if graphql != PlaneQueryAPI {
		t.Errorf("/graphql$ is on %q, want %s", graphql, PlaneQueryAPI)
	}
	if planes[PlaneGoAPI] < 2 || planes[PlaneQueryAPI] < 2 {
		t.Errorf("want rules on both planes, got %v", planes)
	}
}

// TestValidateRefusesWhatItCannotRoute: one case per refusal. Each case starts
// from a valid table, so only the planted defect can be what is refused.
func TestValidateRefusesWhatItCannotRoute(t *testing.T) {
	if err := validContract().Validate(); err != nil {
		t.Fatalf("the base table must be valid: %v", err)
	}
	for name, c := range map[string]struct {
		change func(*Contract)
		want   string
	}{
		"other schema version": {func(c *Contract) { c.SchemaVersion = 2 }, "schema_version is 2"},
		"no regex mode":        {func(c *Contract) { c.RegexMode = false }, "regex_mode is false"},
		"no default rule":      {func(c *Contract) { c.Rules = c.Rules[1:] }, "0 default rules"},
		"default not a prefix": {func(c *Contract) { c.Rules[0].PathType = PathTypeExact }, "the default rule is path"},
		"unknown plane":        {func(c *Contract) { c.Rules[1].Plane = "web" }, `plane "web" is not one of`},
		"empty plane":          {func(c *Contract) { c.Rules[1].Plane = "" }, `plane "" is not one of`},
		"exact rule": {func(c *Contract) { c.Rules = append(c.Rules, rule("/docs", PathTypeExact, PlaneGoAPI)) },
			"no end anchor"},
		"prefix rule": {func(c *Contract) { c.Rules = append(c.Rules, rule("/docs", PathTypePrefix, PlaneGoAPI)) },
			"no end anchor"},
		"unknown path type":  {func(c *Contract) { c.Rules[1].PathType = "Regex" }, "path_type is not"},
		"no end anchor":      {func(c *Contract) { c.Rules[1].Path = "/graphql" }, "not an anchored path"},
		"two anchors":        {func(c *Contract) { c.Rules[1].Path = "/graphql$$" }, "not an anchored path"},
		"start anchor":       {func(c *Contract) { c.Rules[1].Path = "^/graphql$" }, "not an anchored path"},
		"unescaped dot":      {func(c *Contract) { c.Rules[1].Path = "/a.b$" }, "not an anchored path"},
		"any-text regex":     {func(c *Contract) { c.Rules[1].Path = "/graphql.*$" }, "not an anchored path"},
		"group":              {func(c *Contract) { c.Rules[1].Path = "/(graphql)$" }, "not an anchored path"},
		"alternation":        {func(c *Contract) { c.Rules[1].Path = "/graphql|x$" }, "not an anchored path"},
		"token in a segment": {func(c *Contract) { c.Rules[1].Path = "/x-[^/]+$" }, "not an anchored path"},
		"trailing slash":     {func(c *Contract) { c.Rules[1].Path = "/graphql/$" }, "not an anchored path"},
		"empty segment":      {func(c *Contract) { c.Rules[1].Path = "//graphql$" }, "not an anchored path"},
		"root anchored":      {func(c *Contract) { c.Rules[1].Path = "/$" }, "not an anchored path"},
		"double quote":       {func(c *Contract) { c.Rules[1].Path = `/gra"phql$` }, "not an anchored path"},
		"dot-dot segment":    {func(c *Contract) { c.Rules[1].Path = `/a/\.\./b$` }, ". or .. segment"},
		"dot segment":        {func(c *Contract) { c.Rules[1].Path = `/a/\.$` }, ". or .. segment"},
		"same path twice": {func(c *Contract) { c.Rules = append(c.Rules, anchored("/graphql$", PlaneGoAPI)) },
			"the path repeats rule 1"},
		"same path, other case": {func(c *Contract) { c.Rules = append(c.Rules, anchored("/GraphQL$", PlaneGoAPI)) },
			"the path repeats rule 1"},
		"second default": {func(c *Contract) { c.Rules = append(c.Rules, rule("/", PathTypePrefix, PlaneQueryAPI)) },
			"the path repeats rule 0"},
		"token over a literal": {func(c *Contract) { c.Rules = append(c.Rules, anchored("/api/v1/auth/[^/]+$", PlaneQueryAPI)) },
			"can match the same path"},
		"literal over a token": {func(c *Contract) { c.Rules = append(c.Rules, anchored("/api/v1/people/me/metric$", PlaneGoAPI)) },
			"can match the same path"},
		"two tokens": {func(c *Contract) { c.Rules = append(c.Rules, anchored("/api/[^/]+/people/x/[^/]+$", PlaneGoAPI)) },
			"can match the same path"},
		"a token over one-segment rules": {func(c *Contract) { c.Rules = append(c.Rules, anchored("/[^/]+$", PlaneGoAPI)) },
			"can match the same path"},
	} {
		t.Run(name, func(t *testing.T) {
			contract := validContract()
			c.change(&contract)
			err := contract.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a refusal that holds %q, got %v", c.want, err)
			}
			if _, renderErr := Render(contract, DefaultOptions()); renderErr == nil {
				t.Fatalf("Render must refuse a table Validate refuses")
			}
		})
	}
}

// TestValidateAcceptsPathsThatDoNotOverlap: the overlap refusal must not take
// two rules that no request path can match together.
func TestValidateAcceptsPathsThatDoNotOverlap(t *testing.T) {
	contract := validContract()
	contract.Rules = append(contract.Rules,
		anchored("/api/v1/people$", PlaneQueryAPI),                // one segment fewer than the token rule
		anchored("/api/v1/people/[^/]+/summary$", PlaneQueryAPI),  // same shape, another last segment
		anchored("/api/v1/people/[^/]+/metric/more$", PlaneGoAPI), // one segment more
		anchored("/api/v1/auth/login-two$", PlaneGoAPI),           // a longer word is another segment text
	)
	if err := contract.Validate(); err != nil {
		t.Fatalf("these rules cannot match the same path: %v", err)
	}
}

// TestParseRefusesWhatItDoesNotRead: a key this package does not know, and
// data after the document, must not load.
func TestParseRefusesWhatItDoesNotRead(t *testing.T) {
	const good = `{"schema_version": 1, "regex_mode": true, "rules": [{"path": "/", "path_type": "Prefix", "plane": "go-api"}]}`
	if _, err := Parse([]byte(good)); err != nil {
		t.Fatalf("the base document must load: %v", err)
	}
	for name, text := range map[string]string{
		"unknown top key":  `{"schema_version": 1, "regex_mode": true, "hosts": [], "rules": [{"path": "/", "path_type": "Prefix", "plane": "go-api"}]}`,
		"unknown rule key": `{"schema_version": 1, "regex_mode": true, "rules": [{"path": "/", "path_type": "Prefix", "plane": "go-api", "service": "web"}]}`,
		"camel case key":   `{"schema_version": 1, "regex_mode": true, "rules": [{"path": "/", "pathType": "Prefix", "plane": "go-api"}]}`,
		"two documents":    good + good,
		"not json":         `rules: []`,
		"invalid table":    `{"schema_version": 1, "regex_mode": true, "rules": []}`,
	} {
		if _, err := Parse([]byte(text)); err == nil {
			t.Errorf("%s: must not load", name)
		}
	}
}

// TestSchemaAndValidatorAgree: the JSON Schema is what a reader with no Go
// sees. The checked-in contract passes it, and each planted defect that the
// schema can express fails the schema AND the Go validator.
func TestSchemaAndValidatorAgree(t *testing.T) {
	schemaBytes, err := os.ReadFile(repoFile(t, "contracts/ingress/v1/planes.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	contractBytes, err := os.ReadFile(repoFile(t, ContractPath))
	if err != nil {
		t.Fatal(err)
	}
	load := func(t *testing.T) map[string]any {
		t.Helper()
		var document map[string]any
		if err := json.Unmarshal(contractBytes, &document); err != nil {
			t.Fatal(err)
		}
		return document
	}
	if err := resolved.Validate(load(t)); err != nil {
		t.Fatalf("the checked-in contract fails its schema: %v", err)
	}
	rules := func(document map[string]any) []any { return document["rules"].([]any) }
	second := func(document map[string]any) map[string]any { return rules(document)[1].(map[string]any) }
	for name, mutate := range map[string]func(map[string]any){
		"other schema version": func(d map[string]any) { d["schema_version"] = 2 },
		"no regex mode":        func(d map[string]any) { d["regex_mode"] = false },
		"unknown top key":      func(d map[string]any) { d["hosts"] = []any{} },
		"no rules":             func(d map[string]any) { d["rules"] = []any{} },
		"no default rule":      func(d map[string]any) { d["rules"] = rules(d)[1:] },
		"second default rule": func(d map[string]any) {
			d["rules"] = append(rules(d), map[string]any{"path": "/", "path_type": "Prefix", "plane": "query-api"})
		},
		"unknown plane":    func(d map[string]any) { second(d)["plane"] = "web" },
		"exact rule":       func(d map[string]any) { second(d)["path_type"] = "Exact" },
		"prefix rule":      func(d map[string]any) { second(d)["path_type"] = "Prefix" },
		"no end anchor":    func(d map[string]any) { second(d)["path"] = "/graphql" },
		"unescaped dot":    func(d map[string]any) { second(d)["path"] = "/a.b$" },
		"unknown rule key": func(d map[string]any) { second(d)["service"] = "query-api" },
		"missing plane":    func(d map[string]any) { delete(second(d), "plane") },
	} {
		t.Run(name, func(t *testing.T) {
			document := load(t)
			if second(document)["path"] != "/graphql$" {
				t.Fatalf("the second rule of the contract is not /graphql$: the mutations would not plant what their names say")
			}
			mutate(document)
			if err := resolved.Validate(document); err == nil {
				t.Errorf("the schema accepts the planted defect")
			}
			mutated, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Parse(mutated); err == nil {
				t.Errorf("the Go validator accepts the planted defect")
			}
		})
	}
}
