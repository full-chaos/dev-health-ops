package ingressplanes

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
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
	}, PublicHostRules: []Rule{
		rule("/docs", PathTypeExact, PlaneGoAPI),
		rule("/open-api.v3", PathTypeExact, PlaneGoAPI),
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
	// The paths that only the public host lists: the four Exact paths of its
	// block object, all to the default plane.
	var public []string
	for _, r := range contract.PublicHostRules {
		public = append(public, r.PathType+" "+r.Path+" "+r.Plane)
	}
	sort.Strings(public)
	want := []string{"Exact /docs go-api", "Exact /metrics go-api", "Exact /openapi.json go-api", "Exact /redoc go-api"}
	if strings.Join(public, "; ") != strings.Join(want, "; ") {
		t.Errorf("public host rules: got %v, want %v", public, want)
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
		// Characters that would end or change the nginx directive the path is written into.
		"semicolon":                 {func(c *Contract) { c.Rules[1].Path = "/gra;phql$" }, "not an anchored path"},
		"space":                     {func(c *Contract) { c.Rules[1].Path = "/gra phql$" }, "not an anchored path"},
		"tab":                       {func(c *Contract) { c.Rules[1].Path = "/gra\tphql$" }, "not an anchored path"},
		"open brace":                {func(c *Contract) { c.Rules[1].Path = "/gra{phql$" }, "not an anchored path"},
		"close brace":               {func(c *Contract) { c.Rules[1].Path = "/gra}phql$" }, "not an anchored path"},
		"anchor inside the path":    {func(c *Contract) { c.Rules[1].Path = "/gra$phql$" }, "not an anchored path"},
		"anchor before a slash":     {func(c *Contract) { c.Rules[1].Path = "/graphql$/x$" }, "not an anchored path"},
		"newline inside":            {func(c *Contract) { c.Rules[1].Path = "/gra\nphql$" }, "not an anchored path"},
		"newline at the end":        {func(c *Contract) { c.Rules[1].Path = "/graphql$\n" }, "not an anchored path"},
		"newline before the anchor": {func(c *Contract) { c.Rules[1].Path = "/graphql\n$" }, "not an anchored path"},
		"hash":                      {func(c *Contract) { c.Rules[1].Path = "/gra#phql$" }, "not an anchored path"},
		"single quote":              {func(c *Contract) { c.Rules[1].Path = "/gra'phql$" }, "not an anchored path"},
		"lone backslash":            {func(c *Contract) { c.Rules[1].Path = `/gra\phql$` }, "not an anchored path"},
		// The paths that only the public host lists.
		"public: not exact": {func(c *Contract) { c.PublicHostRules[0].PathType = PathTypePrefix }, "a public host rule is an Exact path"},
		"public: anchored": {func(c *Contract) {
			c.PublicHostRules[0] = anchored("/docs$", PlaneGoAPI)
		}, "a public host rule is an Exact path"},
		"public: regex in the path": {func(c *Contract) { c.PublicHostRules[0].Path = "/docs$" }, "not a literal path"},
		"public: token in the path": {func(c *Contract) { c.PublicHostRules[0].Path = "/docs/[^/]+" }, "not a literal path"},
		"public: semicolon":         {func(c *Contract) { c.PublicHostRules[0].Path = "/do;cs" }, "not a literal path"},
		"public: trailing slash":    {func(c *Contract) { c.PublicHostRules[0].Path = "/docs/" }, "not a literal path"},
		"public: root":              {func(c *Contract) { c.PublicHostRules[0].Path = "/" }, "not a literal path"},
		"public: dot-dot segment":   {func(c *Contract) { c.PublicHostRules[0].Path = "/docs/../x" }, ". or .. segment"},
		"public: other plane":       {func(c *Contract) { c.PublicHostRules[0].Plane = PlaneQueryAPI }, "is not the default plane"},
		"public: unknown plane":     {func(c *Contract) { c.PublicHostRules[0].Plane = "web" }, "is not the default plane"},
		"public: same path twice": {func(c *Contract) {
			c.PublicHostRules = append(c.PublicHostRules, rule("/DOCS", PathTypeExact, PlaneGoAPI))
		}, "the path repeats public host rule 0"},
		// On the public host the Exact path is the regex ^<path>: no end anchor, any case, "." is any character.
		"public: the path of a rule of the other plane": {func(c *Contract) { c.PublicHostRules[0].Path = "/graphql" }, "can take a request from rule 1"},
		"public: the same in another case":              {func(c *Contract) { c.PublicHostRules[0].Path = "/GraphQL" }, "can take a request from rule 1"},
		"public: a start of it":                         {func(c *Contract) { c.PublicHostRules[0].Path = "/graph" }, "can take a request from rule 1"},
		"public: a dot for a letter":                    {func(c *Contract) { c.PublicHostRules[0].Path = "/gr.phql" }, "can take a request from rule 1"},
		"public: a dot for a slash":                     {func(c *Contract) { c.PublicHostRules[0].Path = "/api.v1/people" }, "can take a request from rule 2"},
		"public: a start of a token rule":               {func(c *Contract) { c.PublicHostRules[0].Path = "/api/v1/people/me" }, "can take a request from rule 2"},
		"public: a token rule in full":                  {func(c *Contract) { c.PublicHostRules[0].Path = "/api/v1/people/me/metric" }, "can take a request from rule 2"},
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

// TestValidateAcceptsPublicHostPathsThatTakeNoRequest: the refusal of a public
// host path must not take a path that no rule of another plane can match, and
// must not take one that only meets rules of its own plane.
func TestValidateAcceptsPublicHostPathsThatTakeNoRequest(t *testing.T) {
	contract := validContract()
	contract.PublicHostRules = []Rule{
		rule("/graphqlx", PathTypeExact, PlaneGoAPI),                 // longer than /graphql$: that rule ends first
		rule("/graphq.x", PathTypeExact, PlaneGoAPI),                 // the same with a dot
		rule("/api/v1/people/me/metrics", PathTypeExact, PlaneGoAPI), // longer than the token rule's last word
		rule("/api/v1/people/a/b/metric", PathTypeExact, PlaneGoAPI), // two segments where the token takes one
		rule("/api/v1/auth/login", PathTypeExact, PlaneGoAPI),        // meets a rule of its own plane only
		rule("/api/v1/auth", PathTypeExact, PlaneGoAPI),              // a start of a rule of its own plane
		rule("/metrics", PathTypeExact, PlaneGoAPI),                  // meets no rule
	}
	if err := contract.Validate(); err != nil {
		t.Fatalf("these public host paths take no request from a rule of the other plane: %v", err)
	}
	contract.PublicHostRules = nil
	if err := contract.Validate(); err != nil {
		t.Fatalf("a table with no public host rule is valid: %v", err)
	}
}

// TestParseRefusesWhatItDoesNotRead: a key this package does not know, and
// data after the document, must not load.
func TestParseRefusesWhatItDoesNotRead(t *testing.T) {
	const good = `{"schema_version": 1, "regex_mode": true, "rules": [{"path": "/", "path_type": "Prefix", "plane": "go-api"}], "public_host_rules": [{"path": "/docs", "path_type": "Exact", "plane": "go-api"}]}`
	if _, err := Parse([]byte(good)); err != nil {
		t.Fatalf("the base document must load: %v", err)
	}
	for name, text := range map[string]string{
		"unknown top key":      strings.Replace(good, `"regex_mode": true,`, `"regex_mode": true, "hosts": [],`, 1),
		"unknown rule key":     strings.Replace(good, `"plane": "go-api"}]`, `"plane": "go-api", "service": "web"}]`, 1),
		"camel case key":       strings.Replace(good, `"path_type": "Prefix"`, `"pathType": "Prefix"`, 1),
		"two documents":        good + good,
		"not json":             `rules: []`,
		"an array":             `[` + good + `]`,
		"invalid table":        strings.Replace(good, `{"path": "/", "path_type": "Prefix", "plane": "go-api"}`, ``, 1),
		"rules not a list":     strings.Replace(good, `[{"path": "/", "path_type": "Prefix", "plane": "go-api"}]`, `{}`, 1),
		"a rule not an object": strings.Replace(good, `{"path": "/", "path_type": "Prefix", "plane": "go-api"}`, `"/"`, 1),
		// A key that is not there at all.
		"no public_host_rules": strings.Replace(good, `, "public_host_rules": [{"path": "/docs", "path_type": "Exact", "plane": "go-api"}]`, ``, 1),
		"no regex_mode":        strings.Replace(good, `"regex_mode": true, `, ``, 1),
		"no plane":             strings.Replace(good, `, "plane": "go-api"}]`, `}]`, 1),
		// encoding/json fills a field from a key in any case; the schema does not.
		"key Schema_Version":        strings.Replace(good, `"schema_version"`, `"Schema_Version"`, 1),
		"key REGEX_MODE":            strings.Replace(good, `"regex_mode"`, `"REGEX_MODE"`, 1),
		"key Rules":                 strings.Replace(good, `"rules"`, `"Rules"`, 1),
		"key Public_Host_Rules":     strings.Replace(good, `"public_host_rules"`, `"Public_Host_Rules"`, 1),
		"key Path":                  strings.Replace(good, `"path": "/",`, `"Path": "/",`, 1),
		"key PATH_TYPE":             strings.Replace(good, `"path_type": "Prefix"`, `"PATH_TYPE": "Prefix"`, 1),
		"key Plane":                 strings.Replace(good, `"plane": "go-api"}]`, `"Plane": "go-api"}]`, 1),
		"key Path in a public rule": strings.Replace(good, `"path": "/docs"`, `"Path": "/docs"`, 1),
		"key in both cases":         strings.Replace(good, `"path": "/",`, `"path": "/", "Path": "/x$",`, 1),
	} {
		if text == good {
			t.Fatalf("%s: the case changes nothing in the base document", name)
		}
		if _, err := Parse([]byte(text)); err == nil {
			t.Errorf("%s: must not load", name)
		}
	}
}

// TestSchemaAndValidatorAgree: the JSON Schema is what a reader with no Go
// sees. The checked-in contract passes it, and each planted defect that the
// schema can express fails the schema AND the Go reader (Parse). The schema
// holds the shape of a row; the rules across rows (one path one rule, a public
// host path on the default plane) are the Go validator's alone.
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
	public := func(document map[string]any) map[string]any {
		return document["public_host_rules"].([]any)[0].(map[string]any)
	}
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
		"semicolon":        func(d map[string]any) { second(d)["path"] = "/gra;phql$" },
		"space":            func(d map[string]any) { second(d)["path"] = "/gra phql$" },
		"open brace":       func(d map[string]any) { second(d)["path"] = "/gra{phql$" },
		"close brace":      func(d map[string]any) { second(d)["path"] = "/gra}phql$" },
		"anchor inside":    func(d map[string]any) { second(d)["path"] = "/gra$phql$" },
		"newline inside":   func(d map[string]any) { second(d)["path"] = "/gra\nphql$" },
		"hash":             func(d map[string]any) { second(d)["path"] = "/gra#phql$" },
		// A key in another case: the schema reads it as an unknown key and a missing key.
		"key Path":  func(d map[string]any) { second(d)["Path"] = second(d)["path"]; delete(second(d), "path") },
		"key Plane": func(d map[string]any) { second(d)["Plane"] = second(d)["plane"]; delete(second(d), "plane") },
		"key Rules": func(d map[string]any) { d["Rules"] = d["rules"]; delete(d, "rules") },
		"key Regex_Mode": func(d map[string]any) {
			d["Regex_Mode"] = d["regex_mode"]
			delete(d, "regex_mode")
		},
		// The paths that only the public host lists.
		"no public_host_rules":  func(d map[string]any) { delete(d, "public_host_rules") },
		"public: not a list":    func(d map[string]any) { d["public_host_rules"] = map[string]any{} },
		"public: prefix":        func(d map[string]any) { public(d)["path_type"] = "Prefix" },
		"public: anchored":      func(d map[string]any) { public(d)["path_type"] = "ImplementationSpecific" },
		"public: regex path":    func(d map[string]any) { public(d)["path"] = "/metrics$" },
		"public: semicolon":     func(d map[string]any) { public(d)["path"] = "/met;rics" },
		"public: unknown plane": func(d map[string]any) { public(d)["plane"] = "web" },
		"public: unknown key":   func(d map[string]any) { public(d)["service"] = "go-api" },
		"public: missing plane": func(d map[string]any) { delete(public(d), "plane") },
		"public: key Path":      func(d map[string]any) { public(d)["Path"] = public(d)["path"]; delete(public(d), "path") },
	} {
		t.Run(name, func(t *testing.T) {
			document := load(t)
			if second(document)["path"] != "/graphql$" || public(document)["path"] != "/metrics" {
				t.Fatalf("the second rule of the contract is not /graphql$, or the first public host rule is not /metrics: the mutations would not plant what their names say")
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
