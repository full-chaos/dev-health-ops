package queuecreds_test

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/queuecreds"
)

// copyContracts copies the real job contract tree into a temporary root so a
// test can plant one defect and read what the scan reports.
func copyContracts(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	temp := t.TempDir()
	source := filepath.Join(root, "contracts", "jobs", "v1")
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".json" {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		target := filepath.Join(temp, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return temp
}

func mutateJSON(t *testing.T, root, rel string, mutate func(map[string]any)) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var node map[string]any
	if err := json.Unmarshal(data, &node); err != nil {
		t.Fatal(err)
	}
	mutate(node)
	out, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func wholeText(findings []queuecreds.Finding) string {
	var lines []string
	for _, finding := range findings {
		lines = append(lines, finding.String())
	}
	return strings.Join(lines, "\n")
}

const (
	heartbeatSchema  = "contracts/jobs/v1/schemas/system.heartbeat.v1.schema.json"
	heartbeatExample = "contracts/jobs/v1/examples/system.heartbeat.v1.json"
	providerSchema   = "contracts/jobs/v1/schemas/sync.provider_unit.v1.schema.json"
)

func schemaProperties(node map[string]any) map[string]any { return node["properties"].(map[string]any) }

func TestContractScanReportsEachPlantedDefect(t *testing.T) {
	root := repoRoot(t)
	values := valueScanner(t, root)
	cases := []struct {
		name   string
		plant  func(t *testing.T, temp string)
		expect string
	}{
		{"no defect", func(*testing.T, string) {}, ""},
		{"credential-shaped property name", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
				schemaProperties(node)["access_token"] = map[string]any{"type": "string", "pattern": "^[a-z]+$"}
			})
		}, "access_token: property name reads as a credential"},
		{"benign property name stays green", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
				schemaProperties(node)["credential_fingerprint"] = map[string]any{"type": "string", "pattern": "^[0-9a-f]{64}$"}
				schemaProperties(node)["idempotency_key"] = map[string]any{"type": "string", "pattern": "^[a-z]+$"}
			})
		}, ""},
		{"payload schema left open", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) { node["additionalProperties"] = true })
		}, "system.heartbeat.v1.schema.json $: object schema is not closed"},
		{"payload schema without the closing clause", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) { delete(node, "additionalProperties") })
		}, "system.heartbeat.v1.schema.json $: object schema is not closed"},
		{"pattern properties", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
				node["patternProperties"] = map[string]any{"^x-": map[string]any{"type": "string", "pattern": "^a$"}}
			})
		}, "patternProperties lets a payload carry names"},
		{"free-form string property", func(t *testing.T, temp string) {
			mutateJSON(t, temp, providerSchema, func(node map[string]any) {
				unit := schemaProperties(node)["unit_id"].(map[string]any)
				delete(unit, "format")
				delete(unit, "pattern")
			})
		}, "unit_id: string property has no format, pattern, enum or const"},
		{"free-form object property", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
				schemaProperties(node)["extra"] = map[string]any{"type": "object", "properties": map[string]any{}}
			})
		}, "$.properties.extra: object schema is not closed"},
		{"envelope payload slot is not exempt elsewhere", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
				schemaProperties(node)["payload"] = map[string]any{"type": "object", "properties": map[string]any{}, "maxProperties": 32}
			})
		}, "$.properties.payload: object schema is not closed"},
		{"jwt-shaped example value", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatExample, func(node map[string]any) {
				node["payload"].(map[string]any)["scheduled_for"] = plantedCredential()
			})
		}, "value matches the credential rule jwt"},
		{"service credential prefix in an example", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatExample, func(node map[string]any) {
				node["correlation_id"] = "svc_worker_" + "abcdefghijklmnop"
			})
		}, "value matches the credential rule prefix:svc_worker_"},
		{"bearer header in an example", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatExample, func(node map[string]any) {
				node["idempotency_key"] = "Bearer " + "abcdefghijklmnop"
			})
		}, "value matches the credential rule scheme:Bearer"},
		{"credential-shaped key in an example", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatExample, func(node map[string]any) {
				node["payload"].(map[string]any)["refresh_token"] = "x"
			})
		}, "refresh_token: key name reads as a credential"},
		{"url userinfo in an example", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatExample, func(node map[string]any) {
				node["correlation_id"] = "postgres://user:" + "pass@host/db"
			})
		}, "value matches the credential rule url-userinfo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			temp := copyContracts(t)
			tc.plant(t, temp)
			scan, err := queuecreds.ScanContracts(temp, values)
			if err != nil {
				t.Fatal(err)
			}
			text := wholeText(scan.Findings)
			if tc.expect == "" {
				if text != "" {
					t.Fatalf("unexpected findings:\n%s", text)
				}
				return
			}
			if !strings.Contains(text, tc.expect) {
				t.Fatalf("planted defect not reported; want %q in:\n%s", tc.expect, text)
			}
			if got := len(scan.Findings); got != 1 {
				t.Fatalf("planted one defect, got %d findings:\n%s", got, text)
			}
		})
	}
}

func TestValueScannerNeedsAuthDerivedRules(t *testing.T) {
	if _, err := queuecreds.NewValueScanner([]byte(`{"classes":[{"markers":[]}]}`)); err == nil {
		t.Fatal("a credential contract with no prefix or scheme marker built a scanner")
	}
	if _, err := queuecreds.NewValueScanner([]byte(`not json`)); err == nil {
		t.Fatal("an unreadable credential contract built a scanner")
	}
}

func TestCredentialNameWords(t *testing.T) {
	for _, name := range []string{
		"access_token", "refreshToken", "RefreshToken", "client_secret", "Password", "bearer", "Authorization",
		"api_key", "APIKey", "private_key", "signing_key", "dsn", "DSN", "session_cookie", "jwt", "svc_credential", "claimToken",
	} {
		if !queuecreds.CredentialName(name) {
			t.Errorf("%q should read as a credential", name)
		}
	}
	for _, name := range []string{
		"idempotency_key", "trace_parent", "unit_id", "organization_id", "scheduled_for", "credential_fingerprint",
		"token_hash", "dsn_key", "DSNKey", "contract_version", "correlation_id", "retention_policy", "tokenizer", "key",
	} {
		if queuecreds.CredentialName(name) {
			t.Errorf("%q should not read as a credential", name)
		}
	}
}

type clean struct {
	UnitID string `json:"unit_id"`
	Nested struct {
		Count int `json:"count"`
	} `json:"nested"`
	Skipped string `json:"-"`
}

type embedded struct{ Token string }

type withEmbedded struct {
	embedded
	Name string `json:"name"`
}

type exportedEmbedded struct{ Secret string }

type withExportedEmbedded struct {
	exportedEmbedded
}

type openCarriers struct {
	Any    any            `json:"a"`
	Map    map[string]any `json:"m"`
	Bytes  []byte         `json:"b"`
	Raw    json.RawMessage
	Nested []struct {
		Password string `json:"password"`
	} `json:"nested"`
}

type serializedSecret struct {
	ClaimToken string
}

type hiddenSecret struct {
	ClaimToken string `json:"-"`
}

type recursive struct {
	Next *recursive `json:"next"`
	ID   string     `json:"id"`
}

func TestScanTypeReportsEachPlantedDefect(t *testing.T) {
	cases := []struct {
		name   string
		typ    reflect.Type
		expect []string
	}{
		{"clean type", reflect.TypeOf(clean{}), nil},
		{"untagged field uses the Go name", reflect.TypeOf(serializedSecret{}), []string{"ClaimToken: field name reads as a credential"}},
		{"unserialized field is skipped", reflect.TypeOf(hiddenSecret{}), nil},
		{"recursive type ends", reflect.TypeOf(recursive{}), nil},
		{"open carriers and nested names", reflect.TypeOf(openCarriers{}), []string{
			".a: interface", ".m: map", ".b: byte slice", ".Raw: byte slice", ".nested[].password: field name reads as a credential"}},
		{"unexported embedded struct is flattened", reflect.TypeOf(withEmbedded{}), []string{".Token: field name reads as a credential"}},
		{"exported embedded struct is flattened", reflect.TypeOf(withExportedEmbedded{}), []string{".Secret: field name reads as a credential"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text := wholeText(queuecreds.ScanType("T", tc.typ))
			if len(tc.expect) == 0 && text != "" {
				t.Fatalf("unexpected findings:\n%s", text)
			}
			for _, want := range tc.expect {
				if !strings.Contains(text, want) {
					t.Errorf("want %q in:\n%s", want, text)
				}
			}
			if got := strings.Count(text, "\n") + 1; text != "" && got != len(tc.expect) {
				t.Errorf("want %d findings, got %d:\n%s", len(tc.expect), got, text)
			}
		})
	}
}

func writePackage(t *testing.T, source string) queuecreds.PackageTypes {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte("package p\n"+source), 0o644); err != nil {
		t.Fatal(err)
	}
	parsed, err := queuecreds.ParsePackage(dir)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

const dsnRecord = `
func logResolvedDatabase() {
	var record struct {
		DSN struct {
			Name     string ` + "`json:\"name\"`" + `
			Form     string ` + "`json:\"form\"`" + `
			Database string ` + "`json:\"database,omitempty\"`" + `
		} ` + "`json:\"dsn\"`" + `
	}
	_ = record
}
`

func TestOperatorExemptionPinsTheShape(t *testing.T) {
	cases := []struct {
		name   string
		source string
		expect []string
	}{
		{"pinned shape is exempt", dsnRecord, nil},
		{"string-typed dsn at the pinned path", `
func logResolvedDatabase() {
	var record struct {
		DSN string ` + "`json:\"dsn\"`" + `
	}
	_ = record
}`, []string{"json tag dsn: tag name reads as a credential", "exemption logResolvedDatabase/dsn: exemption matches no field"}},
		{"extra child", strings.Replace(dsnRecord, "Database string", "Host string `json:\"host\"`\n\t\t\tDatabase string", 1),
			[]string{"json tag dsn: tag name reads as a credential", "exemption logResolvedDatabase/dsn"}},
		{"removed child", strings.Replace(dsnRecord, "\t\t\tForm     string `json:\"form\"`\n", "", 1),
			[]string{"json tag dsn: tag name reads as a credential", "exemption logResolvedDatabase/dsn"}},
		{"child with a credential name", strings.Replace(dsnRecord, `json:"form"`, `json:"password"`, 1),
			[]string{"json tag dsn: tag name reads as a credential", "json tag password", "exemption logResolvedDatabase/dsn"}},
		{"child of another type", strings.Replace(dsnRecord, "Form     string", "Form     []byte", 1),
			[]string{"json tag dsn: tag name reads as a credential", "exemption logResolvedDatabase/dsn"}},
		{"same tag in another function", strings.Replace(dsnRecord, "logResolvedDatabase", "somethingElse", 1),
			[]string{"json tag dsn: tag name reads as a credential", "exemption logResolvedDatabase/dsn"}},
		{"tag renamed leaves a stale row", strings.Replace(dsnRecord, `json:"dsn"`, `json:"database_label"`, 1),
			[]string{"exemption logResolvedDatabase/dsn: exemption matches no field"}},
		{"package-level credential tag", dsnRecord + "\ntype out struct {\n\tAccessToken string `json:\"access_token\"`\n}\n",
			[]string{"json tag access_token: tag name reads as a credential"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text := wholeText(writePackage(t, tc.source).OperatorFindings("p", queuecreds.OperatorExemptions))
			if len(tc.expect) == 0 && text != "" {
				t.Fatalf("unexpected findings:\n%s", text)
			}
			for _, want := range tc.expect {
				if !strings.Contains(text, want) {
					t.Errorf("want %q in:\n%s", want, text)
				}
			}
			if text != "" && strings.Count(text, "\n")+1 != len(tc.expect) {
				t.Errorf("want %d findings, got:\n%s", len(tc.expect), text)
			}
		})
	}
}

func TestContractScanReadsReferencesAndNestedSchemas(t *testing.T) {
	root := repoRoot(t)
	values := valueScanner(t, root)
	t.Run("reference to an outside schema", func(t *testing.T) {
		temp := copyContracts(t)
		mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
			schemaProperties(node)["scheduled_for"] = map[string]any{"$ref": "../outside.schema.json"}
		})
		scan, err := queuecreds.ScanContracts(temp, values)
		if err != nil {
			t.Fatal(err)
		}
		text := wholeText(scan.Findings)
		if !strings.Contains(text, "$ref points at a schema the scan does not read") || len(scan.Findings) != 1 {
			t.Fatalf("want exactly the reference finding, got:\n%s", text)
		}
	})
	t.Run("schema in a subdirectory", func(t *testing.T) {
		temp := copyContracts(t)
		base, err := queuecreds.ScanContracts(temp, values)
		if err != nil {
			t.Fatal(err)
		}
		nested := filepath.Join(temp, "contracts", "jobs", "v1", "schemas", "nested")
		if err := os.MkdirAll(nested, 0o755); err != nil {
			t.Fatal(err)
		}
		body := `{"type":"object","additionalProperties":false,"properties":{"access_token":{"type":"string","pattern":"^a$"}}}`
		if err := os.WriteFile(filepath.Join(nested, "x.schema.json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		scan, err := queuecreds.ScanContracts(temp, values)
		if err != nil {
			t.Fatal(err)
		}
		text := wholeText(scan.Findings)
		if scan.Schemas != base.Schemas+1 || !strings.Contains(text, "nested/x.schema.json") || len(scan.Findings) != 1 {
			t.Fatalf("want the nested schema scanned with one finding, got %d schemas:\n%s", scan.Schemas, text)
		}
	})
}

func TestOperatorScanReadsUntaggedFields(t *testing.T) {
	cases := []struct {
		name   string
		source string
		expect string
	}{
		{"exported untagged field", "type out struct {\n\tAccessToken string\n}\n", "json tag AccessToken: tag name reads as a credential"},
		{"unexported untagged field", "type out struct {\n\taccessToken string\n}\n", ""},
		{"untagged field hidden from json", "type out struct {\n\tAccessToken string `json:\"-\"`\n}\n", ""},
		{"untagged benign field", "type out struct {\n\tCount int\n\tCredentialFingerprint string\n}\n", ""},
		{"parameter named like a credential", "func f(AccessToken string) {}\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text := wholeText(writePackage(t, tc.source).OperatorFindings("p", nil))
			if tc.expect == "" && text != "" {
				t.Fatalf("unexpected findings:\n%s", text)
			}
			if tc.expect != "" && (!strings.Contains(text, tc.expect) || strings.Count(text, "\n") != 0) {
				t.Fatalf("want exactly %q, got:\n%s", tc.expect, text)
			}
		})
	}
}

func TestFindEmbeddersFollowsEmbeddingChains(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "p")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := `package p

import "example.test/jobruntime"

type Direct struct {
	jobruntime.HeartbeatArgs
	Token string
}
type Pointer struct{ *Direct }
type Generic struct{ Wrapper[Pointer] }
type Wrapper[T any] struct{}
type Unrelated struct{ Count int }
type Named struct{ inner jobruntime.HeartbeatArgs }
`
	if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "internal", "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "internal", "testdata", "t.go"), []byte("package t\ntype Skipped struct{ HeartbeatArgs }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := queuecreds.FindEmbedders(root, []string{"internal"}, map[string]bool{"HeartbeatArgs": true})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"p.Direct", "p.Pointer"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("embedders = %v, want %v", got, want)
	}
	if _, err := queuecreds.FindEmbedders(root, []string{"missing"}, nil); err == nil {
		t.Fatal("a missing directory was scanned as empty")
	}
}

func TestAdmittingStringsReportsAPlantedLooseProperty(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	probes, err := queuecreds.CredentialProbes(data)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		plant func(t *testing.T, temp string)
		extra string
		lost  string
	}{
		{"no change", func(*testing.T, string) {}, "", ""},
		{"payload property loosened to a token alphabet", func(t *testing.T, temp string) {
			mutateJSON(t, temp, providerSchema, func(node map[string]any) {
				unit := schemaProperties(node)["unit_id"].(map[string]any)
				delete(unit, "format")
				unit["pattern"] = "^[A-Za-z0-9._-]+$"
			})
		}, "schemas/sync.provider_unit.v1.schema.json $.properties.unit_id", ""},
		{"payload property with no restriction", func(t *testing.T, temp string) {
			mutateJSON(t, temp, providerSchema, func(node map[string]any) {
				unit := schemaProperties(node)["unit_id"].(map[string]any)
				delete(unit, "format")
				delete(unit, "pattern")
			})
		}, "schemas/sync.provider_unit.v1.schema.json $.properties.unit_id", ""},
		{"date-time property with a pattern that matches anything", func(t *testing.T, temp string) {
			mutateJSON(t, temp, heartbeatSchema, func(node map[string]any) {
				schemaProperties(node)["scheduled_for"] = map[string]any{"type": "string", "format": "date-time", "pattern": ".*"}
			})
		}, "", ""},
		{"free-text identifier tightened", func(t *testing.T, temp string) {
			mutateJSON(t, temp, "contracts/jobs/v1/envelope.schema.json", func(node map[string]any) {
				schemaProperties(node)["correlation_id"].(map[string]any)["pattern"] = "^job-[0-9]{4}$"
			})
		}, "", "envelope.schema.json $.properties.correlation_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			temp := copyContracts(t)
			tc.plant(t, temp)
			got, err := queuecreds.AdmittingStrings(temp, probes)
			if err != nil {
				t.Fatal(err)
			}
			base := append(append([]string{}, queuecreds.FreeTextIdentifiers...), queuecreds.RuntimeBoundStrings...)
			if tc.lost != "" {
				kept := base[:0]
				for _, entry := range base {
					if entry != tc.lost {
						kept = append(kept, entry)
					}
				}
				base = kept
			}
			if tc.extra != "" {
				base = append(base, tc.extra)
			}
			sort.Strings(base)
			if !reflect.DeepEqual(got, base) {
				t.Fatalf("admitting strings:\n got  %q\n want %q", got, base)
			}
		})
	}
}
