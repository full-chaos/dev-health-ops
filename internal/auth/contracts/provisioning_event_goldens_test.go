package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const manifestFileName = "manifest" + ".json"

// wireCorpus mirrors an examples/<surface>/manifest.json. Both surfaces below
// share its shape.
type wireCorpus struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
	RejectByClient []struct {
		File string `json:"file"`
		Why  string `json:"why"`
	} `json:"reject_by_client"`
	TimestampVectors struct {
		Accept []string `json:"accept"`
		Reject []string `json:"reject"`
	} `json:"timestamp_vectors"`
	ConsumerVectors struct {
		DedupeKeyExamples []struct {
			AggregateType string `json:"aggregate_type"`
			AggregateID   string `json:"aggregate_id"`
			Revision      int64  `json:"revision"`
			Key           string `json:"key"`
		} `json:"dedupe_key_examples"`
		Cases []struct {
			Name  string  `json:"name"`
			Steps [][]any `json:"steps"`
		} `json:"cases"`
	} `json:"consumer_vectors"`
}

// goMessageForProvisioningKeyword extends the shared token tables with the
// keywords only these corpora assert. Read off the library by running the
// fixtures, not guessed.
var goMessageForProvisioningKeyword = map[string]string{
	"maxProperties": "maxProperties:",
	"maximum":       "maximum:",
	"type":          "type:",
	"minItems":      "minItems:",
}

func provisioningTokenFor(keyword string) (string, bool) {
	if token, ok := goMessageForKeyword[keyword]; ok {
		return token, true
	}
	token, ok := goMessageForProvisioningKeyword[keyword]
	return token, ok
}

type provisioningSurface struct {
	surface string
	dir     string
	parse   func(root string, raw []byte) error
}

var provisioningSurfaces = []provisioningSurface{
	{ProvisioningEventSurface, "contracts/auth/v1/examples/provisioning-event", func(root string, raw []byte) error {
		_, err := ParseProvisioningEvent(root, raw)
		return err
	}},
	{AuditEventSurface, "contracts/auth/v1/examples/audit-event", func(root string, raw []byte) error {
		_, err := ParseAuditEvent(root, raw)
		return err
	}},
	{ExternalPushLifecycleEventSurface, "contracts/auth/v1/examples/external-push-lifecycle-event", func(root string, raw []byte) error {
		_, err := ParseExternalPushLifecycleEvent(root, raw)
		return err
	}},
}

func loadWireCorpus(t *testing.T, s provisioningSurface) wireCorpus {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), s.dir, manifestFileName))
	if err != nil {
		t.Fatalf("reading manifest: %v", err)
	}
	var corpus wireCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("parsing manifest: %v", err)
	}
	return corpus
}

func readCorpusFile(t *testing.T, s provisioningSurface, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), s.dir, name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func decodedCorpusFile(t *testing.T, s provisioningSurface, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(readCorpusFile(t, s, name), &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

func forEachProvisioningSurface(t *testing.T, body func(t *testing.T, s provisioningSurface, corpus wireCorpus)) {
	for _, s := range provisioningSurfaces {
		t.Run(s.surface, func(t *testing.T) { body(t, s, loadWireCorpus(t, s)) })
	}
}

func TestProvisioningCorporaAreNotEmptyAndEveryFixtureIsClaimed(t *testing.T) {
	forEachProvisioningSurface(t, func(t *testing.T, s provisioningSurface, c wireCorpus) {
		if len(c.Accept) == 0 || len(c.Reject) == 0 || len(c.RejectByClient) == 0 {
			t.Fatal("a manifest category is empty; every table-driven test would pass vacuously")
		}
		claimed := map[string]bool{}
		for _, a := range c.Accept {
			claimed[a.File] = true
		}
		for _, r := range c.Reject {
			claimed[r.File] = true
		}
		for _, r := range c.RejectByClient {
			claimed[r.File] = true
		}
		entries, err := os.ReadDir(filepath.Join(testRoot(t), s.dir))
		if err != nil {
			t.Fatal(err)
		}
		var unclaimed, missing []string
		onDisk := map[string]bool{}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".json") || name == manifestFileName {
				continue
			}
			onDisk[name] = true
			if !claimed[name] {
				unclaimed = append(unclaimed, name)
			}
		}
		for name := range claimed {
			if !onDisk[name] {
				missing = append(missing, name)
			}
		}
		sort.Strings(unclaimed)
		sort.Strings(missing)
		if len(unclaimed) > 0 || len(missing) > 0 {
			t.Fatalf("unclaimed on disk: %v; claimed but absent: %v", unclaimed, missing)
		}
	})
}

func TestAcceptedProvisioningFixturesValidateAndParse(t *testing.T) {
	forEachProvisioningSurface(t, func(t *testing.T, s provisioningSurface, c wireCorpus) {
		root := testRoot(t)
		for _, entry := range c.Accept {
			t.Run(entry.File, func(t *testing.T) {
				if err := Validate(root, s.surface, decodedCorpusFile(t, s, entry.File)); err != nil {
					t.Fatalf("schema refused an accept fixture: %v", err)
				}
				if err := s.parse(root, readCorpusFile(t, s, entry.File)); err != nil {
					t.Fatalf("client refused an accept fixture: %v", err)
				}
			})
		}
	})
}

func TestRejectedProvisioningFixturesFailByTheDeclaredKeyword(t *testing.T) {
	forEachProvisioningSurface(t, func(t *testing.T, s provisioningSurface, c wireCorpus) {
		root := testRoot(t)
		for _, entry := range c.Reject {
			t.Run(entry.File, func(t *testing.T) {
				err := Validate(root, s.surface, decodedCorpusFile(t, s, entry.File))
				if err == nil {
					t.Fatalf("validated cleanly; expected %q at %q (%s)", entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
				}
				token, ok := provisioningTokenFor(entry.ExpectKeyword)
				if !ok {
					t.Fatalf("no Go message token for keyword %q", entry.ExpectKeyword)
				}
				if !strings.Contains(err.Error(), token) {
					t.Fatalf("rejected, but not by %q (token %q absent): %v", entry.ExpectKeyword, token, err)
				}
				if parseErr := s.parse(root, readCorpusFile(t, s, entry.File)); parseErr == nil {
					t.Fatal("the client accepted a document the schema refuses")
				}
			})
		}
	})
}

func TestClientEnforcedProvisioningFixturesValidateButTheClientRefusesThem(t *testing.T) {
	forEachProvisioningSurface(t, func(t *testing.T, s provisioningSurface, c wireCorpus) {
		root := testRoot(t)
		for _, entry := range c.RejectByClient {
			t.Run(entry.File, func(t *testing.T) {
				if err := Validate(root, s.surface, decodedCorpusFile(t, s, entry.File)); err != nil {
					t.Fatalf("meant to be schema-valid; schema said: %v", err)
				}
				err := s.parse(root, readCorpusFile(t, s, entry.File))
				if err == nil {
					t.Fatalf("the client accepted a document only it can refuse (%s)", entry.Why)
				}
				if !strings.Contains(err.Error(), "decimal or exponent form") &&
					!strings.Contains(err.Error(), "does not equal") &&
					!strings.Contains(err.Error(), "RFC 3339") {
					t.Fatalf("refused, but not by a client rule: %v", err)
				}
			})
		}
	})
}

func TestEveryAggregateTypeHasAnIdentityAndVocabularyBranch(t *testing.T) {
	// A new aggregate_type with no conditional branch would carry an
	// unconstrained id, event_type and change object.
	raw, err := os.ReadFile(SchemaPath(testRoot(t), ProvisioningEventSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			AggregateType struct {
				Enum []string `json:"enum"`
			} `json:"aggregate_type"`
		} `json:"properties"`
		AllOf []struct {
			If struct {
				Properties struct {
					AggregateType struct {
						Const string `json:"const"`
					} `json:"aggregate_type"`
				} `json:"properties"`
			} `json:"if"`
			Then struct {
				Properties map[string]any `json:"properties"`
			} `json:"then"`
		} `json:"allOf"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	branches := map[string]bool{}
	for _, b := range schema.AllOf {
		for _, field := range []string{"aggregate_id", "event_type", "payload"} {
			if _, ok := b.Then.Properties[field]; !ok {
				t.Errorf("branch for %q does not constrain %s", b.If.Properties.AggregateType.Const, field)
			}
		}
		payload, _ := b.Then.Properties["payload"].(map[string]any)
		inner, _ := payload["properties"].(map[string]any)
		if _, ok := inner["change"]; !ok {
			t.Errorf("branch for %q does not constrain payload.change", b.If.Properties.AggregateType.Const)
		}
		branches[b.If.Properties.AggregateType.Const] = true
	}
	if len(schema.Properties.AggregateType.Enum) == 0 {
		t.Fatal("no aggregate types; the check would be vacuous")
	}
	for _, aggregate := range schema.Properties.AggregateType.Enum {
		if !branches[aggregate] {
			t.Errorf("aggregate_type %q has no conditional branch", aggregate)
		}
	}
}

func TestADuplicateMemberOrTrailingContentIsRefusedInEveryProvisioningParser(t *testing.T) {
	root := testRoot(t)
	valid := string(readCorpusFile(t, provisioningSurfaces[0], "valid-minimal.json"))
	validAudit := string(readCorpusFile(t, provisioningSurfaces[1], "valid-minimal.json"))
	for _, tc := range []struct {
		name string
		raw  string
		s    provisioningSurface
		want string
	}{
		{"duplicate revision", strings.Replace(valid, `"revision": 1,`, `"revision": 2, "revision": 1,`, 1), provisioningSurfaces[0], "duplicate object member"},
		{"duplicate attributes", strings.Replace(validAudit, `"attributes": {}`, `"attributes": {}, "attributes": {"note": "x"}`, 1), provisioningSurfaces[1], "duplicate object member"},
		{"trailing document", valid + "\n{}", provisioningSurfaces[0], "trailing content"},
		{"trailing byte", validAudit + "x", provisioningSurfaces[1], "trailing content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.s.parse(root, []byte(tc.raw))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestConsumerVectorsReplayToTheDeclaredDispositions(t *testing.T) {
	c := loadWireCorpus(t, provisioningSurfaces[0])
	if len(c.ConsumerVectors.Cases) == 0 {
		t.Fatal("no consumer vectors")
	}
	for _, tc := range c.ConsumerVectors.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			cursor := NewConsumerCursor()
			for i, step := range tc.Steps {
				event := ProvisioningEvent{
					AggregateType: step[0].(string),
					AggregateID:   step[1].(string),
					Payload:       ProvisioningPayload{Revision: Revision(int64(step[2].(float64)))},
				}
				if got := cursor.Observe(event); string(got) != step[3].(string) {
					t.Fatalf("step %d (%v): disposition %q, want %q", i, step[:3], got, step[3])
				}
			}
		})
	}
	for _, ex := range c.ConsumerVectors.DedupeKeyExamples {
		got := ProvisioningEvent{AggregateType: ex.AggregateType, AggregateID: ex.AggregateID, Payload: ProvisioningPayload{Revision: Revision(ex.Revision)}}.DedupeKey()
		if got != ex.Key {
			t.Errorf("DedupeKey = %q, want %q", got, ex.Key)
		}
	}
}

func jsonTags(typ reflect.Type) []string {
	var tags []string
	for i := 0; i < typ.NumField(); i++ {
		name := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			tags = append(tags, name)
		}
	}
	sort.Strings(tags)
	return tags
}

func schemaPropertyNames(t *testing.T, surface string, path ...string) []string {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
	if err != nil {
		t.Fatal(err)
	}
	var node map[string]any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	for _, step := range path {
		props, _ := node["properties"].(map[string]any)
		next, ok := props[step].(map[string]any)
		if !ok {
			t.Fatalf("%s has no %v", surface, path)
		}
		node = next
	}
	props, _ := node["properties"].(map[string]any)
	var names []string
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestParsedTypesCarryEverySchemaMember(t *testing.T) {
	// A parser that validates a document and then drops a member the schema
	// admits loses data without a sound. Each Go type must name exactly the
	// members of the schema object it parses, in both directions.
	for _, tc := range []struct {
		name    string
		typ     reflect.Type
		surface string
		path    []string
	}{
		{"ProvisioningEvent", reflect.TypeOf(ProvisioningEvent{}), ProvisioningEventSurface, nil},
		{"ProvisioningPayload", reflect.TypeOf(ProvisioningPayload{}), ProvisioningEventSurface, []string{"payload"}},
		{"AuditEvent", reflect.TypeOf(AuditEvent{}), AuditEventSurface, nil},
		{"AuditAttributes", reflect.TypeOf(AuditAttributes{}), AuditEventSurface, []string{"attributes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := jsonTags(tc.typ)
			want := schemaPropertyNames(t, tc.surface, tc.path...)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("%s members = %v, schema members = %v", tc.name, got, want)
			}
		})
	}
}

func TestTheAuditParserReturnsWhatTheDocumentCarried(t *testing.T) {
	event, err := ParseAuditEvent(testRoot(t), readCorpusFile(t, provisioningSurfaces[1], "valid-full.json"))
	if err != nil {
		t.Fatal(err)
	}
	if event.ActorPrincipalID == "" || event.SubjectPrincipalID == "" || event.OrganizationID == "" || event.RequestID == "" {
		t.Fatalf("identities dropped: %+v", event)
	}
	if event.PolicyRevision == nil || *event.PolicyRevision != 42 {
		t.Fatalf("policy revision = %v", event.PolicyRevision)
	}
	if event.Attributes.Action != "acr.context.read" || event.Attributes.AggregateRevision == nil || *event.Attributes.AggregateRevision != 4 {
		t.Fatalf("attributes dropped: %+v", event.Attributes)
	}
}

func TestTimestampVectorsReplayToTheSharedRule(t *testing.T) {
	// The same list drives the Python test: one rule, two implementations.
	c := loadWireCorpus(t, provisioningSurfaces[0])
	if len(c.TimestampVectors.Accept) == 0 || len(c.TimestampVectors.Reject) == 0 {
		t.Fatal("no timestamp vectors; the replay would pass vacuously")
	}
	for _, v := range c.TimestampVectors.Accept {
		if _, err := ParseWireTimestamp(ProvisioningEventSurface, "created_at", v); err != nil {
			t.Errorf("accept vector %s refused: %v", v, err)
		}
	}
	for _, v := range c.TimestampVectors.Reject {
		if _, err := ParseWireTimestamp(ProvisioningEventSurface, "created_at", v); err == nil {
			t.Errorf("reject vector %s accepted", v)
		}
	}
}

func TestEveryTimestampVectorPassesTheSchemaPattern(t *testing.T) {
	// The vectors exist for what the pattern cannot see, so each must get past it.
	c := loadWireCorpus(t, provisioningSurfaces[0])
	root := testRoot(t)
	for _, v := range append(append([]string{}, c.TimestampVectors.Accept...), c.TimestampVectors.Reject...) {
		document := map[string]any{
			"schema_version": "provisioning-event.v1", "aggregate_type": "principal",
			"aggregate_id": "prn_EXAMPLE0000000000000001", "event_type": "principal.created",
			"idempotency_key": "principal:prn_EXAMPLE0000000000000001:1", "created_at": v,
			"payload": map[string]any{"revision": 1, "change": map[string]any{}},
		}
		if err := Validate(root, ProvisioningEventSurface, document); err != nil {
			t.Errorf("vector %s does not pass the schema, so it tests nothing the client alone decides: %v", v, err)
		}
	}
}
