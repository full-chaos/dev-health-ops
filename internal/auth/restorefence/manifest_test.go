package restorefence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func sampleManifest(t *testing.T) Manifest {
	t.Helper()
	m := Manifest{Version: ManifestVersion, SourceTimeline: 2,
		Floors: []Floor{
			{AggregateType: "organization", AggregateID: "org_b", Revision: 7},
			{AggregateType: "organization", AggregateID: "org_a", Revision: 3},
		}}
	for _, s := range sections {
		rows := []Row{}
		switch s.name {
		case "auth.credential_registry":
			rows = []Row{{"id": "00000000-0000-4000-8000-000000000002", "revoked_at": "2026-10-29T00:00:01+00:00", "revoked_reason": "compromised"}}
		case "public.users":
			rows = []Row{{"id": "00000000-0000-4000-8000-000000000003", "is_active": false, "token_version": json.Number("4")}}
		}
		m.Sections = append(m.Sections, Section{Name: s.name, Rows: rows})
	}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAManifestRoundTripsAndKeepsItsDigest(t *testing.T) {
	m := sampleManifest(t)
	raw, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseManifest(raw)
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if got.SHA256 != m.SHA256 || len(got.Floors) != 2 || len(got.Sections) != len(sections) {
		t.Fatalf("round trip lost content: %+v", got)
	}
}

func TestTheDigestDoesNotDependOnRowOrSectionOrder(t *testing.T) {
	a := sampleManifest(t)
	b := a
	b.Floors = []Floor{a.Floors[1], a.Floors[0]}
	b.Sections = append([]Section(nil), a.Sections...)
	for i, j := 0, len(b.Sections)-1; i < j; i, j = i+1, j-1 {
		b.Sections[i], b.Sections[j] = b.Sections[j], b.Sections[i]
	}
	da, _ := a.Digest()
	db, _ := b.Digest()
	if da != db {
		t.Fatal("the digest changed with the order of floors or sections")
	}
}

func TestAChangedValueChangesTheDigestAndIsRefused(t *testing.T) {
	m := sampleManifest(t)
	raw, _ := m.Marshal()
	tampered := bytes.Replace(raw, []byte(`"revision": 7`), []byte(`"revision": 6`), 1)
	if bytes.Equal(raw, tampered) {
		t.Fatal("fixture did not change")
	}
	if _, err := ParseManifest(tampered); err == nil {
		t.Fatal("a manifest whose content no longer matches its digest was accepted")
	}
}

func TestParseManifestRefusesWhatTheFenceDidNotWrite(t *testing.T) {
	good := sampleManifest(t)
	cases := map[string]func(*Manifest){
		"a floor below 1":           func(m *Manifest) { m.Floors[0].Revision = 0 },
		"a floor above 2^53-1":      func(m *Manifest) { m.Floors[0].Revision = maxRevision + 1 },
		"a floor listed twice":      func(m *Manifest) { m.Floors = append(m.Floors, m.Floors[0]) },
		"an unnamed floor":          func(m *Manifest) { m.Floors[0].AggregateID = "" },
		"an unknown section":        func(m *Manifest) { m.Sections = append(m.Sections, Section{Name: "public.secrets", Rows: []Row{}}) },
		"a section twice":           func(m *Manifest) { m.Sections = append(m.Sections, m.Sections[0]) },
		"an extra column":           func(m *Manifest) { m.Sections[0].Rows = []Row{{"id": "x", "token_hash": "abc"}} },
		"a missing column":          func(m *Manifest) { m.Sections[0].Rows = []Row{{"id": "x"}} },
		"a value of another shape":  func(m *Manifest) { m.Sections[0].Rows = rowWithValue(m.Sections[0].Name, []any{"x"}) },
		"another version":           func(m *Manifest) { m.Version = ManifestVersion + 1 },
		"a source timeline below 1": func(m *Manifest) { m.SourceTimeline = 0 },
	}
	for name, mutate := range cases {
		m := good
		m.Floors = append([]Floor(nil), good.Floors...)
		m.Sections = append([]Section(nil), good.Sections...)
		mutate(&m)
		if err := m.Seal(); err != nil {
			t.Fatalf("%s: seal: %v", name, err)
		}
		raw, err := m.Marshal()
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		if _, err := ParseManifest(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func rowWithValue(section string, v any) []Row {
	def, _ := sectionByName(section)
	row := Row{}
	for _, c := range def.columnNames() {
		row[c] = v
	}
	return []Row{row}
}

func TestParseManifestRefusesTrailingContentAndUnknownMembers(t *testing.T) {
	m := sampleManifest(t)
	raw, _ := m.Marshal()
	if _, err := ParseManifest(append(append([]byte(nil), raw...), []byte(`{}`)...)); err == nil {
		t.Error("trailing content accepted")
	}
	version := fmt.Sprintf(`"version": %d,`, ManifestVersion)
	extra := strings.Replace(string(raw), version, version+` "note": "x",`, 1)
	if extra == string(raw) {
		t.Fatal("the version member was not found")
	}
	if _, err := ParseManifest([]byte(extra)); err == nil {
		t.Error("an unknown member was accepted")
	}
}

func TestEveryCarriedColumnIsAnIdentifierTimestampStatusOrCounter(t *testing.T) {
	forbidden := []string{"hash", "secret", "password_hash", "token_hash", "encrypted", "jti", "successor"}
	for _, s := range sections {
		for _, c := range s.columnNames() {
			for _, f := range forbidden {
				if f == "hash" && s.name == "auth.saml_assertion_uses" && c == "assertion_id_hash" {
					continue
				}
				if strings.Contains(c, f) {
					t.Errorf("section %s carries column %q, which names credential material", s.name, c)
				}
			}
		}
	}
}
