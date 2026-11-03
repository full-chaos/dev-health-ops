package restorefence

import "testing"

// Follow-up (round1128 P3): bytes after the manifest object must be refused, whatever they are, not only a trailing
// token that decodes.
func TestAManifestWithAnyTrailingBytesIsRefused(t *testing.T) {
	m := Manifest{Version: ManifestVersion, SourceTimeline: 2, Floors: []Floor{}, Sections: []Section{}}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	raw, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseManifest(append(append([]byte{}, raw...), '\n', ' ', '\t')); err != nil {
		t.Fatalf("trailing whitespace is fine: %v", err)
	}
	for name, tail := range map[string]string{"a token": " {}", "a stray brace": "}", "garbage": " garbage", "a NUL": "\x00", "a comma": ","} {
		if _, err := ParseManifest(append(append([]byte{}, raw...), tail...)); err == nil {
			t.Errorf("a manifest followed by %s was accepted", name)
		}
	}
}
