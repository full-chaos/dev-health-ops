package providersync

import "testing"

func TestTestOpsRoutesOptIntoBoundedChunkPolicy(t *testing.T) {
	for _, route := range [][2]string{
		{"github", "cicd"}, {"github", "tests"},
		{"gitlab", "cicd"}, {"gitlab", "tests"},
	} {
		descriptor, ok := Descriptor(route[0], route[1])
		if !ok || !descriptor.RouteReady || !descriptor.Chunked {
			t.Fatalf("%s/%s descriptor=%+v ok=%t", route[0], route[1], descriptor, ok)
		}
		if descriptor.ChunkPolicy != DefaultChunkPolicy() {
			t.Fatalf("%s/%s policy=%+v want=%+v", route[0], route[1], descriptor.ChunkPolicy, DefaultChunkPolicy())
		}
	}
	for _, route := range [][2]string{
		{"github", "commits"}, {"gitlab", "commits"}, {"launchdarkly", "feature-flags"},
	} {
		descriptor, ok := Descriptor(route[0], route[1])
		if !ok || descriptor.Chunked || descriptor.ChunkPolicy != (ChunkPolicy{}) {
			t.Fatalf("legacy %s/%s descriptor=%+v ok=%t", route[0], route[1], descriptor, ok)
		}
	}
}
