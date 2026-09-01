package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestDocument_KnownDigests pins the CHAOS-4696 executed evidence values
// so a future change to this algorithm is a loud, deliberate diff here
// rather than a silent drift observed only via registrydump's output.
func TestDocument_KnownDigests(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{
			name: "featureFlags web source text (unprinted, pre-fix)",
			text: `query FeatureFlagRegistry($orgId: String!, $provider: String, $project: String, $includeArchived: Boolean, $limit: Int!) {
  featureFlags(orgId: $orgId, provider: $provider, project: $project, includeArchived: $includeArchived, limit: $limit) {
    flags {
      flagId
      flagKey
      provider
      projectKey
      flagType
      createdAt
      archivedAt
    }
    totalCount
    degradedReason
  }
}`,
			want: "555bc9f82339b8321f309a26d310c4a7e41e79b9b155da41f62d8e97b50da8b7",
		},
		{
			name: "trailing/leading whitespace is trimmed",
			text: "\n\n  hello world  \n\n",
			want: Document("hello world"),
		},
		{
			name: "empty string",
			text: "",
			want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Document(tc.text)
			if got != tc.want {
				t.Fatalf("Document(%q) = %s, want %s", tc.text, got, tc.want)
			}
		})
	}
}

// TestSchema_KnownDigests pins the algorithm on synthetic bytes (never
// the real schema.graphql content, which legitimately changes as the
// schema evolves -- see query_route_schemadigest_test.go for a test
// against the real embedded SDL). This test exists to pin the ALGORITHM:
// sha256 of the RAW bytes, "sha256:" prefix, hex-encoded, and explicitly
// NOT trimmed -- a leading/trailing-whitespace difference must change
// the digest, unlike Document's TrimSpace behavior, because
// schema.graphql is a checked-in contract file where every byte
// (including a trailing newline) is part of what test_schema_sdl_pinned.py
// already pins on the Python side.
func TestSchema_KnownDigests(t *testing.T) {
	cases := []struct {
		name string
		sdl  []byte
		want string
	}{
		{
			name: "empty bytes",
			sdl:  []byte{},
			want: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "trivial SDL",
			sdl:  []byte("type Query { hello: String }\n"),
			want: "sha256:" + shaHex("type Query { hello: String }\n"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Schema(tc.sdl)
			if got != tc.want {
				t.Fatalf("Schema(%q) = %s, want %s", tc.sdl, got, tc.want)
			}
		})
	}
}

// TestSchema_UnlikeDocument_DoesNotTrim is the specific behavioral
// contract that distinguishes Schema from Document: whitespace is
// significant.
func TestSchema_UnlikeDocument_DoesNotTrim(t *testing.T) {
	trimmed := Schema([]byte("hello"))
	padded := Schema([]byte("  hello  \n"))
	if trimmed == padded {
		t.Fatalf("Schema(%q) == Schema(%q) = %s -- Schema must NOT trim, unlike Document", "hello", "  hello  \n", trimmed)
	}
}

// TestSchema_DeterministicAndSensitiveToContent proves Schema is a pure
// function of its input (same bytes in, same digest out; different bytes
// in, different digest out) -- the property GO_API_SCHEMA_DIGEST
// verification depends on.
func TestSchema_DeterministicAndSensitiveToContent(t *testing.T) {
	a := Schema([]byte("type Query { a: String }"))
	aAgain := Schema([]byte("type Query { a: String }"))
	b := Schema([]byte("type Query { b: String }"))

	if a != aAgain {
		t.Fatalf("Schema is not deterministic: %s != %s for identical input", a, aAgain)
	}
	if a == b {
		t.Fatalf("Schema(a) == Schema(b) = %s for DIFFERENT input -- digest is not sensitive to content", a)
	}
}

func shaHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
