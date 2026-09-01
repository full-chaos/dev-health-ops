package digest

import "testing"

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
