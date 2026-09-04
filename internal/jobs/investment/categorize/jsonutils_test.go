package categorize

import "testing"

func TestValidateJSONOrEmpty(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"whitespace only", "   \n\t", ""},
		{"invalid json", "not json", ""},
		{"truncated json", `{"a": 1`, ""},
		{"compacts valid json", `{"a":   1,   "b": [1,2,3]}`, `{"a":1,"b":[1,2,3]}`},
		{"array input", `[1, 2, 3]`, `[1,2,3]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validateJSONOrEmpty(tc.input); got != tc.want {
				t.Errorf("validateJSONOrEmpty(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
