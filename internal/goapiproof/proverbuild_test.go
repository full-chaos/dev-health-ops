package goapiproof

import (
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

func TestProverBuild(t *testing.T) {
	const candidate = "94527a87f9aa9f7e4a1d6956d62b78093038ca0b"
	for _, tc := range []struct {
		name      string
		prover    version.Info
		candidate string
		allow     bool
		wantSkew  bool
		wantErr   bool
	}{
		{"same commit", version.Info{Commit: candidate}, candidate, false, false, false},
		{"same commit, override set", version.Info{Commit: candidate}, candidate, true, false, false},
		{"different commit", version.Info{Commit: "a38c5bb70bc926e10059f8b13d63d098d6756ba5"}, candidate, false, true, true},
		{"different commit, override set", version.Info{Commit: "a38c5bb70bc926e10059f8b13d63d098d6756ba5"}, candidate, true, true, false},
		{"short prefix of the candidate", version.Info{Commit: candidate[:7]}, candidate, false, true, true},
		{"upper-case spelling", version.Info{Commit: strings.ToUpper(candidate)}, candidate, false, true, true},
		{"surrounding whitespace", version.Info{Commit: " " + candidate + "\n"}, candidate, false, false, false},
		{"unstamped prover", version.Info{Commit: "unknown"}, candidate, false, true, true},
		{"unstamped prover, override set", version.Info{Commit: "unknown"}, candidate, true, true, false},
		{"empty prover commit", version.Info{Commit: ""}, candidate, false, true, true},
		{"unstamped prover against an unknown candidate", version.Info{Commit: "unknown"}, "unknown", false, true, true},
		{"empty prover against an empty candidate", version.Info{Commit: ""}, "", false, true, true},
		{"modified prover tree at the candidate commit", version.Info{Commit: candidate, Modified: true}, candidate, false, true, true},
		{"modified prover tree, override set", version.Info{Commit: candidate, Modified: true}, candidate, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builds := NewProverBuild(tc.prover, tc.candidate, tc.allow)
			if builds.Skew != tc.wantSkew {
				t.Fatalf("Skew = %t, want %t (%+v)", builds.Skew, tc.wantSkew, builds)
			}
			if builds.SkewAllowed != (tc.wantSkew && tc.allow) {
				t.Fatalf("SkewAllowed = %t, want %t", builds.SkewAllowed, tc.wantSkew && tc.allow)
			}
			err := builds.Check("-allow-prover-build-skew")
			if (err != nil) != tc.wantErr {
				t.Fatalf("Check() = %v, want error %t", err, tc.wantErr)
			}
			if err != nil {
				if !errors.Is(err, ErrProverBuildSkew) {
					t.Fatalf("Check() = %v, want ErrProverBuildSkew", err)
				}
				for _, part := range []string{builds.Prover, tc.candidate, "-allow-prover-build-skew"} {
					if !strings.Contains(err.Error(), part) {
						t.Fatalf("Check() = %q, want it to name %q", err, part)
					}
				}
			}
		})
	}
}

func TestProverBuildLine(t *testing.T) {
	for _, tc := range []struct {
		name   string
		builds ProverBuild
		want   string
	}{
		{"same", NewProverBuild(version.Info{Commit: "abc"}, "abc", false), "prover_build=abc prover_build_modified=false candidate_build=abc prover_build_skew=false prover_build_skew_allowed=false"},
		{"skew refused", NewProverBuild(version.Info{Commit: "abc"}, "def", false), "prover_build=abc prover_build_modified=false candidate_build=def prover_build_skew=true prover_build_skew_allowed=false"},
		{"skew allowed", NewProverBuild(version.Info{Commit: "abc"}, "def", true), "prover_build=abc prover_build_modified=false candidate_build=def prover_build_skew=true prover_build_skew_allowed=true"},
		{"modified", NewProverBuild(version.Info{Commit: "abc", Modified: true}, "abc", true), "prover_build=abc prover_build_modified=true candidate_build=abc prover_build_skew=true prover_build_skew_allowed=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.builds.Line(); got != tc.want {
				t.Fatalf("Line() = %q, want %q", got, tc.want)
			}
		})
	}
}
