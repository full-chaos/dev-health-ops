package providersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestFetchGitHubFileContentsSkipsABlobWithANulLikeABinaryOne(t *testing.T) {
	fake := newFilesChunkFake(t, 3, 100, 0)
	paths := fake.trees["tree-1"]
	fake.texts[paths[0]] = "package a\x00\n"
	fake.texts[paths[1]] = "package b\n"
	sizes := map[string]*int{}
	for _, path := range paths {
		size := fake.sizes[path]
		sizes[path] = &size
	}
	contents, err := fetchGitHubFileContents(t.Context(), fake.client(t), "acme", "api", "tree-1", paths, sizes)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := contents[paths[0]]; present {
		t.Fatalf("a blob with a NUL byte was kept: %q", contents[paths[0]])
	}
	if contents[paths[1]] != "package b\n" || len(contents) != 2 {
		t.Fatalf("contents = %d entries, want the two NUL-free blobs", len(contents))
	}
}

func TestGitHubFilesChunkCountsANulBlobAsUnavailableAndEmitsNoNul(t *testing.T) {
	fake := newFilesChunkFake(t, 3, 100, 0)
	paths := fake.trees["tree-1"]
	fake.texts[paths[0]] = "x\x00y"
	run, err := runFilesChunks(t, fake, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range run.rows {
		encoded, marshalErr := json.Marshal(row)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if strings.Contains(string(encoded), `\u0000`) {
			t.Fatalf("row %s carries a NUL escape that jsonb refuses", row.Path)
		}
		if row.Path == paths[0] && row.Contents != nil {
			t.Fatalf("NUL blob contents = %q, want none", *row.Contents)
		}
	}
	final := run.emissions[len(run.emissions)-1].Batch.Result
	if final["files_with_contents"] != 2 || final["contents_unavailable"] != 1 {
		t.Fatalf("result = %v, want 2 with contents and 1 unavailable", final)
	}
}

func TestGitFileRowsNeverCarryNulContents(t *testing.T) {
	claim := Claim{OrgID: "org-1"}
	at := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	repoID := strings.Repeat("a", 36)
	if row := newGitHubFileRow(claim, repoID, "a.go", "x\x00y", at); row.Contents != nil {
		t.Fatalf("github row contents = %q, want none", *row.Contents)
	}
	if row := newGitLabFileRow(claim, repoID, "a.go", map[string]string{"a.go": "x\x00y"}, at); row.Contents != nil {
		t.Fatalf("gitlab row contents = %q, want none", *row.Contents)
	}
	if row := newGitHubFileRow(claim, repoID, "a.go", "ok\n", at); row.Contents == nil || *row.Contents != "ok\n" {
		t.Fatal("a NUL-free body must be kept unchanged")
	}
	if row := newGitLabFileRow(claim, repoID, "a.go", map[string]string{"a.go": "ok\n"}, at); row.Contents == nil || *row.Contents != "ok\n" {
		t.Fatal("a NUL-free gitlab body must be kept unchanged")
	}
}

func TestChunkStoreDataErrorIsExactlyUnsupportedUnicodeEscape(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"22P05", NewChunkStoreError(&pgconn.PgError{Code: "22P05"}), true},
		{"22P05 wrapped", fmt.Errorf("unit: %w", NewChunkStoreError(fmt.Errorf("exec: %w", &pgconn.PgError{Code: "22P05"}))), true},
		{"serialization failure", NewChunkStoreError(&pgconn.PgError{Code: "40001"}), false},
		{"other data exception", NewChunkStoreError(&pgconn.PgError{Code: "22P02"}), false},
		{"plain cause", NewChunkStoreError(errors.New("db")), false},
		{"bare conflict", ErrChunkCheckpointConflict, false},
		{"nil", nil, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := IsChunkStoreDataError(testCase.err); got != testCase.want {
				t.Fatalf("IsChunkStoreDataError = %v, want %v", got, testCase.want)
			}
		})
	}
}
