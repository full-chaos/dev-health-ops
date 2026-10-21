package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The size model: one row's encoding differs from the first row's only in the
// digits of line_no (a GitHub blame row carries no line text; author, email and
// commit are constant across a range). The model must equal the real rows.
func TestGitHubBlameRangeEncodedBytesEqualsTheSumOfEveryRealRow(t *testing.T) {
	name, email := "Ada Lovelace-Byron", "ada@example.com"
	for _, tc := range []struct {
		name       string
		start, end uint32
		author     *string
		email      *string
	}{
		{"one to 100,000 crosses five powers of ten", 1, 100_000, &name, &email},
		{"around 1,000", 999, 1001, &name, &email},
		{"single line", 7, 7, &name, &email},
		{"unknown author defaults", 99, 101, nil, nil},
		{"top of uint32", 4_294_967_290, 4_294_967_295, &name, &email},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := nativeTestClaim("github", "blame")
			var blameRange gitHubBlameRange
			blameRange.StartingLine, blameRange.EndingLine = tc.start, tc.end
			blameRange.Commit.OID = "0123456789abcdef0123456789abcdef01234567"
			blameRange.Commit.Author.Name, blameRange.Commit.Author.Email = tc.author, tc.email
			now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
			var actual int64
			var first int
			for line := tc.start; ; line++ {
				encoded, err := json.Marshal(newGitHubBlameRow(claim, "c7198fbc-1945-3717-05d8-eb78866b4e79", "src/a.go", line, blameRange, now))
				if err != nil {
					t.Fatal(err)
				}
				if line == tc.start {
					first = len(encoded)
				}
				actual += int64(len(encoded))
				if line == tc.end {
					break
				}
			}
			if got := gitHubBlameRangeEncodedBytes(first, tc.start, tc.end); got != actual {
				t.Fatalf("model=%d actual=%d (first row %d bytes)", got, actual, first)
			}
		})
	}
}

// The payload bound is inclusive and exact for a range that crosses a digit
// boundary: the first-line estimate was 11 bytes short here, so a file whose real
// rows were above the bound was accepted.
func TestGitHubBlamePayloadBoundIsExactAcrossADigitBoundary(t *testing.T) {
	blameCaptureSlog(t)
	run := func(maxPayload int) (rows int, retryable any) {
		client := gitHubRepositoryClient(t, fakehttp.Client(gitHubFixedRangeDoer{
			t: t, ranges: `[{"startingLine":99990,"endingLine":100010,"commit":{"oid":"abc"}}]`,
		}), "https://api.github.com")
		limits := defaultGitHubBlameLimits()
		limits.maxPayloadBytes = maxPayload
		claim := nativeTestClaim("github", "blame")
		batch, err := collectGitHubBlame(
			context.Background(), claim, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
			staticGitHubBlameCoverage{}, nil, gitHubBlameMaxFiles, false, "", limits,
		)
		if err != nil {
			t.Fatal(err)
		}
		return len(batch.Effects[1].Rows), batch.Result["retryable_path_failures"]
	}
	rows, _ := run(1 << 30)
	var actual int
	{
		client := gitHubRepositoryClient(t, fakehttp.Client(gitHubFixedRangeDoer{
			t: t, ranges: `[{"startingLine":99990,"endingLine":100010,"commit":{"oid":"abc"}}]`,
		}), "https://api.github.com")
		batch, err := collectGitHubBlame(
			context.Background(), nativeTestClaim("github", "blame"), client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
			staticGitHubBlameCoverage{}, nil, gitHubBlameMaxFiles, false, "", defaultGitHubBlameLimits(),
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range batch.Effects[1].Rows {
			actual += len(row)
		}
	}
	if rows != 21 || actual == 0 {
		t.Fatalf("rows=%d actual=%d", rows, actual)
	}
	if got, _ := run(actual); got != 21 {
		t.Fatalf("at the exact bound (%d bytes) rows=%d, want the file accepted", actual, got)
	}
	got, retryable := run(actual - 1)
	if got != 0 || fmt.Sprint(retryable) != "1" {
		t.Fatalf("one byte under the bound: rows=%d retryable=%v, want the file refused", got, retryable)
	}
}
