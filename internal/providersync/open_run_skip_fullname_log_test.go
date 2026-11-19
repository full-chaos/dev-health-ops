package providersync

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// A repository full_name is listing text. The open-run skip WARN carries it as
// a bounded field with control and format characters escaped.
func TestOpenRunSkipWarnBoundsAndEscapesListingFullName(t *testing.T) {
	hostile := "acme/" + strings.Repeat("x", 5000) + "\n\x1b[31m‮​"
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&output, slog.LevelInfo))
	defer slog.SetDefault(previous)
	cursor, err := encodeGitHubTestsChunkCursor(githubTestsChunkCursor{
		Phase: "done", OpenSkipEvent: &githubTestsOpenSkipEvent{RunID: "42", Reason: "run_read_failed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cursor, `"ose"`) {
		t.Fatal("cursor carries no open run skip")
	}
	claim := Claim{Provider: "github", Dataset: "tests", ID: "unit-1", SourceExternalID: hostile}
	GitHubTestsRouteHandler{}.ObserveUnconfirmedChunk(claim, PreparedProviderChunk{CursorAfter: cursor})

	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &line); err != nil {
		t.Fatalf("log line is not one JSON object: %v: %q", err, output.String())
	}
	repository, _ := line["repository"].(string)
	if repository == "" || utf8.RuneCountInString(repository) > 128+len("...[cut]") || !strings.HasSuffix(repository, "...[cut]") {
		t.Fatalf("repository field not bounded with the cut marker: %q", repository)
	}
	if strings.ContainsAny(repository, "\n\x1b‮​") {
		t.Fatalf("repository field kept a raw control or format character: %q", repository)
	}
}
