//go:build integration

package streamhandlers

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// One push batch holds a team and two identities. One identity names the team
// of the batch and a team the source never pushes. On the schema of the
// migration chain, after the batch is written:
//
//   - the team of the batch has its row, so only the team that was never
//     pushed is counted as a team id with no team row;
//   - the identities are stored as they were pushed, with both team ids;
//   - no team row is made up for the team that was never pushed;
//   - the sink says it in ONE WARN line that holds counts and no id.
//
// The ORDER of the writes of a batch is held by the unit test of the sink
// (TestThePushedTeamsOfABatchAreWrittenBeforeTheIdentitiesThatNameThem): this
// test reads the state after the whole batch.
func TestAPushBatchCountsTheIdentityTeamIDsWithNoTeamRowAndMakesNoTeamUp(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	const org = "org-team-row-first"
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	record := func(index int, kind string, payload map[string]any) externalSinkRecord {
		t.Helper()
		if err := validateExternalRecord(kind, payload); err != nil {
			t.Fatalf("record %d is not a valid %s: %v", index, kind, err)
		}
		return externalSinkRecord{Index: index, Kind: kind, ExternalID: kind + "-" + uuid.NewString(), Payload: payload}
	}
	if _, err := sink.Write(context.WithoutCancel(ctx), externalSinkBatch{
		Pointer: externalPointer{
			IngestionID: uuid.New(), OrgID: org, SourceSystem: "github",
			SourceInstance: "acme/api", SchemaVersion: externalSchemaVersion,
		},
		SourceID: uuid.New(),
		Records: []externalSinkRecord{
			record(0, "identity.v1", map[string]any{"canonicalId": "ada", "teamIds": []any{"platform", "never-pushed"}, "updatedAt": "2026-07-23T11:00:00Z"}),
			record(1, "identity.v1", map[string]any{"canonicalId": "bob", "teamIds": []any{"platform"}, "updatedAt": "2026-07-23T11:00:00Z"}),
			record(2, "team.v1", map[string]any{"id": "platform", "name": "Platform", "updatedAt": "2026-07-23T11:00:00Z"}),
		},
	}); err != nil {
		t.Fatal(err)
	}

	var teams []string
	if err := conn.QueryRow(ctx, `SELECT arraySort(groupUniqArray(id)) FROM teams WHERE org_id = ?`, org).Scan(&teams); err != nil {
		t.Fatal(err)
	}
	if len(teams) != 1 || teams[0] != "gh:platform" {
		t.Errorf("team rows of the organization = %v, want only the pushed team gh:platform", teams)
	}
	var named []string
	if err := conn.QueryRow(ctx, `SELECT arraySort(groupUniqArray(arrayJoin(team_ids))) FROM identities FINAL WHERE org_id = ?`, org).Scan(&named); err != nil {
		t.Fatal(err)
	}
	if len(named) != 2 || named[0] != "gh:never-pushed" || named[1] != "gh:platform" {
		t.Errorf("team ids of the stored identities = %v, want both as pushed", named)
	}

	if lines := strings.Count(logs.String(), externalIdentityTeamsWithNoRowEvent); lines != 1 {
		t.Fatalf("%d WARN lines of the event, want 1:\n%s", lines, logs.String())
	}
	for _, want := range []string{"level=WARN", "identities=2", "team_ids_named=2", "team_ids_with_no_team_row=1", "organization_id=" + org} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the line lacks %q:\n%s", want, logs.String())
		}
	}
	for _, private := range []string{"never-pushed", "platform", "ada", "bob"} {
		if strings.Contains(logs.String(), private) {
			t.Errorf("the line holds the id %q:\n%s", private, logs.String())
		}
	}
}
