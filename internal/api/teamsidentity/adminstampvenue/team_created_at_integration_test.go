//go:build integration

package adminstampvenue

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// teamCreatedAtAssertions are the named created_at checks of the venue. The
// Python reference renders created_at as updated_at, so the clock requests
// (retired from the Python comparison, pinned with their instants blanked)
// cannot tell the two stamps apart; these checks read the Go answer itself.
//
//   - an admin update of a seeded team keeps the stored creation time: the
//     seed stamp of that team, strictly before the new updated_at;
//   - a team the admin creates has created_at == updated_at.
var teamCreatedAtAssertions = map[string]struct {
	seedStamp int // index into stamps; -1 = a team created in this run
}{
	"clock: POST over a seeded team":      {seedStamp: 0},
	"clock: PATCH a seeded team":          {seedStamp: 1},
	"clock: PATCH with nothing to change": {seedStamp: 2},
	"clock: patched team read back":       {seedStamp: 1},
	"clock: POST a new team":              {seedStamp: -1},
	"clock: written team read back":       {seedStamp: -1},
}

func checkTeamCreatedAt(t *testing.T, request venueoracle.Request, response venueoracle.Response) {
	t.Helper()
	want, named := teamCreatedAtAssertions[request.Name]
	if !named {
		return
	}
	var team struct {
		CreatedAt string `json:"created_at"`
		UpdatedAt string `json:"updated_at"`
	}
	if err := json.Unmarshal([]byte(response.Body), &team); err != nil {
		t.Errorf("%s: not a team: %v\n%s", request.Name, err, response.Body)
		return
	}
	created, err := parseStamp(team.CreatedAt)
	if err != nil {
		t.Errorf("%s: created_at %q: %v", request.Name, team.CreatedAt, err)
		return
	}
	updated, err := parseStamp(team.UpdatedAt)
	if err != nil {
		t.Errorf("%s: updated_at %q: %v", request.Name, team.UpdatedAt, err)
		return
	}
	if want.seedStamp < 0 {
		if !created.Equal(updated) {
			t.Errorf("%s: a new team has created_at %s != updated_at %s", request.Name, team.CreatedAt, team.UpdatedAt)
		}
		return
	}
	seeded, err := seedInstant(stamps[want.seedStamp])
	if err != nil {
		t.Fatal(err)
	}
	if !created.Equal(seeded) {
		t.Errorf("%s: created_at %s is not the seeded creation time %s", request.Name, team.CreatedAt, seeded.Format(time.RFC3339Nano))
	}
	if !created.Before(updated) {
		t.Errorf("%s: created_at %s is not before updated_at %s", request.Name, team.CreatedAt, team.UpdatedAt)
	}
}

// seedInstant is the instant a seeded stamp denotes on this venue's server:
// its wall clock in the server zone (UTC unless the zone test sets one).
func seedInstant(stamp string) (time.Time, error) {
	location := time.UTC
	if zone := os.Getenv(containers.ClickHouseTimezoneEnv); zone != "" {
		var err error
		if location, err = time.LoadLocation(zone); err != nil {
			return time.Time{}, err
		}
	}
	return time.ParseInLocation("2006-01-02 15:04:05.000000", stamp, location)
}

// parseStamp reads a rendered stamp: with an offset, or naive (UTC).
func parseStamp(text string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02T15:04:05.999999999Z07:00", "2006-01-02T15:04:05.999999999"} {
		if at, err := time.Parse(layout, text); err == nil {
			return at, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised stamp %q", strings.TrimSpace(text))
}
