package home

import (
	"encoding/json"
	"fmt"
	"time"
)

// The leaves Python holds as a NAIVE datetime (SparkPoint.ts, a ClickHouse
// Date read as a `date`; Freshness.last_ingested_at and HealthState.as_of, a
// DateTime('UTC') read that clickhouse-connect returns naive) are
// pytime.NaiveDateTime, the one implementation of that wire form.

// MicroDateTime marshals like Pydantic's `datetime` field does when fed
// a tz-aware (UTC) value: "2006-01-02T15:04:05Z" when the microsecond
// component is zero, else "2006-01-02T15:04:05.dddddd" plus "Z", the
// fraction ALWAYS exactly six digits, zero-padded, never trimmed. Two
// leaves carry this type:
//   - Freshness.latest_successful_sync_at (api/queries/sync_freshness.py's
//     fetch_latest_successful_sync_at, which always forces UTC tzinfo
//     onto a Postgres timestamp column, microsecond precision) -- e.g.
//     ".066950Z", the trailing zero kept. Go's own default time.Time
//     marshaling trims a trailing zero digit, which this type does not.
//   - EventItem.ts (services/home.py's `EventItem(ts=datetime.now(
//     timezone.utc), ...)`, a per-request wall-clock read): microsecond
//     resolution on the Python side, so the six-digit width is fixed
//     even though the VALUE itself is never expected to compare equal
//     between planes.
type MicroDateTime time.Time

const microDateTimeSecondsLayout = "2006-01-02T15:04:05"

func (m MicroDateTime) MarshalJSON() ([]byte, error) {
	t := time.Time(m).UTC()
	s := t.Format(microDateTimeSecondsLayout)
	if micros := t.Nanosecond() / 1000; micros != 0 {
		s += fmt.Sprintf(".%06d", micros)
	}
	return json.Marshal(s + "Z")
}

func (m *MicroDateTime) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	*m = MicroDateTime(t)
	return nil
}
