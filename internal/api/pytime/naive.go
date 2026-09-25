package pytime

import (
	"encoding/json"
	"time"
)

// NaiveDateTime is a wire timestamp Python holds as a NAIVE datetime: a
// pydantic `datetime` field fed a value with no tzinfo (a `date` from a
// ClickHouse Date column, or a DateTime('UTC') read that clickhouse-connect
// returns naive). It marshals as pydantic-core does through Pydantic: ISO
// date and time, ".ffffff" only when the microseconds are non-zero, no zone.
// The struct field types of a response model use it in place of time.Time,
// whose encoding/json form always carries a "Z".
type NaiveDateTime time.Time

// NaiveDay is the midnight datetime pydantic makes of a date: the wall-clock
// year, month and day of at (in its own location, never converted), 00:00:00.
func NaiveDay(at time.Time) NaiveDateTime {
	year, month, day := at.Date()
	return NaiveDateTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}

// NaiveUTC is a UTC instant read as the naive wall clock Python holds.
func NaiveUTC(at time.Time) NaiveDateTime { return NaiveDateTime(at.UTC()) }

// MarshalJSON writes the Pydantic naive form of the UTC wall clock.
func (n NaiveDateTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(Pydantic(DateTime{Time: time.Time(n).UTC()}))
}

// UnmarshalJSON reads the form MarshalJSON writes (fixtures and tests).
func (n *NaiveDateTime) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	at, err := time.Parse("2006-01-02T15:04:05.999999", text)
	if err != nil {
		return err
	}
	*n = NaiveDateTime(at)
	return nil
}
