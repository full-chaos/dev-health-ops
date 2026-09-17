package home

import (
	"encoding/json"
	"time"
)

// NaiveDateTime marshals like Pydantic's `datetime` field does when fed
// a bare `date` value (no tzinfo attached): "2006-01-02T15:04:05", with
// no offset/"Z" suffix. SparkPoint.ts (services/home.py's
// `SparkPoint(ts=row["day"], ...)`, row["day"] being a ClickHouse Date-
// typed column decoded as a plain Python `date`) round-trips through
// Pydantic's model_dump(mode="json") this exact way, confirmed against
// the captured golden fixture -- every OTHER datetime field in this
// response (last_ingested_at, health_state.as_of, events[].ts) is fed a
// genuinely tz-aware value and keeps its "Z" suffix; only this field's
// Python source is bare-date-shaped.
type NaiveDateTime time.Time

const naiveDateTimeLayout = "2006-01-02T15:04:05"

func (n NaiveDateTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(n).Format(naiveDateTimeLayout))
}

func (n *NaiveDateTime) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	t, err := time.Parse(naiveDateTimeLayout, s)
	if err != nil {
		return err
	}
	*n = NaiveDateTime(t)
	return nil
}
