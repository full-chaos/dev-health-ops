package atlassian

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

func ParseRetryAfter(value string) (time.Time, string, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, "", errors.New("retry-after header missing or empty")
	}

	cleaned := strings.TrimSpace(value)
	if strings.HasSuffix(cleaned, "Z") {
		cleaned = strings.TrimSuffix(cleaned, "Z") + "+00:00"
	}
	layouts := []struct {
		layout string
		label  string
	}{
		{time.RFC3339, "rfc3339"},
		{"2006-01-02T15:04Z07:00", "rfc3339-minute"},
		{time.RFC3339Nano, "rfc3339-nano"},
	}
	for _, candidate := range layouts {
		if parsed, err := time.Parse(candidate.layout, cleaned); err == nil {
			return parsed.UTC(), candidate.label, nil
		}
	}
	if parsed, err := http.ParseTime(value); err == nil {
		return parsed.UTC(), "http-date", nil
	}
	return time.Time{}, "", errors.New("unable to parse Retry-After header")
}
