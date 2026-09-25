package atlassian

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

func ParseRetryAfterAny(value string, now time.Time) (time.Time, string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, "", errors.New("retry-after header missing or empty")
	}
	if seconds, err := strconv.Atoi(trimmed); err == nil {
		if seconds < 0 {
			seconds = 0
		}
		return now.Add(time.Duration(seconds) * time.Second).UTC(), "delta-seconds", nil
	}
	return ParseRetryAfter(trimmed)
}
