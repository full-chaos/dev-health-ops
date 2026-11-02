package contracts

import (
	"testing"
	"time"
)

func TestServiceAccountTokenLifetimeNeverPassesTheKeyExpiry(t *testing.T) {
	now := time.Unix(1760000000, 0).UTC()
	for name, c := range map[string]struct {
		keyExpiresAt time.Time
		want         time.Duration
	}{
		"key far away: the ceiling":                  {now.Add(90 * 24 * time.Hour), 300 * time.Second},
		"key exactly at the ceiling":                 {now.Add(300 * time.Second), 300 * time.Second},
		"key one second inside the ceiling":          {now.Add(299 * time.Second), 299 * time.Second},
		"key one minute away":                        {now.Add(time.Minute), time.Minute},
		"key one second away":                        {now.Add(time.Second), time.Second},
		"key under one second away":                  {now.Add(900 * time.Millisecond), 0},
		"key expired at now":                         {now, 0},
		"key expired in the past":                    {now.Add(-time.Hour), 0},
		"no key expiry known: fail closed, no token": {time.Time{}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			got := ServiceAccountTokenLifetime(now, c.keyExpiresAt)
			if got != c.want {
				t.Fatalf("lifetime = %s, want %s", got, c.want)
			}
			if got > 0 && now.Add(got).After(c.keyExpiresAt) {
				t.Fatalf("a token issued now lives to %s, past the key expiry %s", now.Add(got), c.keyExpiresAt)
			}
			if got > ServiceAccountTokenLifetimeCeiling {
				t.Fatalf("lifetime %s is over the ceiling", got)
			}
		})
	}
}
