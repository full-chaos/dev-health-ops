//go:build integration

package llmorgsettings

import (
	"context"
	"testing"
)

// A saved BYO config of ANY purpose makes the org a BYO org: the default
// provider row, or the provider row of any purpose override. A provider of
// auto/mock/none is no BYO config.
func TestHasSavedBYOCountsTheDefaultAndEveryPurposeProvider(t *testing.T) {
	ctx := context.Background()
	_, pool := newTestStore(t)
	for _, tc := range []struct {
		name string
		rows map[string]string
		want bool
	}{
		{name: "default provider only", rows: map[string]string{"provider": "anthropic"}, want: true},
		{name: "investigate purpose provider only", rows: map[string]string{PurposeKey(PurposeInvestigate, "provider"): "anthropic"}, want: true},
		{name: "categorize purpose provider only", rows: map[string]string{PurposeKey(PurposeCategorize, "provider"): "openai"}, want: true},
		{name: "explain purpose provider only", rows: map[string]string{PurposeKey(PurposeExplain, "provider"): "openai"}, want: true},
		{name: "default provider auto", rows: map[string]string{"provider": "auto"}, want: false},
		{name: "purpose provider none", rows: map[string]string{PurposeKey(PurposeCategorize, "provider"): "none"}, want: false},
		{name: "a model row only", rows: map[string]string{"model": "some-model"}, want: false},
		{name: "nothing stored", rows: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orgID := seedOrg(ctx, t, pool, "team")
			for key, value := range tc.rows {
				insertSetting(ctx, t, pool, orgID, key, value)
			}
			got, err := HasSavedBYO(ctx, pool, orgID.String())
			if err != nil || got != tc.want {
				t.Fatalf("HasSavedBYO = %t, %v; want %t", got, err, tc.want)
			}
		})
	}
}
