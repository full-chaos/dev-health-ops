package featureflags

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded golden is declared. A CLAIM path is one whose
// change makes TestFeatureFlagResolversMatchTheFrozenGolden fail; the other two are named with
// their reason. The mutation run in the pull request that added this file backs the claims.

var goldenClaims = []string{
	"[].args.environment",
	"[].args.flag_key",
	"[].args.include_archived",
	"[].args.limit",
	"[].args.project",
	"[].args.provider",
	"[].errs[]",
	"[].errs[].code",
	"[].kind",
	"[].python_params[].environment",
	"[].python_params[].flag_key",
	"[].python_params[].limit",
	"[].python_params[].org_id",
	"[].python_params[].project",
	"[].python_params[].provider",
	"[].python_queries[]",
	"[].raises",
	"[].result",
	"[].result.degraded_reason",
	"[].result.items[].actor_type",
	"[].result.items[].archived_at",
	"[].result.items[].created_at",
	"[].result.items[].environment",
	"[].result.items[].event_ts",
	"[].result.items[].event_type",
	"[].result.items[].flag_id",
	"[].result.items[].flag_key",
	"[].result.items[].flag_type",
	"[].result.items[].next_state",
	"[].result.items[].prev_state",
	"[].result.items[].project_key",
	"[].result.items[].provider",
	"[].result.total_count",
	"[].rows[].actor_type",
	"[].rows[].archived_at",
	"[].rows[].created_at",
	"[].rows[].environment",
	"[].rows[].event_ts",
	"[].rows[].event_type",
	"[].rows[].flag_key",
	"[].rows[].flag_type",
	"[].rows[].next_state",
	"[].rows[].prev_state",
	"[].rows[].project_key",
	"[].rows[].provider",
	"[].total",
}

var goldenNotClaims = map[string]string{
	"[].name":        "a case label (subtest name only)",
	"[].errs[].text": "consumed input, not compared: the resolver matches it with a regex for code 60, so an appended suffix changes nothing; TestRecordedErrorTextDecidesDegradedOrRaised blanks it and requires the answer to change",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile("testdata/feature_flags_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, goldenClaims, goldenNotClaims)
}

// TestRecordedErrorTextDecidesDegradedOrRaised proves the recorded ClickHouse message is read:
// for each case where Python degraded on a code-60 error, an empty message must make the Go
// resolver raise instead. Without it a golden whose error texts were all replaced would pass.
func TestRecordedErrorTextDecidesDegradedOrRaised(t *testing.T) {
	raw, err := os.ReadFile("testdata/feature_flags_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	probed := 0
	for _, tc := range cases {
		degraded := false
		blanked := make([]*goldenErr, len(tc.Errs))
		for i, e := range tc.Errs {
			if e == nil {
				continue
			}
			copied := *e
			if e.Code == 60 && tc.Result != nil && tc.Result.DegradedReason != nil {
				copied.Text = ""
				degraded = true
			}
			blanked[i] = &copied
		}
		if !degraded {
			continue
		}
		probed++
		client := &goldenClient{rows: tc.Rows, total: tc.Total, errs: blanked, kind: tc.Kind}
		var gotErr error
		if tc.Kind == "flags" {
			_, gotErr = Resolve(context.Background(), client, "org-1", strPtr(tc.Args, "provider"), strPtr(tc.Args, "project"), tc.Args["include_archived"].(bool), int(tc.Args["limit"].(float64)))
		} else {
			_, gotErr = ResolveEvents(context.Background(), client, "org-1", strPtr(tc.Args, "flag_key"), strPtr(tc.Args, "environment"), int(tc.Args["limit"].(float64)))
		}
		if gotErr == nil {
			t.Errorf("case %q: Python degraded on this recorded error text, and the Go resolver still degrades with the text blanked: the text is not read", tc.Name)
		}
	}
	if probed == 0 {
		t.Fatal("no degraded code-60 case in the golden: the probe proved nothing")
	}
}
