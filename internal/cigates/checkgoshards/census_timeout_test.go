//go:build !race

package checkgoshards_test

import (
	"strings"
	"testing"
)

// CHAOS-11762 interim, widened by CHAOS-11787: a go test call of a test shard gets -timeout 20m when its package list holds a
// package under internal/cigates/ (checkgo ran 485-757 s on hosted merge_group); every other go test call keeps 10m. An inherited
// GO_TEST_JSON_TIMEOUT changes nothing: the caller's environment never picks a timeout. Which shard holds checkgo is the weights'
// decision (ci/go_test_weights.tsv), so the test finds it in the plan output and does not name a shard number.

const checkgoTimeoutPackage = "internal/cigates/checkgo"

func TestTheCensusCallOfAShardTakes20mOnlyWhenItHoldsCheckgo(t *testing.T) {
	t.Parallel()
	for _, ambient := range []string{"", "GO_TEST_JSON_TIMEOUT=1s"} {
		shardsWithCheckgo := []int{}
		for shard := 1; shard <= 3; shard++ {
			var extra []string
			if ambient != "" {
				extra = []string{ambient}
			}
			res, lines := runShardWithFakeGo(t, shard, "", extra...)
			if res.code != 0 {
				t.Fatalf("test shard %d/3 (%q) exited %d: %s", shard, ambient, res.code, failureTail(res))
			}
			withCheckgo := 0
			for _, l := range lines {
				if strings.Contains(l, "-tags=integration") || !strings.Contains(l, " -timeout ") {
					continue
				}
				if strings.Contains(l, "1s") && strings.Contains(l, " -timeout 1s") {
					t.Errorf("shard %d (%q): an inherited value reached a go test call: %s", shard, ambient, l)
				}
				want := " -timeout 10m "
				for _, f := range strings.Fields(l) {
					if strings.HasPrefix(f, cigatesPrefix) {
						want = " -timeout 20m "
					}
				}
				if hasField(l, fullPkg(checkgoTimeoutPackage)) {
					withCheckgo++
				}
				if !strings.Contains(l, want) {
					t.Errorf("shard %d (%q): want%s in: %s", shard, ambient, want, l)
				}
			}
			if withCheckgo > 1 {
				t.Errorf("shard %d (%q): %d go test calls name %s, want at most one", shard, ambient, withCheckgo, checkgoTimeoutPackage)
			}
			if withCheckgo == 1 {
				shardsWithCheckgo = append(shardsWithCheckgo, shard)
			}
		}
		if len(shardsWithCheckgo) != 1 {
			t.Errorf("(%q): shards %v hold %s, want exactly one shard (the weights decide which)", ambient, shardsWithCheckgo, checkgoTimeoutPackage)
		}
	}
}
