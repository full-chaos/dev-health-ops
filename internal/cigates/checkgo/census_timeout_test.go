//go:build !race

package checkgo_test

import (
	"strings"
	"testing"
)

// CHAOS-11762 interim: the census call of a test shard gets -timeout 20m only when its package list holds
// internal/cigates/checkgo (485-562 s on hosted merge_group); every other go test call keeps 10m. An inherited
// GO_TEST_JSON_TIMEOUT changes nothing: the caller's environment never picks a timeout.

const checkgoTimeoutPackage = "internal/cigates/checkgo"

func TestTheCensusCallOfAShardTakes20mOnlyWhenItHoldsCheckgo(t *testing.T) {
	t.Parallel()
	for _, ambient := range []string{"", "GO_TEST_JSON_TIMEOUT=1s"} {
		for shard := 1; shard <= 3; shard++ {
			var extra []string
			if ambient != "" {
				extra = []string{ambient}
			}
			res, lines := runShardWithFakeGo(t, shard, "", extra...)
			if res.code != 0 {
				t.Fatalf("test shard %d/3 (%q) exited %d: %s", shard, ambient, res.code, tail(res.stderr+res.stdout, 2000))
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
				if hasField(l, fullPkg(checkgoTimeoutPackage)) {
					withCheckgo++
					want = " -timeout 20m "
				}
				if !strings.Contains(l, want) {
					t.Errorf("shard %d (%q): want%s in: %s", shard, ambient, want, l)
				}
			}
			if (shard == 3) != (withCheckgo == 1) || withCheckgo > 1 {
				t.Errorf("shard %d (%q): %d go test call(s) name %s, want exactly one in shard 3 and none elsewhere", shard, ambient, withCheckgo, checkgoTimeoutPackage)
			}
		}
	}
}
