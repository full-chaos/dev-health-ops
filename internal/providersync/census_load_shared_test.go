package providersync

import "testing"

func TestCensusLoadSharesOneLoadAcrossPatterns(t *testing.T) {
	first, err := censusLoad("./internal/providersync")
	if err != nil || len(first) != 1 {
		t.Fatalf("load: %v (%d packages)", err, len(first))
	}
	again, err := censusLoad("./internal/providersync")
	if err != nil || len(again) != 1 || again[0] != first[0] {
		t.Fatalf("a second request must return the same loaded package: %v", err)
	}
	all, err := censusLoad(logSitePackages...)
	if err != nil || len(all) != len(logSitePackages) {
		t.Fatalf("the log-site packages: %v (%d of %d)", err, len(all), len(logSitePackages))
	}
	if all[0] != first[0] {
		t.Fatal("the log-site load and the single-package load are different loads")
	}
	if _, err := censusLoad("./internal/notashared"); err == nil {
		t.Fatal("a package outside the shared set must be refused, not loaded empty")
	}
}
