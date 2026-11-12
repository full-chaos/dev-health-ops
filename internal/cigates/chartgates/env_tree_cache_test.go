package chartgates_test

import (
	"reflect"
	"testing"
)

func TestEnvReaderCacheDeepCopyFixture(t *testing.T) {
	units := checkSources(map[string]string{
		"internal/x/a.go": "package x\nfunc A(lookup func(string) string) string { return lookup(\"KEY\") }\n",
		"internal/x/b.go": "package x\nfunc B(lookup func(string) string) string { return lookup(\"KEY\") }\n",
	})
	var cache envReaderCache
	want := map[string][]string{"KEY": {"internal/x/a.go", "internal/x/b.go"}}
	got := cache.scan(units)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("first scan = %v, want %v", got, want)
	}
	got["KEY"][0] = "corrupted"
	got["KEY"] = append(got["KEY"], "appended")
	got["ADDED"] = []string{"new"}
	// Nil units also proves that the second call uses the memoized scan.
	if again := cache.scan(nil); !reflect.DeepEqual(again, want) {
		t.Fatalf("second scan = %v, want %v", again, want)
	}
}
