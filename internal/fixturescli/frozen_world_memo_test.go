package fixturescli

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

var memoFrozenParams = GenerateParams{Provider: "synthetic", RepoName: "acme/live-e2e", RepoCount: 1, Days: 14, CommitsPerDay: 6, PRCount: 24, TeamCount: 10, Seed: 20260219, WithMetrics: true, WithWorkGraph: true}

func TestLoadingOneFrozenWorldDecodesNoOtherWorld(t *testing.T) {
	before := worldDecodes.Load()
	if _, err := LoadFrozenWorld(memoFrozenParams); err != nil {
		t.Fatal(err)
	}
	if got := worldDecodes.Load() - before; got > 1 {
		t.Fatalf("one load of %s decoded %d world(s); a load that succeeds decodes at most the world it asked for", memoFrozenParams, got)
	}
}

func TestLoadFrozenWorldDecodesEachWorldOnceAndHandsOutCopies(t *testing.T) {
	first, err := LoadFrozenWorld(memoFrozenParams)
	if err != nil {
		t.Fatal(err)
	}
	before := worldDecodes.Load()
	second, err := LoadFrozenWorld(memoFrozenParams)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFrozenWorld(memoFrozenParams); err != nil {
		t.Fatal(err)
	}
	if got := worldDecodes.Load() - before; got != 0 {
		t.Fatalf("a repeated load of a loaded world decoded %d world(s); it must decode none", got)
	}
	if len(first.Tables) == 0 || len(first.Tables[0].Rows) == 0 || len(first.Tables[0].Rows[0]) == 0 {
		t.Fatal("the frozen world has no first row to mutate")
	}
	first.Tables[0].Rows[0][0] = "mutated by the caller"
	first.Tables[0].Name = "mutated"
	first.Tables[0].Columns[0].Name = "mutated"
	third, err := LoadFrozenWorld(memoFrozenParams)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []FrozenWorld{second, third} {
		if w.Tables[0].Rows[0][0] == "mutated by the caller" || w.Tables[0].Name == "mutated" || w.Tables[0].Columns[0].Name == "mutated" {
			t.Fatal("a caller's change to a loaded world reached another load")
		}
	}
}

func TestLoadFrozenSetHandsOutCopies(t *testing.T) {
	const org, repo, days = "c0ffee00-dead-4bee-8bad-f00dfeedface", "ci-metrics-executed-proof/repo", 7
	first, err := LoadFrozenSet(org, repo, days)
	if err != nil {
		t.Fatal(err)
	}
	table := &first.Targets[0].Tables[0]
	if len(table.Rows) == 0 || len(table.Rows[0]) == 0 {
		t.Fatal("the frozen set has no first row to mutate")
	}
	table.Rows[0][0] = "mutated by the caller"
	table.Columns[0].Name = "mutated"
	first.Targets[0].Name = "mutated"
	second, err := LoadFrozenSet(org, repo, days)
	if err != nil {
		t.Fatal(err)
	}
	got := second.Targets[0]
	if got.Name == "mutated" || got.Tables[0].Rows[0][0] == "mutated by the caller" || got.Tables[0].Columns[0].Name == "mutated" {
		t.Fatal("a caller's change to a loaded set reached the next load")
	}
}

// The values were read from the loader before the decode was memoized: a load must hand out the same
// content, first load or repeat, as it always did.
var loadedFrozenContentDigests = map[string]string{
	"11111111-2222-4333-8444-555555555555_acme__live-e2e_14d.json.gz":                 "fef84138568edf904a0558e012163a023b5cd3a3c3144c7e1f41ab99206ffc34",
	"c0ffee00-dead-4bee-8bad-f00dfeedface_ci-metrics-executed-proof__repo_7d.json.gz": "e22d0b5168b6bb6eda7ceca7097676b4f218d030ab0d7e1a2846a30a9069bade",
	"github_acme__live-e2e_r1_14d_c6_p24_t10_s20260219_raw.json.gz":                   "63d063244cf2f6cea217b638a5713152532df6e298f90ca5584c12136c1b4245",
	"github_chaos-fixtures__payments-api_r2_120d_c0_p6_t4_s1_raw.json.gz":             "016dfb47f2a05544e8563c0e75508af778de0ed43e2182677041cc54e5c45872",
	"gitlab_chaos-gl__billing-service_r2_160d_c0_p3_t4_s1_raw.json.gz":                "b1c179b2bcbf445361180cf846ead3c09d9b60b455e485d5dddfcd23dcbe6ba5",
	"jira_chaos-fx__PAY_r2_180d_c0_p4_t4_s1_raw.json.gz":                              "015b86bfc57d761e96ef6f6c05cd9753d694d8140cf4c8151b78ae5d7c595251",
	"linear_chaos-fx__ENG_r2_160d_c0_p4_t4_s1_raw.json.gz":                            "64ffa9cb6d1a224041cd1f5f8a38e3ceae538d93648fddfc8441d291765c67e8",
	"mixed_chaos-fixtures__shared-api_r18_180d_c0_p189_t8_s9440_raw.json.gz":          "b4ed6a5e8582cfe94932a74a0385a030287eae2c38b6e1dceb841518bde69f2e",
	"synthetic_acme__live-e2e_r1_14d_c6_p24_t10_s20260219_mg.json.gz":                 "cec7a2964706cca515736b4a3d96e8fe5a71cfc974e0cd2d75a9d3afed1578d3",
	"synthetic_ci-metrics-executed-proof__repo_r1_7d_c5_p20_t1_s4276_raw.json.gz":     "1736c6081da3c8acea0bfb54ee82d9a32376ddf94e5fbaa35b0b11a569e4e0d6",
}

func contentDigest(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func TestEveryFrozenWorldAndSetLoadsToItsPinnedContent(t *testing.T) {
	got := map[string]string{}
	worlds, err := worldFiles.ReadDir("testdata/generate")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range worlds {
		raw, err := worldFiles.ReadFile("testdata/generate/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		recorded, err := decodeWorld(raw)
		if err != nil {
			t.Fatal(err)
		}
		for round := 0; round < 2; round++ {
			world, err := LoadFrozenWorld(recorded.Params)
			if err != nil {
				t.Fatal(err)
			}
			digest := contentDigest(t, world)
			if previous, seen := got[entry.Name()]; seen && previous != digest {
				t.Fatalf("%s: a repeat load handed out other content than the first", entry.Name())
			}
			got[entry.Name()] = digest
		}
	}
	sets, err := syntheticFiles.ReadDir("testdata/synthetic")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range sets {
		raw, err := syntheticFiles.ReadFile("testdata/synthetic/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		recorded, err := decodeFrozenSet(raw)
		if err != nil {
			t.Fatal(err)
		}
		for round := 0; round < 2; round++ {
			set, err := LoadFrozenSet(recorded.OrgID, recorded.RepoName, recorded.Days)
			if err != nil {
				t.Fatal(err)
			}
			digest := contentDigest(t, set)
			if previous, seen := got[entry.Name()]; seen && previous != digest {
				t.Fatalf("%s: a repeat load handed out other content than the first", entry.Name())
			}
			got[entry.Name()] = digest
		}
	}
	if len(got) != len(loadedFrozenContentDigests) {
		t.Errorf("loaded %d frozen files, %d are pinned", len(got), len(loadedFrozenContentDigests))
	}
	for name, digest := range got {
		if want := loadedFrozenContentDigests[name]; digest != want {
			t.Errorf("%s loads to content %s, pinned %s", name, digest, want)
		}
	}
}

func TestRefusalTextIsUnchanged(t *testing.T) {
	unfrozen := memoFrozenParams
	unfrozen.Days = 15
	_, err := LoadFrozenWorld(unfrozen)
	if err == nil || err.Error() != wantWorldRefusal {
		t.Errorf("world refusal = %q, want %q", err, wantWorldRefusal)
	}
	_, err = LoadFrozenSet("c0ffee00-dead-4bee-8bad-f00dfeedface", "ci-metrics-executed-proof/repo", 8)
	if err == nil || err.Error() != wantSetRefusal {
		t.Errorf("set refusal = %q, want %q", err, wantSetRefusal)
	}
}

const (
	wantWorldRefusal = "no frozen world for --provider synthetic --repo-name acme/live-e2e --repo-count 1 --days 15 --commits-per-day 6 --pr-count 24 --team-count 10 --seed 20260219 --with-metrics --with-work-graph; the frozen worlds are:\n  --provider github --repo-name acme/live-e2e --repo-count 1 --days 14 --commits-per-day 6 --pr-count 24 --team-count 10 --seed 20260219\n  --provider github --repo-name chaos-fixtures/payments-api --repo-count 2 --days 120 --commits-per-day 0 --pr-count 6 --team-count 4 --seed 1\n  --provider gitlab --repo-name chaos-gl/billing-service --repo-count 2 --days 160 --commits-per-day 0 --pr-count 3 --team-count 4 --seed 1\n  --provider jira --repo-name chaos-fx/PAY --repo-count 2 --days 180 --commits-per-day 0 --pr-count 4 --team-count 4 --seed 1\n  --provider linear --repo-name chaos-fx/ENG --repo-count 2 --days 160 --commits-per-day 0 --pr-count 4 --team-count 4 --seed 1\n  --provider mixed --repo-name chaos-fixtures/shared-api --repo-count 18 --days 180 --commits-per-day 0 --pr-count 189 --team-count 8 --seed 9440\n  --provider synthetic --repo-name acme/live-e2e --repo-count 1 --days 14 --commits-per-day 6 --pr-count 24 --team-count 10 --seed 20260219 --with-metrics --with-work-graph\n  --provider synthetic --repo-name ci-metrics-executed-proof/repo --repo-count 1 --days 7 --commits-per-day 5 --pr-count 20 --team-count 1 --seed 4276"
	wantSetRefusal   = "no frozen synthetic rows for organization c0ffee00-dead-4bee-8bad-f00dfeedface, repository ci-metrics-executed-proof/repo, 8 day(s); the frozen sets are: 11111111-2222-4333-8444-555555555555 / acme/live-e2e / 14 days; c0ffee00-dead-4bee-8bad-f00dfeedface / ci-metrics-executed-proof/repo / 7 days"
)

func TestDecodeWorldCachedKeysByContent(t *testing.T) {
	pack := func(org string) []byte {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write([]byte(`{"producer":"p","org_id":"` + org + `","tables":[]}`)); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	a, b := pack("org-a"), pack("org-b")
	first, err := decodeWorldCached(a, sha256.Sum256(a))
	if err != nil {
		t.Fatal(err)
	}
	second, err := decodeWorldCached(b, sha256.Sum256(b))
	if err != nil {
		t.Fatal(err)
	}
	if first.OrgID != "org-a" || second.OrgID != "org-b" {
		t.Fatalf("a different file got another file's decode: %q, %q", first.OrgID, second.OrgID)
	}
}

// Every reference-typed field of a memoized type, found by reflection, is filled and must come out of
// the copy on its own memory: a field added later that the copy does not cover fails here, before it
// can let one caller's change reach the memo.
func TestTheMemoCopiesShareNoMemoryWithTheMemo(t *testing.T) {
	for name, copyOf := range map[string]func(any) any{
		"FrozenWorld": func(v any) any { return cloneWorld(v.(FrozenWorld)) },
		"FrozenSet":   func(v any) any { return cloneSet(v.(FrozenSet)) },
	} {
		typ := reflect.TypeOf(FrozenWorld{})
		if name == "FrozenSet" {
			typ = reflect.TypeOf(FrozenSet{})
		}
		original := reflect.New(typ).Elem()
		fillEveryReference(original, 0)
		copied := reflect.ValueOf(copyOf(original.Interface()))
		var shared []string
		sharedMemory(name, original, copied, &shared)
		if len(shared) > 0 {
			t.Errorf("the copy of %s shares memory with the memo at: %v", name, shared)
		}
	}
}

func fillEveryReference(v reflect.Value, depth int) {
	if depth > 8 {
		return
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanSet() {
				fillEveryReference(v.Field(i), depth+1)
			}
		}
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillEveryReference(v.Index(0), depth+1)
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		key := reflect.New(v.Type().Key()).Elem()
		elem := reflect.New(v.Type().Elem()).Elem()
		fillEveryReference(elem, depth+1)
		v.SetMapIndex(key, elem)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillEveryReference(v.Elem(), depth+1)
	case reflect.Interface:
		if v.NumMethod() == 0 {
			v.Set(reflect.ValueOf([]any{map[string]any{"k": []any{"v"}}}))
		}
	}
}

func sharedMemory(path string, a, b reflect.Value, shared *[]string) {
	if a.Kind() != b.Kind() {
		return
	}
	switch a.Kind() {
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			sharedMemory(path+"."+a.Type().Field(i).Name, a.Field(i), b.Field(i), shared)
		}
	case reflect.Slice:
		if a.Len() > 0 && b.Len() > 0 && a.Pointer() == b.Pointer() {
			*shared = append(*shared, path)
			return
		}
		for i := 0; i < a.Len() && i < b.Len(); i++ {
			sharedMemory(fmt.Sprintf("%s[%d]", path, i), a.Index(i), b.Index(i), shared)
		}
	case reflect.Map:
		if a.Len() > 0 && a.Pointer() == b.Pointer() {
			*shared = append(*shared, path)
			return
		}
		for _, key := range a.MapKeys() {
			if other := b.MapIndex(key); other.IsValid() {
				sharedMemory(fmt.Sprintf("%s[%v]", path, key), a.MapIndex(key), other, shared)
			}
		}
	case reflect.Pointer:
		if !a.IsNil() && a.Pointer() == b.Pointer() {
			*shared = append(*shared, path)
			return
		}
		if !a.IsNil() && !b.IsNil() {
			sharedMemory(path, a.Elem(), b.Elem(), shared)
		}
	case reflect.Interface:
		if !a.IsNil() && !b.IsNil() {
			sharedMemory(path, a.Elem(), b.Elem(), shared)
		}
	}
}
