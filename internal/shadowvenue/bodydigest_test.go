package shadowvenue

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

const (
	bdOrg1  = "3d206dbd-aea8-4914-a75c-0777e8247241"
	bdOrg4  = "9d206dbd-aea8-4914-a75c-0777e8247299"
	bdUser1 = "77b0f61d-ae0d-4060-b661-7f5254661447"
	bdUser4 = "88b0f61d-ae0d-4060-b661-7f5254661448"
	bdKey   = "GET /api/v1/admin/teams"
)

func bdFrozen(t *testing.T) ThrowawayIDs {
	t.Helper()
	ids, err := ParseThrowawayIDs([]byte(throwawayHeader + "\norg\tT1\t" + bdOrg1 + "\norg\tT4\t" + bdOrg4 + "\nuser\tadmin-T1\t" + bdUser1 + "\nuser\tadmin-T4\t" + bdUser4 + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestBodyDigestRecordsLengthRawShaAndNeverTheBody(t *testing.T) {
	raw := []byte(`{"b":1,"a":"secret-value"}`)
	d := DigestBody(bdKey, raw, bdFrozen(t), nil)
	if d.Len != len(raw) || len(d.SHA) != 64 || len(d.Norm) != 64 {
		t.Fatalf("%+v", d)
	}
	if strings.Contains(d.SHA+d.Norm, "secret") {
		t.Fatal("a digest holds the body")
	}
	if other := DigestBody(bdKey, []byte(`{"b":1,"a":"secret-valuf"}`), bdFrozen(t), nil); other.SHA == d.SHA || other.Norm == d.Norm {
		t.Fatal("two bodies of different content share a digest")
	}
}

func TestNormalizedDigestIgnoresKeyOrderAndTheFrozenIdsOfADifferentPool(t *testing.T) {
	a := DigestBody(bdKey, []byte(`{"org":"`+bdOrg1+`","users":[{"id":"`+strings.ToUpper(bdUser1)+`","n":1}],"z":true}`), bdFrozen(t), nil)
	b := DigestBody(bdKey, []byte(`{"z":true,"users":[{"n":1,"id":"`+bdUser4+`"}],"org":"`+bdOrg4+`"}`), bdFrozen(t), nil)
	if a.Norm != b.Norm {
		t.Fatal("the same body of two pools differs after normalization")
	}
	if a.SHA == b.SHA {
		t.Fatal("the raw digests of two pools are equal")
	}
	// an org id where a user id stood is another body
	swapped := DigestBody(bdKey, []byte(`{"org":"`+bdOrg1+`","users":[{"id":"`+bdOrg4+`","n":1}],"z":true}`), bdFrozen(t), nil)
	if swapped.Norm == a.Norm {
		t.Fatal("an org placeholder stands for a user id")
	}
	// an unfrozen id is not replaced
	other := DigestBody(bdKey, []byte(`{"org":"`+bdOrg1+`","users":[{"id":"11111111-1111-4111-8111-111111111111","n":1}],"z":true}`), bdFrozen(t), nil)
	if other.Norm == a.Norm {
		t.Fatal("an id outside the frozen set was replaced")
	}
}

func TestNormalizedDigestDropsOnlyThePinnedVolatileKeysOfTheRow(t *testing.T) {
	volatile := VolatileKeys{bdKey: {"updated_at": "set on write"}}
	a := DigestBody(bdKey, []byte(`{"id":"x","updated_at":"2026-11-08T01:00:00Z"}`), bdFrozen(t), volatile)
	b := DigestBody(bdKey, []byte(`{"id":"x","updated_at":"2026-11-08T02:00:00Z"}`), bdFrozen(t), volatile)
	if a.Norm != b.Norm {
		t.Fatal("a pinned volatile key changed the digest")
	}
	if c := DigestBody(bdKey, []byte(`{"id":"y","updated_at":"2026-11-08T02:00:00Z"}`), bdFrozen(t), volatile); c.Norm == a.Norm {
		t.Fatal("a key that is not pinned was dropped")
	}
	if d := DigestBody("GET /api/v1/other", []byte(`{"id":"x","updated_at":"2026-11-08T02:00:00Z"}`), bdFrozen(t), volatile); d.Norm == DigestBody("GET /api/v1/other", []byte(`{"id":"x","updated_at":"2026-11-08T01:00:00Z"}`), bdFrozen(t), volatile).Norm {
		t.Fatal("a volatile key of another row was dropped")
	}
	// nested keys are dropped at every level
	n1 := DigestBody(bdKey, []byte(`{"items":[{"updated_at":"a","v":1}]}`), bdFrozen(t), volatile)
	n2 := DigestBody(bdKey, []byte(`{"items":[{"updated_at":"b","v":1}]}`), bdFrozen(t), volatile)
	if n1.Norm != n2.Norm {
		t.Fatal("a nested pinned key was kept")
	}
	if len(PinnedVolatile) != 0 {
		t.Fatalf("the pinned volatile set starts empty until DC11 shows a key: %v", PinnedVolatile)
	}
}

func TestNormalizedDigestDropsTheStateOfAURLAndReadsNonJSONAsText(t *testing.T) {
	u1 := `{"url":"https://github.com/apps/x/installations/new?state=AAA.BBB&other=1"}`
	u2 := `{"url":"https://github.com/apps/x/installations/new?other=1&state=CCC.DDD"}`
	if DigestBody(bdKey, []byte(u1), bdFrozen(t), nil).Norm != DigestBody(bdKey, []byte(u2), bdFrozen(t), nil).Norm {
		t.Fatal("a state value changed the normalized digest")
	}
	if DigestBody(bdKey, []byte(u1), bdFrozen(t), nil).Norm == DigestBody(bdKey, []byte(`{"url":"https://github.com/apps/x/installations/new?state=AAA.BBB&other=2"}`), bdFrozen(t), nil).Norm {
		t.Fatal("another query value is ignored")
	}
	if DigestBody(bdKey, []byte("not json "+bdOrg1), bdFrozen(t), nil).Norm != DigestBody(bdKey, []byte("not json "+bdOrg4), bdFrozen(t), nil).Norm {
		t.Fatal("a text body keeps its frozen id")
	}
	if e := DigestBody(bdKey, nil, bdFrozen(t), nil); e.Len != 0 || len(e.Norm) != 64 {
		t.Fatalf("an empty body: %+v", e)
	}
}

func TestNormalizedDigestReplacesAFrozenIDUsedAsAKey(t *testing.T) {
	a := DigestBody(bdKey, []byte(`{"`+bdUser1+`":{"n":1}}`), bdFrozen(t), nil)
	c := DigestBody(bdKey, []byte(`{"`+bdUser4+`":{"n":1}}`), bdFrozen(t), nil)
	if a.Norm != c.Norm {
		t.Fatal("a frozen id used as a key kept its value")
	}
}

// Frozen ids of one kind used as object keys keep one entry each: the normalizer never lets one overwrite another, so two
// bodies that differ in which value stands for which id never share a digest, and equal bodies always do.
func TestFrozenIDsAsObjectKeysNeverCollapse(t *testing.T) {
	var lines strings.Builder
	lines.WriteString(throwawayHeader + "\n")
	fmt.Fprintf(&lines, "org\tT1\t%s\n", bdOrg1)
	ids := make([]string, 8)
	for i := range ids {
		ids[i] = fmt.Sprintf("%08x-aea8-4914-a75c-0777e8247241", 0x10000000+i)
		fmt.Fprintf(&lines, "user\tu%d\t%s\n", i, ids[i])
	}
	frozen, err := ParseThrowawayIDs([]byte(lines.String()))
	if err != nil {
		t.Fatal(err)
	}
	body := func(order []int) []byte {
		var b strings.Builder
		b.WriteString("{")
		for n, i := range order {
			if n > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "%q:%d", ids[i], i)
		}
		b.WriteString("}")
		return []byte(b.String())
	}
	same := []int{0, 1, 2, 3, 4, 5, 6, 7}
	swapped := []int{0, 1, 2, 3, 4, 5, 7, 6}
	want := DigestBody(bdKey, []byte(strings.ReplaceAll(string(body(same)), fmt.Sprintf("%q:6", ids[6]), fmt.Sprintf("%q:6", ids[6]))), frozen, nil).Norm
	for run := 0; run < 200; run++ {
		if got := DigestBody(bdKey, body(same), frozen, nil).Norm; got != want {
			t.Fatalf("run %d: the same body gave two digests", run)
		}
		other := []byte(strings.Replace(string(body(same)), fmt.Sprintf("%q:6", ids[6]), fmt.Sprintf("%q:600", ids[6]), 1))
		if DigestBody(bdKey, other, frozen, nil).Norm == want {
			t.Fatalf("run %d: a changed value of one frozen-id key shares the digest", run)
		}
		_ = swapped
	}
	// the values of two frozen-id keys exchanged is another body
	a := []byte(fmt.Sprintf(`{%q:1,%q:2}`, ids[0], ids[1]))
	b := []byte(fmt.Sprintf(`{%q:2,%q:1}`, ids[0], ids[1]))
	if DigestBody(bdKey, a, frozen, nil).Norm == DigestBody(bdKey, b, frozen, nil).Norm {
		t.Fatal("exchanging the values of two frozen-id keys gave the same digest")
	}
	// a key holding a literal that looks like a placeholder does not overwrite either
	c := []byte(fmt.Sprintf(`{%q:1,"{T_USER}":2}`, ids[0]))
	if got := normalizeBody(bdKey, c, frozen, nil); len(bytes.TrimSpace(got)) == 0 || strings.Count(string(got), ":") != 2 {
		t.Fatalf("a key collided with a literal placeholder: %s", got)
	}
}
