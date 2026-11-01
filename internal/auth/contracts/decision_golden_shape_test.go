package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const decisionGoldenDir = "contracts/auth/v1/golden/decision"

var decisionTopFiles = map[string]bool{"README.md": true, "manifest.json": true, "DIGEST": true}

type decisionFile struct {
	rel string
	raw []byte
}

func decisionFiles(t *testing.T) []decisionFile {
	t.Helper()
	root := filepath.Join(testRoot(t), decisionGoldenDir)
	var files []decisionFile
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, path)
		files = append(files, decisionFile{filepath.ToSlash(rel), raw})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	return files
}

func decisionDigest(files []decisionFile) string {
	h := sha256.New()
	for _, f := range files {
		if f.rel == "DIGEST" {
			continue
		}
		fmt.Fprintf(h, "%s\n%d\n", f.rel, len(f.raw))
		h.Write(f.raw)
		h.Write([]byte("\n"))
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// decisionWalk refuses a repeated member or a non-integer number and returns the value.
func decisionWalk(dec *json.Decoder, path string) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case json.Delim:
		if v == '{' {
			obj := map[string]any{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				k := kt.(string)
				if _, dup := obj[k]; dup {
					return nil, fmt.Errorf("%s: repeated member %q", path, k)
				}
				val, err := decisionWalk(dec, path+"/"+k)
				if err != nil {
					return nil, err
				}
				obj[k] = val
			}
			_, err := dec.Token()
			return obj, err
		}
		arr := []any{}
		for dec.More() {
			val, err := decisionWalk(dec, fmt.Sprintf("%s/%d", path, len(arr)))
			if err != nil {
				return nil, err
			}
			arr = append(arr, val)
		}
		_, err := dec.Token()
		return arr, err
	case json.Number:
		if strings.ContainsAny(v.String(), ".eE") {
			return nil, fmt.Errorf("%s: non-integer number %s", path, v)
		}
	}
	return tok, nil
}

func decisionParse(raw []byte, name string) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := decisionWalk(dec, name)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%s: data after the document", name)
	}
	return v, nil
}

func decisionCanonical(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
	return b.Bytes()
}

func decisionKeys(t *testing.T, name string, v any, want ...string) map[string]any {
	t.Helper()
	obj, ok := v.(map[string]any)
	if !ok {
		t.Errorf("%s: not an object", name)
		return map[string]any{}
	}
	got := make([]string, 0, len(obj))
	for k := range obj {
		got = append(got, k)
	}
	sort.Strings(got)
	w := append([]string(nil), want...)
	sort.Strings(w)
	if strings.Join(got, ",") != strings.Join(w, ",") {
		t.Errorf("%s: members %v, want exactly %v", name, got, w)
	}
	return obj
}

type decisionManifestRow struct{ file, id, layer string }

func decisionManifest(t *testing.T, files []decisionFile) (rows []decisionManifestRow, catalogs []string) {
	t.Helper()
	for _, f := range files {
		if f.rel != "manifest.json" {
			continue
		}
		doc, err := decisionParse(f.raw, f.rel)
		if err != nil {
			t.Fatal(err)
		}
		m := decisionKeys(t, "manifest", doc, "catalogs", "description", "surface", "vectors")
		if m["surface"] != "authorization-golden.v1" {
			t.Errorf("manifest surface %v", m["surface"])
		}
		for i, c := range m["catalogs"].([]any) {
			row := decisionKeys(t, fmt.Sprintf("manifest catalogs[%d]", i), c, "file", "id")
			catalogs = append(catalogs, row["file"].(string))
		}
		prev := ""
		for i, r := range m["vectors"].([]any) {
			row := decisionKeys(t, fmt.Sprintf("manifest vectors[%d]", i), r, "file", "id", "layer")
			x := decisionManifestRow{row["file"].(string), row["id"].(string), row["layer"].(string)}
			if x.layer != "pdp" && x.layer != "pep" {
				t.Errorf("row %s: layer %q is outside pdp|pep", x.id, x.layer)
			}
			if x.file != x.id+".json" {
				t.Errorf("row %s: file %q", x.id, x.file)
			}
			if x.id <= prev {
				t.Errorf("row %s: manifest rows are not sorted by id", x.id)
			}
			prev = x.id
			rows = append(rows, x)
		}
		return rows, catalogs
	}
	t.Fatal("no manifest.json")
	return nil, nil
}

func TestDecisionGoldenManifestClaimsExactlyTheFilesOnDisk(t *testing.T) {
	files := decisionFiles(t)
	rows, catalogs := decisionManifest(t, files)
	if len(rows) == 0 {
		t.Fatal("manifest holds no vector row")
	}
	claimed := map[string]bool{}
	for _, r := range rows {
		claimed["vectors/"+r.file] = true
	}
	for _, c := range catalogs {
		claimed[c] = true
	}
	for _, f := range files {
		if !claimed[f.rel] && !decisionTopFiles[f.rel] {
			t.Errorf("%s is on disk and not claimed by the manifest", f.rel)
		}
		delete(claimed, f.rel)
	}
	for c := range claimed {
		t.Errorf("manifest claims %s, which is not on disk", c)
	}
	for k := range decisionTopFiles {
		found := false
		for _, f := range files {
			found = found || f.rel == k
		}
		if !found {
			t.Errorf("%s is missing", k)
		}
	}
}

var (
	decisionStateKeys   = []string{"catalog", "snapshot", "clock", "principal_status", "platform_role", "organizations", "memberships", "service_accounts", "grants", "grants_error", "workload"}
	decisionRequestKeys = []string{"action", "resource_kind", "organization_id", "resource_key"}
	decisionExpectKeys  = []string{"allowed", "reason", "cache_class", "expires_in_seconds", "policy_revision"}
)

func TestDecisionGoldenFilesAreCanonicalAndClosed(t *testing.T) {
	files := decisionFiles(t)
	rows, _ := decisionManifest(t, files)
	layers := map[string]string{}
	for _, r := range rows {
		layers["vectors/"+r.file] = r.layer
	}
	for _, f := range files {
		if !strings.HasSuffix(f.rel, ".json") {
			continue
		}
		doc, err := decisionParse(f.raw, f.rel)
		if err != nil {
			t.Error(err)
			continue
		}
		if !bytes.Equal(decisionCanonical(doc), f.raw) {
			t.Errorf("%s is not canonical JSON (UTF-8, LF, 2 spaces, sorted keys, trailing newline)", f.rel)
		}
		switch {
		case strings.HasPrefix(f.rel, "catalogs/"):
			c := decisionKeys(t, f.rel, doc, "id", "description", "action", "holders")
			decisionKeys(t, f.rel+" action", c["action"], "action", "scope", "resource_kind", "risk_class", "cache_class")
		case strings.HasPrefix(f.rel, "vectors/"):
			v, ok := doc.(map[string]any)
			if !ok {
				continue
			}
			switch layers[f.rel] {
			case "pdp":
				v = decisionKeys(t, f.rel, doc, "id", "layer", "why", "state", "request", "expect")
				stateKeys := decisionStateKeys
				if st, ok := v["state"].(map[string]any); ok {
					if d, has := st["delegation"]; has {
						stateKeys = append(append([]string(nil), decisionStateKeys...), "delegation")
						dm := decisionKeys(t, f.rel+" delegation", d, "chain", "actor_platform_role", "target_organization_id", "assurance")
						if chain, ok := dm["chain"].([]any); ok {
							for _, hop := range chain {
								decisionKeys(t, f.rel+" delegation hop", hop, "actor", "permitted_actions")
							}
						} else {
							t.Errorf("%s: delegation chain is not an array", f.rel)
						}
					}
				}
				decisionKeys(t, f.rel+" state", v["state"], stateKeys...)
				decisionKeys(t, f.rel+" request", v["request"], decisionRequestKeys...)
				decisionKeys(t, f.rel+" expect", v["expect"], decisionExpectKeys...)
				if st, ok := v["state"].(map[string]any); ok {
					if c := st["catalog"]; c != "embedded" && c != "planted-repo-reader" {
						t.Errorf("%s: catalog %v outside embedded|planted-repo-reader", f.rel, c)
					}
				}
			case "pep":
				v = decisionKeys(t, f.rel, doc, "id", "layer", "why", "request_raw", "expect")
				decisionKeys(t, f.rel+" expect", v["expect"], "outcome", "refusal", "question")
			default:
				t.Errorf("%s has no manifest row", f.rel)
				continue
			}
			if v["layer"] != layers[f.rel] {
				t.Errorf("%s: layer %v, manifest %s", f.rel, v["layer"], layers[f.rel])
			}
		}
	}
}

func TestDecisionGoldenDigestIsPinned(t *testing.T) {
	files := decisionFiles(t)
	got := decisionDigest(files)
	for _, f := range files {
		if f.rel == "DIGEST" {
			if string(f.raw) != got+"\n" {
				t.Fatalf("DIGEST holds %q, the directory hashes to %s", strings.TrimSpace(string(f.raw)), got)
			}
			return
		}
	}
	t.Fatalf("DIGEST is missing; the directory hashes to %s", got)
}

func TestDecisionGoldenReadmeStatesTheDigestFraming(t *testing.T) {
	for _, f := range decisionFiles(t) {
		if f.rel == "README.md" {
			for _, want := range []string{"path LF decimal-length LF bytes LF", "sorted by relative path", "excluding DIGEST", "expect.reason", "INTERNAL"} {
				if !strings.Contains(string(f.raw), want) {
					t.Errorf("README does not state %q", want)
				}
			}
			return
		}
	}
	t.Fatal("README.md is missing")
}

func TestDecisionGoldenIsScannedForSecrets(t *testing.T) {
	files := decisionFiles(t)
	scanned := 0
	for _, f := range files {
		scanned++
		for _, finding := range scanFixtureContent(decisionGoldenDir+"/"+f.rel, string(f.raw)) {
			t.Error(finding)
		}
	}
	if scanned < 4 {
		t.Fatalf("scanned %d files under %s", scanned, decisionGoldenDir)
	}
	control := "eyJhbGciOiJFZERTQSJ" + "9.eyJzdWIiOiJ4In0" + ".AAAAAAAAAAAAAAAA"
	if len(scanFixtureContent("control", `{"request_raw":"`+control+`"}`)) == 0 {
		t.Fatal("the scanner does not flag a planted token shape")
	}
}
