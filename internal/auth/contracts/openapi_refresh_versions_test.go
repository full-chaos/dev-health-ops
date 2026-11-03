package contracts

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// refreshResponseRefs returns the component names the refresh operation's 200
// response offers, or a problem when its shape is not a oneOf of $refs.
func refreshResponseRefs(document map[string]any) ([]string, string) {
	paths, _ := document["paths"].(map[string]any)
	item, _ := paths["/v1/token/refresh"].(map[string]any)
	post, _ := item["post"].(map[string]any)
	responses, _ := post["responses"].(map[string]any)
	ok, _ := responses["200"].(map[string]any)
	content, _ := ok["content"].(map[string]any)
	body, _ := content["application/json"].(map[string]any)
	schema, _ := body["schema"].(map[string]any)
	choices, _ := schema["oneOf"].([]any)
	if len(choices) == 0 {
		return nil, "POST /v1/token/refresh 200 has no oneOf of refresh responses"
	}
	var names []string
	for _, choice := range choices {
		ref, _ := choice.(map[string]any)["$ref"].(string)
		name, found := strings.CutPrefix(ref, "#/components/schemas/")
		if !found {
			return nil, "a refresh 200 choice is not a component $ref"
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, ""
}

// TestRefreshOperationAdvertisesEveryRefreshResponseVersion: the refresh 200
// offers one component per refresh-response.vN schema on disk, so a version
// cannot ship in jsonschema/ and be dropped from the operation.
func TestRefreshOperationAdvertisesEveryRefreshResponseVersion(t *testing.T) {
	root := testRoot(t)
	files, err := filepath.Glob(filepath.Join(ContractsDir(root), "jsonschema", "refresh-response.v*.schema.json"))
	if err != nil || len(files) < 2 {
		t.Fatalf("refresh-response schemas on disk: %v (err %v), want v1 and v2", files, err)
	}
	var want []string
	for _, file := range files {
		version := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), "refresh-response.v"), ".schema.json")
		if version == "1" {
			want = append(want, "RefreshResponse")
		} else {
			want = append(want, "RefreshResponseV"+version)
		}
	}
	sort.Strings(want)

	document := loadOpenAPIRaw(t)
	got, problem := refreshResponseRefs(document)
	if problem != "" {
		t.Fatal(problem)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("refresh 200 offers %v, the schemas on disk are %v", got, want)
	}

	// Each version's component points at its own schema file.
	components, _ := document["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	for _, file := range files {
		version := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), "refresh-response.v"), ".schema.json")
		component := "RefreshResponse"
		if version != "1" {
			component += "V" + version
		}
		entry, _ := schemas[component].(map[string]any)
		if ref, _ := entry["$ref"].(string); ref != "../jsonschema/"+filepath.Base(file) {
			t.Errorf("component %s references %q, want ../jsonschema/%s", component, ref, filepath.Base(file))
		}
	}

	// Planted control: the same check on a document with the v2 choice removed
	// must differ.
	paths := document["paths"].(map[string]any)
	schema := paths["/v1/token/refresh"].(map[string]any)["post"].(map[string]any)["responses"].(map[string]any)["200"].(map[string]any)["content"].(map[string]any)["application/json"].(map[string]any)["schema"].(map[string]any)
	choices := schema["oneOf"].([]any)
	schema["oneOf"] = choices[:1]
	planted, _ := refreshResponseRefs(document)
	if strings.Join(planted, ",") == strings.Join(want, ",") {
		t.Error("removing the v2 choice left the check satisfied")
	}
}
