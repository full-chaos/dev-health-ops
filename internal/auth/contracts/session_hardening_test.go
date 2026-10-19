package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"
)

func firstAccepted(t *testing.T, s sessionSurface) []byte {
	t.Helper()
	dir, m := loadSessionCorpus(t, s)
	raw, err := os.ReadFile(filepath.Join(dir, m.Accept[0].File))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func utf16WithBOM(raw []byte) []byte {
	units := utf16.Encode([]rune(string(raw)))
	out := []byte{0xff, 0xfe}
	for _, u := range units {
		out = append(out, byte(u), byte(u>>8))
	}
	return out
}

func TestEveryParserRefusesDocumentsThatAreNotPlainUTF8(t *testing.T) {
	root := testRoot(t)
	for _, s := range sessionSurfaces() {
		raw := firstAccepted(t, s)
		cases := map[string][]byte{
			"utf-16 with BOM": utf16WithBOM(raw),
			"utf-8 BOM":       append([]byte{0xef, 0xbb, 0xbf}, raw...),
		}
		for name, doc := range cases {
			t.Run(s.dir+"/"+name, func(t *testing.T) {
				if err := s.parse(root, doc); err == nil {
					t.Fatal("parser accepted a document that is not plain UTF-8 JSON")
				}
			})
		}
	}
}

func TestSecretBearingValuesNeverReachFormattersOrLoggers(t *testing.T) {
	root := testRoot(t)
	dir := filepath.Join(root, "contracts/auth/v1/examples")
	respRaw, err := os.ReadFile(filepath.Join(dir, "refresh-response", "valid-full-lifetimes.json"))
	if err != nil {
		t.Fatal(err)
	}
	reqRaw, err := os.ReadFile(filepath.Join(dir, "refresh-request", "valid-minimal.json"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ParseRefreshResponse(root, respRaw)
	if err != nil {
		t.Fatal(err)
	}
	req, err := ParseRefreshRequest(root, reqRaw)
	if err != nil {
		t.Fatal(err)
	}
	secrets := []string{"EXAMPLESIGNATURE", "EXAMPLEREFRESH", "EXAMPLEROTATED"}

	var jsonLog, textLog bytes.Buffer
	slog.New(slog.NewJSONHandler(&jsonLog, nil)).Info("probe", "response", resp, "request", req, "valueResponse", *resp)
	slog.New(slog.NewTextHandler(&textLog, nil)).Info("probe", "response", resp, "request", req, "valueResponse", *resp)
	marshalled, err := json.Marshal(map[string]any{"response": resp, "request": req})
	if err != nil {
		t.Fatal(err)
	}
	outputs := map[string]string{
		"%v":           fmt.Sprintf("%v %v", resp, req),
		"%+v":          fmt.Sprintf("%+v %+v", *resp, *req),
		"%#v":          fmt.Sprintf("%#v %#v", *resp, *req),
		"%v value":     fmt.Sprintf("%v %v", *resp, *req),
		"slog json":    jsonLog.String(),
		"slog text":    textLog.String(),
		"json.Marshal": string(marshalled),
	}
	for name, out := range outputs {
		for _, secret := range secrets {
			if strings.Contains(out, secret) {
				t.Errorf("%s leaks %q: %s", name, secret, out)
			}
		}
	}
}

func TestRejectedCredentialsNeverAppearInParserErrors(t *testing.T) {
	root := testRoot(t)
	// A runtime-built value keeps a credential-shaped literal out of the
	// source; the trailing dot makes it fail the credential pattern, so the
	// parser must reject it without repeating it.
	dummy := "Zq" + strings.Repeat("k7", 24) + "."
	cases := []struct {
		name  string
		file  string
		field string
		parse func(root string, raw []byte) error
	}{
		{"request refresh_credential", "refresh-request/valid-minimal.json", "refresh_credential", func(r string, b []byte) error { _, err := ParseRefreshRequest(r, b); return err }},
		{"response access_token", "refresh-response/valid-full-lifetimes.json", "access_token", func(r string, b []byte) error { _, err := ParseRefreshResponse(r, b); return err }},
		{"response refresh_credential", "refresh-response/valid-full-lifetimes.json", "refresh_credential", func(r string, b []byte) error { _, err := ParseRefreshResponse(r, b); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, "contracts/auth/v1/examples", c.file))
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			doc[c.field] = dummy
			mutated, _ := json.Marshal(doc)
			perr := c.parse(root, mutated)
			if perr == nil {
				t.Fatal("parser accepted a credential that fails the pattern")
			}
			if strings.Contains(perr.Error(), dummy) {
				t.Fatalf("the parser error echoes the rejected credential: %v", perr)
			}
			var logged bytes.Buffer
			slog.New(slog.NewJSONHandler(&logged, nil)).Info("rejected", "err", perr)
			if strings.Contains(logged.String(), dummy) {
				t.Fatalf("an Info log of the error echoes the credential: %s", logged.String())
			}
		})
	}
}
