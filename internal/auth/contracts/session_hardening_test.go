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
	secrets := []string{"EXAMPLEPAYLOAD", "EXAMPLESIGNATURE", "EXAMPLEREFRESH", "EXAMPLEROTATED"}

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
