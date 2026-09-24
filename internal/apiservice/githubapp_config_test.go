package apiservice

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGitHubAppConfigReadsTheEnvironment(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "app.pem")
	if err := os.WriteFile(keyFile, []byte("-----BEGIN KEY-----\nfile\n-----END KEY-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"GITHUB_APP_SLUG": "my-app", "GITHUB_APP_ID": "42", "GITHUB_APP_CLIENT_ID": "Iv1.x", "GITHUB_APP_CLIENT_SECRET": "sec",
		"GITHUB_APP_CALLBACK_URL": "https://app.example.test/cb",
	}
	lookup := func(name string) (string, bool) { value, ok := env[name]; return value, ok }
	config := GitHubAppConfig(lookup)
	if config.Slug != "my-app" || config.AppID != "42" || config.ClientID != "Iv1.x" || config.ClientSecret != "sec" || config.CallbackURL != "https://app.example.test/cb" {
		t.Fatalf("config = %+v", config)
	}
	if key, err := config.PrivateKey(); key != "" || err != nil {
		t.Fatalf("no key configured: PrivateKey = %q, %v", key, err)
	}

	env["GITHUB_APP_PRIVATE_KEY"] = `-----BEGIN KEY-----\ninline\n-----END KEY-----`
	env["GITHUB_APP_PRIVATE_KEY_PATH"] = keyFile
	inline, err := GitHubAppConfig(lookup).PrivateKey()
	if err != nil || inline != "-----BEGIN KEY-----\ninline\n-----END KEY-----" {
		t.Fatalf("an inline key wins and its escaped newlines are real ones: %q, %v", inline, err)
	}

	delete(env, "GITHUB_APP_PRIVATE_KEY")
	fromFile, err := GitHubAppConfig(lookup).PrivateKey()
	if err != nil || fromFile != "-----BEGIN KEY-----\nfile\n-----END KEY-----\n" {
		t.Fatalf("the key file: %q, %v", fromFile, err)
	}

	env["GITHUB_APP_PRIVATE_KEY_PATH"] = filepath.Join(t.TempDir(), "missing.pem")
	if _, err := GitHubAppConfig(lookup).PrivateKey(); err == nil {
		t.Fatal("an unreadable key file must be an error")
	}
	env["GITHUB_APP_PRIVATE_KEY"] = ""
	env["GITHUB_APP_PRIVATE_KEY_PATH"] = ""
	if key, err := GitHubAppConfig(lookup).PrivateKey(); key != "" || err != nil {
		t.Fatalf("empty values are unset: %q, %v", key, err)
	}
}
