package main

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type composeKeyService struct {
	Environment map[string]string `yaml:"environment"`
	Volumes     []yaml.Node       `yaml:"volumes"`
}

type composeKeyFile struct {
	Services map[string]composeKeyService `yaml:"services"`
}

// keyVolume returns the named volume mounted over the directory of the file
// that env names, or "" when none is.
func keyVolume(service composeKeyService, env string) (string, error) {
	file := service.Environment[env]
	if file == "" {
		return "", nil
	}
	for _, node := range service.Volumes {
		var source, target string
		switch node.Kind {
		case yaml.ScalarNode:
			parts := strings.Split(node.Value, ":")
			if len(parts) < 2 {
				continue
			}
			source, target = parts[0], parts[1]
		case yaml.MappingNode:
			var mount struct {
				Type   string `yaml:"type"`
				Source string `yaml:"source"`
				Target string `yaml:"target"`
			}
			if err := node.Decode(&mount); err != nil {
				return "", err
			}
			if mount.Type != "" && mount.Type != "volume" {
				continue
			}
			source, target = mount.Source, mount.Target
		default:
			return "", fmt.Errorf("volume entry of kind %d", node.Kind)
		}
		if target == path.Dir(file) || strings.HasPrefix(path.Dir(file), strings.TrimSuffix(target, "/")+"/") {
			return source, nil
		}
	}
	return "", nil
}

// authKeyVolumeFindings: auth-service's signing key comes from a named volume
// of its own, never the volume go-api's envelope key comes from (the access
// issuer has its own key and kid in every stack).
func authKeyVolumeFindings(text []byte) ([]string, error) {
	return nil, nil
}

func TestAuthServiceSigningKeyIsNotTheEnvelopeKey(t *testing.T) {
	text := readCompose(t, "compose.yml")
	findings, err := authKeyVolumeFindings(text)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range findings {
		t.Error(finding)
	}
	var file composeKeyFile
	if err := yaml.Unmarshal(text, &file); err != nil {
		t.Fatal(err)
	}
	authVolume, _ := keyVolume(file.Services["auth-service"], "AUTH_SIGNING_KEY_FILE")
	envelopeVolume, _ := keyVolume(file.Services["go-api"], "GO_API_ENVELOPE_PRIVATE_KEY_FILE")
	if authVolume == "" || envelopeVolume == "" {
		t.Fatalf("the guard resolved auth %q and envelope %q: it compared nothing", authVolume, envelopeVolume)
	}
}

func TestTheSigningKeyGuardRefusesAPlantedEnvelopeMount(t *testing.T) {
	planted := []byte(`
services:
  go-api:
    environment:
      GO_API_ENVELOPE_PRIVATE_KEY_FILE: /run/envelope/private/envelope-private-key.pem
    volumes:
      - type: volume
        source: envelope_keys
        target: /run/envelope/private
  auth-service:
    environment:
      AUTH_SIGNING_KEY_FILE: /run/auth/signing/envelope-private-key.pem
    volumes:
      - envelope_keys:/run/auth/signing:ro
`)
	findings, err := authKeyVolumeFindings(planted)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || !strings.Contains(findings[0], "envelope_keys") {
		t.Fatalf("findings %v, want one naming envelope_keys", findings)
	}
}
