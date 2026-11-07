package livee2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Test-only stand-in for the Kubernetes API server of the workload exchange. auth-service takes its subject issuer from
// configuration (AUTH_WORKLOAD_EXCHANGE_KUBE_API_URL): in prod that is the cluster, in the live e2e it is this handler.
// It authenticates exactly one subject token. The product path is not changed.

const (
	tokenReviewPath    = "/apis/authentication.k8s.io/v1/tokenreviews"
	maxTokenReviewBody = 64 << 10
)

// TokenReviewConfig is what the fake answers for: one subject, one reviewer, one service account.
type TokenReviewConfig struct {
	Subject        string
	ReviewerToken  string
	Audience       string
	Namespace      string
	ServiceAccount string
	UID            string
}

// NewSubjectJWS returns a three-segment token with an exp claim. Its signature is not checked by anything: the fake
// TokenReview is the only authority, as the API server is in prod. The key is ephemeral and dropped.
func NewSubjectJWS(now time.Time, ttl time.Duration) (string, error) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"iss": "live-e2e-tokenreview", "iat": now.Unix(), "exp": now.Add(ttl).Unix()})
	if err != nil {
		return "", err
	}
	signing := header + "." + enc.EncodeToString(claims)
	return signing + "." + enc.EncodeToString(ed25519.Sign(key, []byte(signing))), nil
}

type reviewAnswer struct {
	APIVersion string       `json:"apiVersion"`
	Kind       string       `json:"kind"`
	Status     reviewStatus `json:"status"`
}

type reviewStatus struct {
	Authenticated bool        `json:"authenticated"`
	Audiences     []string    `json:"audiences,omitempty"`
	User          *reviewUser `json:"user,omitempty"`
}

type reviewUser struct {
	Username string `json:"username"`
	UID      string `json:"uid"`
}

// TokenReviewHandler serves POST /apis/authentication.k8s.io/v1/tokenreviews. A wrong reviewer bearer is 401; a subject
// other than cfg.Subject, or an audience list without cfg.Audience, is an unauthenticated review (201).
func TokenReviewHandler(cfg TokenReviewConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tokenReviewPath {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(bearer), []byte(cfg.ReviewerToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req struct {
			Spec struct {
				Token     string   `json:"token"`
				Audiences []string `json:"audiences"`
			} `json:"spec"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, maxTokenReviewBody)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		answer := reviewAnswer{APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview"}
		asked := false
		for _, a := range req.Spec.Audiences {
			asked = asked || a == cfg.Audience
		}
		if asked && subtle.ConstantTimeCompare([]byte(req.Spec.Token), []byte(cfg.Subject)) == 1 {
			answer.Status = reviewStatus{
				Authenticated: true, Audiences: []string{cfg.Audience},
				User: &reviewUser{Username: "system:serviceaccount:" + cfg.Namespace + ":" + cfg.ServiceAccount, UID: cfg.UID},
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(answer)
	})
}

// WriteNewFile creates path (mode 0600, must not exist) with content and a final newline.
func WriteNewFile(path, content string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("cannot create " + path)
	}
	if _, err := fmt.Fprintln(f, content); err != nil {
		_ = f.Close()
		return errors.New("cannot write " + path)
	}
	return f.Close()
}

func readTrimmed(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("cannot read " + path)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", errors.New(path + " is empty")
	}
	return value, nil
}

// ServeTokenReview serves the fake over TLS until ctx ends. The subject and the reviewer token come from files.
func ServeTokenReview(ctx context.Context, addr, certFile, keyFile, subjectFile, reviewerFile string, cfg TokenReviewConfig) error {
	var err error
	if cfg.Subject, err = readTrimmed(subjectFile); err != nil {
		return err
	}
	if cfg.ReviewerToken, err = readTrimmed(reviewerFile); err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: TokenReviewHandler(cfg), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServeTLS(certFile, keyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// ReviewAudience is the audience auth-service asks TokenReview to confirm (ops internal/auth/workloadexchange ReviewAudience).
const ReviewAudience = "dev-health-workload-exchange"

func newTestSecret(verb string) (string, error) {
	if verb == "subject-jws" {
		return NewSubjectJWS(time.Now().UTC(), time.Hour)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
