package githubapp

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Config is github_app_config.py: the App's identity and OAuth client, read
// from the process environment when the routes are built (Python reads each
// per request, so a change needs a restart here). An empty value is unset,
// as Python's truthiness checks treat it.
type Config struct {
	Slug         string
	AppID        string
	ClientID     string
	ClientSecret string
	CallbackURL  string
	// PrivateKey is github_app_private_key(): the key text, "" when neither
	// source is configured, or an error when the configured key file cannot
	// be read.
	PrivateKey func() (string, error)
}

// Deps is what the routes need.
type Deps struct {
	Pool   *pgxpool.Pool
	Guard  *policy.Guard
	Valkey valkeygo.Client
	Cipher credentials.Cipher
	Logger *slog.Logger
	Now    func() time.Time
	Config Config
	Signer Signer
	// HTTPClient carries the two GitHub calls of the callback (nil: a client
	// with a 10 s timeout that follows no redirects, as httpx does).
	// GitHubURL and GitHubAPIURL are the two hosts (empty: github.com and
	// api.github.com); the venue oracle points them at a stub.
	HTTPClient   *http.Client
	GitHubURL    string
	GitHubAPIURL string
}

type handlers struct {
	Deps
	saver credentials.Saver
}

// Routes returns the two install routes.
func Routes(deps Deps) []httpapi.Route {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.HTTPClient == nil {
		deps.HTTPClient = &http.Client{Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if deps.GitHubURL == "" {
		deps.GitHubURL = "https://github.com"
	}
	if deps.GitHubAPIURL == "" {
		deps.GitHubAPIURL = "https://api.github.com"
	}
	h := handlers{Deps: deps, saver: credentials.NewSaver(deps.Cipher, deps.Now)}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: "/api/v1/admin/integrations/github/install-url",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.installURL))},
		{Method: http.MethodPost, Pattern: "/api/v1/admin/integrations/github/install-callback",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.installCallback))},
	}
}

func (h handlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.Logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", what), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

func detail(w http.ResponseWriter, status int, message string) {
	policy.WriteDetail(w, status, message, nil)
}

// installURL is POST /integrations/github/install-url.
func (h handlers) installURL(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var problems pybody.Errors
	var returnTo *string
	if !body.Missing {
		object, ok := problems.Object(body)
		if ok {
			if value, present := problems.OptionalString(object, "return_to", 0, 0); present {
				returnTo = &value
			}
		}
		if len(problems) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
			return
		}
	}
	if h.Config.Slug == "" {
		detail(w, http.StatusBadRequest, "GITHUB_APP_SLUG is not configured")
		return
	}
	canonical := canonicalizeReturnTo(returnTo)
	state, err := h.Signer.Mint(policy.UserFrom(r.Context()).OrgID, &canonical, h.Now())
	if err != nil {
		h.internal(w, r, "mint install state", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("install_url", installURL(h.Config.Slug, state, h.Config.CallbackURL))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// callbackBody is GitHubInstallCallbackRequest.
type callbackBody struct {
	installationID *big.Int
	setupAction    *string
	state          string
	code           string
}

func parseCallbackBody(body pybody.Body) (callbackBody, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return callbackBody{}, problems
	}
	var out callbackBody
	if _, present := object.Get("installation_id"); !present {
		problems = append(problems, pybody.Error{Type: "missing", Loc: []pyjson.Value{"body", "installation_id"}, Msg: "Field required", Input: object})
	} else if value, valid := problems.DefaultedMinInt(object, "installation_id", 1); valid {
		out.installationID = value
	}
	if value, present := problems.OptionalString(object, "setup_action", 0, 0); present {
		out.setupAction = &value
	}
	out.state, _ = problems.RequiredString(object, "state", 0, 0)
	out.code, _ = problems.OptionalString(object, "code", 0, 0)
	return out, problems
}

// installCallback is POST /integrations/github/install-callback.
func (h handlers) installCallback(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	request, problems := parseCallbackBody(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	ctx := r.Context()
	orgID := policy.UserFrom(ctx).OrgID
	verified, err := h.Signer.Verify(request.state, h.Now())
	if err != nil {
		detail(w, http.StatusBadRequest, err.Error())
		return
	}
	if verified.OrgID != orgID {
		detail(w, http.StatusForbidden, "GitHub App state org mismatch")
		return
	}
	if request.code == "" {
		detail(w, http.StatusBadRequest, "GitHub user authorization is required to complete the install; enable 'Request user authorization (OAuth) during installation' on the App")
		return
	}
	if h.consumeState(ctx, verified.JTI) {
		detail(w, http.StatusBadRequest, "GitHub App installation state already used")
		return
	}

	privateKey, keyErr := "", error(nil)
	if h.Config.PrivateKey != nil {
		privateKey, keyErr = h.Config.PrivateKey()
	}
	if keyErr != nil {
		detail(w, http.StatusInternalServerError, "GITHUB_APP_PRIVATE_KEY_PATH could not be read")
		return
	}
	if h.Config.AppID == "" || privateKey == "" {
		detail(w, http.StatusInternalServerError, "GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY or GITHUB_APP_PRIVATE_KEY_PATH are required")
		return
	}
	installation, outcome := h.verifyInstallerAccess(ctx, request.installationID, request.code)
	if outcome.err != nil {
		h.internal(w, r, "verify installer access", outcome.err)
		return
	}
	if outcome.status != 0 {
		detail(w, outcome.status, outcome.detail)
		return
	}

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.internal(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if conflict, err := h.upsertInstallation(ctx, tx, request.installationID, orgID, installation); err != nil {
		h.internal(w, r, "upsert installation", err)
		return
	} else if conflict {
		detail(w, http.StatusConflict, "installation already linked to another organization")
		return
	}

	creds := pyjson.NewObject()
	creds.Set("app_id", h.Config.AppID)
	creds.Set("private_key", privateKey)
	creds.Set("installation_id", request.installationID.String())
	config := pyjson.NewObject()
	config.Set("auth_mode", "github_app")
	config.Set("installation_id", pyjson.Int{Int: request.installationID})
	if request.setupAction != nil {
		config.Set("setup_action", *request.setupAction)
	} else {
		config.Set("setup_action", nil)
	}
	if err := h.saver.Set(ctx, tx, orgID, "github", "github-app", creds, config, true); err != nil {
		h.internal(w, r, "save credential", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internal(w, r, "commit", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("connected", true)
	out.Set("installation_id", pyjson.Int{Int: request.installationID})
	out.Set("credential_name", "github-app")
	out.Set("return_to", canonicalizeReturnTo(verified.ReturnTo))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// consumeState is _consume_install_state_jti, reporting whether the state was
// already used. The replay cache is defence in depth (GitHub's OAuth code is
// single use): an unavailable cache is logged and the callback proceeds.
func (h handlers) consumeState(ctx context.Context, jti string) bool {
	if h.Valkey == nil {
		h.Logger.WarnContext(ctx, "GitHub App installation state replay cache unavailable", slog.String("error", "no Valkey client"))
		return false
	}
	key := "github_app_install_state:" + jti
	_, err := h.Valkey.Do(ctx, h.Valkey.B().Get().Key(key).Build()).ToString()
	if err == nil {
		return true
	}
	if !valkeygo.IsValkeyNil(err) {
		h.Logger.WarnContext(ctx, "GitHub App installation state replay cache unavailable", slog.String("error", err.Error()))
		return false
	}
	set := h.Valkey.B().Set().Key(key).Value(`"used"`).Ex(stateTTL).Build()
	if err := h.Valkey.Do(ctx, set).Error(); err != nil {
		h.Logger.WarnContext(ctx, "GitHub App installation state replay cache unavailable", slog.String("error", err.Error()))
	}
	return false
}

// upsertInstallation is _upsert_installation: insert the row, or claim an
// existing one that is unowned or already this org's. conflict is true when
// another org holds it (409).
func (h handlers) upsertInstallation(ctx context.Context, tx pgx.Tx, installationID *big.Int, orgID string, installation *pyjson.Object) (conflict bool, err error) {
	if !installationID.IsInt64() {
		return false, errors.New("installation id is outside the bigint range")
	}
	id := installationID.Int64()
	login, accountType := accountField(installation, "login"), accountField(installation, "type")
	now := h.Now().UTC().Truncate(time.Microsecond)

	var existing string
	err = tx.QueryRow(ctx, `SELECT id::text FROM github_app_installations WHERE installation_id = $1`, id).Scan(&existing)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		inserted, insertErr := h.insertInstallation(ctx, tx, id, orgID, login, accountType, now)
		if insertErr != nil {
			return false, insertErr
		}
		if inserted {
			return false, nil
		}
	case err != nil:
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE github_app_installations
		SET org_id = $1, account_login = $2, account_type = $3, suspended_at = NULL, updated_at = $4
		WHERE installation_id = $5 AND (org_id IS NULL OR org_id = $1)`, orgID, login, accountType, now, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 0, nil
}

// insertInstallation inserts inside a savepoint; a unique violation (another
// request inserted it first) reports inserted=false so the caller claims the
// row instead, and only if it is really there.
func (h handlers) insertInstallation(ctx context.Context, tx pgx.Tx, id int64, orgID string, login, accountType *string, now time.Time) (bool, error) {
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return false, err
	}
	_, err = savepoint.Exec(ctx, `INSERT INTO github_app_installations (id, installation_id, account_login, account_type, org_id, suspended_at, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, NULL, $5, $5)`, id, login, accountType, orgID, now)
	if err == nil {
		return true, savepoint.Commit(ctx)
	}
	_ = savepoint.Rollback(ctx)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		var raced string
		if lookup := tx.QueryRow(ctx, `SELECT id::text FROM github_app_installations WHERE installation_id = $1`, id).Scan(&raced); lookup != nil {
			return false, err
		}
		return false, nil
	}
	return false, err
}

// accountField is _account_field: installation["account"][field] when both
// are as expected, else nil.
func accountField(installation *pyjson.Object, field string) *string {
	if installation == nil {
		return nil
	}
	rawAccount, _ := installation.Get("account")
	account, ok := rawAccount.(*pyjson.Object)
	if !ok {
		return nil
	}
	value, _ := account.Get(field)
	if text, ok := value.(string); ok {
		return &text
	}
	return nil
}
