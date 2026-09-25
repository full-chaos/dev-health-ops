package credentials

import (
	"context"
	"errors"
	"math/big"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/api/gitlabcode"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// repoCredential is what list_credential_repos reads of a stored credential:
// its provider, its config (nil is an absent or falsy one, Python's `or {}`)
// and its decrypted payload as json.loads gave it.
type repoCredential struct {
	provider  string
	config    pyjson.Value
	decrypted pyjson.Value
}

// lookupForRepos is get_decrypted_credentials_by_id: nil when the id is not a
// UUID, names no credential of the org, the row has no payload, the payload
// cannot be read, or it decodes to JSON null (`decrypted is None`). A missing
// encryption key is Python's uncaught RuntimeError, returned as an error.
func (h handlers) lookupForRepos(ctx context.Context, orgID, credentialID string) (*repoCredential, error) {
	id, ok := policy.ParsePyUUID(credentialID)
	if !ok {
		return nil, nil
	}
	var provider string
	var config, ciphertext *string
	err := h.pool.QueryRow(ctx, `SELECT provider, config::text, credentials_encrypted FROM integration_credentials WHERE org_id = $1 AND id = $2`,
		orgID, id).Scan(&provider, &config, &ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if ciphertext == nil || *ciphertext == "" {
		return nil, nil
	}
	decrypted, readable, err := h.decryptValue(*ciphertext)
	if err != nil {
		return nil, err
	}
	if !readable {
		RecordDecryptFailed(ctx, provider)
		return nil, nil
	}
	if decrypted == nil {
		return nil, nil
	}
	row := &repoCredential{provider: provider, decrypted: decrypted}
	if config != nil {
		if row.config, err = pyjson.DecodeString(*config); err != nil {
			return nil, err
		}
	}
	return row, nil
}

// errNotADict stands for the AttributeError or TypeError Python raises when a
// payload or config that is not a dict is read as one.
var errNotADict = errors.New("stored credential value is not a dict")

// repos is GET /credentials/{credential_id}/repos, which Python registers
// before the {provider}/{name} read and so wins that route's shape. Only
// GitLab is listed by this plane; a GitHub credential is refused with 501.
func (h handlers) repos(w http.ResponseWriter, r *http.Request) {
	var problems pybody.Errors
	query := r.URL.Query()
	owner := pybody.LastQueryValue(query, "owner")
	search := pybody.LastQueryValue(query, "search")
	maxRepos, _ := problems.QueryInt("max_repos", pybody.LastQueryValue(query, "max_repos"), 100, nil, nil)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	ctx := r.Context()
	credential, err := h.lookupForRepos(ctx, orgIDOf(ctx), r.PathValue("provider"))
	if err != nil {
		h.internal(w, r, "look up credential for repo listing", err)
		return
	}
	if credential == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
		return
	}
	switch credential.provider {
	case "github":
		policy.WriteDetail(w, http.StatusNotImplemented, "Repository listing is not served by this API plane", nil)
	case "gitlab":
		h.listGitLabRepos(w, r, credential, str(owner), str(search), maxRepos)
	default:
		policy.WriteDetail(w, http.StatusBadRequest, "Repo listing not supported for provider: "+credential.provider, nil)
	}
}

// str is a query value with None as the empty string, which every use here
// treats as Python's falsy.
func str(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// dictGet is `mapping.get(key)` on a value Python reads as a dict: a
// non-dict is the AttributeError Python raises.
func dictGet(value pyjson.Value, key string) (pyjson.Value, error) {
	object, ok := value.(*pyjson.Object)
	if !ok {
		return nil, errNotADict
	}
	got, _ := object.Get(key)
	return got, nil
}

// configGet is `config.get(key)` for `config = credential.config or {}`.
func (c *repoCredential) configGet(key string) (pyjson.Value, error) {
	if !pyjson.Truthy(c.config) {
		return nil, nil
	}
	return dictGet(c.config, key)
}

// listGitLabRepos is the gitlab branch of list_credential_repos.
func (h handlers) listGitLabRepos(w http.ResponseWriter, r *http.Request, credential *repoCredential, owner, search string, maxRepos *big.Int) {
	ctx := r.Context()
	fail := func(err error) { h.internal(w, r, "list gitlab repositories", err) }
	token, err := dictGet(credential.decrypted, "token")
	if err != nil {
		fail(err)
		return
	}
	// url = _string_value(decrypted url) or ... or "https://gitlab.com",
	// each later source read only when the earlier ones gave nothing.
	baseURL := ""
	for _, source := range []struct {
		fromConfig bool
		key        string
	}{{false, "url"}, {false, "base_url"}, {true, "url"}, {true, "base_url"}} {
		var value pyjson.Value
		if source.fromConfig {
			value, err = credential.configGet(source.key)
		} else {
			value, err = dictGet(credential.decrypted, source.key)
		}
		if err != nil {
			fail(err)
			return
		}
		if baseURL = stringValue(value); baseURL != "" {
			break
		}
	}
	if baseURL == "" {
		baseURL = defaultGitLabBase
	}
	if !pyjson.Truthy(token) {
		policy.WriteDetail(w, http.StatusBadRequest, "GitLab credential missing token", nil)
		return
	}
	if valid, detail := externalurl.Validate(ctx, baseURL, h.lookup); !valid {
		policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
		return
	}
	group := owner
	if group == "" {
		configured, err := credential.configGet("group")
		if err != nil {
			fail(err)
			return
		}
		group = stringValue(configured)
	}
	options := gitlabcode.ListOptions{Membership: group == "", MaxProjects: maxRepos}
	if group != "" {
		options.Group, options.Search = &group, search
	} else {
		options.Pattern = pattern(search)
	}
	client := gitlabcode.Client{BaseURL: baseURL, Token: pyjson.Str(token), HTTP: h.repoClient}
	projects, err := client.ListProjects(ctx, options)
	if err != nil {
		var listErr *gitlabcode.Error
		if errors.As(err, &listErr) {
			switch listErr.Class {
			case "NotFoundException":
				policy.WriteModel(w, http.StatusOK, repoListing("gitlab", nil), nil)
				return
			case "AuthenticationException":
				policy.WriteDetail(w, http.StatusUnauthorized, listErr.Message, nil)
				return
			case "RateLimitException":
				policy.WriteDetail(w, http.StatusTooManyRequests, listErr.Message, nil)
				return
			}
		}
		fail(err)
		return
	}
	repos := make([]pyjson.Value, 0, len(projects))
	for _, project := range projects {
		repo, err := discoveredRepo(project)
		if err != nil {
			fail(err)
			return
		}
		repos = append(repos, repo)
	}
	policy.WriteModel(w, http.StatusOK, repoListing("gitlab", repos), nil)
}

// pattern is `f"*{search}*" if search`: the glob a membership listing
// filters by, none for an empty search.
func pattern(search string) string {
	if search == "" {
		return ""
	}
	return "*" + search + "*"
}

// discoveredRepo is DiscoveredRepo(name, full_name, description, url): the
// description and the url must be a string or null, or pydantic's
// ValidationError is the request's 500.
func discoveredRepo(project gitlabcode.Project) (pyjson.Value, error) {
	for _, value := range []pyjson.Value{project.Description, project.URL} {
		if _, isString := value.(string); value != nil && !isString {
			return nil, errors.New("DiscoveredRepo: a description or url is not a string")
		}
	}
	out := pyjson.NewObject()
	out.Set("name", project.Name)
	out.Set("full_name", project.FullName)
	out.Set("description", project.Description)
	out.Set("url", project.URL)
	return out, nil
}

// repoListing is DiscoveredReposResponse.
func repoListing(provider string, repos []pyjson.Value) *pyjson.Object {
	if repos == nil {
		repos = []pyjson.Value{}
	}
	out := pyjson.NewObject()
	out.Set("provider", provider)
	out.Set("repos", repos)
	out.Set("total", int64(len(repos)))
	return out
}
