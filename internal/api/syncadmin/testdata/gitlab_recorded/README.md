# Recorded GitLab responses

Real responses from gitlab.com for `GET /api/v4/groups/:id/projects`,
recorded 2026-09-24T11:15Z without a credential (public groups). Each
`<name>.status` is the HTTP status, `<name>.headers` the response headers
the listing reads (content type and pagination), and `<name>.body` the
raw body. The sync config writes' venue oracle serves them to both
planes from one fake GitLab.

Capture commands (`curl -sS -D <headers> -o <name>.body`):

- `maven_page1`: `https://gitlab.com/api/v4/groups/gitlab-examples%2Fmaven/projects?page=1&per_page=2`
- `maven_page2`: `https://gitlab.com/api/v4/groups/gitlab-examples%2Fmaven/projects?page=2&per_page=2`
- `empty_group`: `https://gitlab.com/api/v4/groups/gitlab-examples%2Fops/projects?page=1&per_page=2`
- `missing_group`: `https://gitlab.com/api/v4/groups/gitlab-examples%2Fno-such-group-for-venue-test/projects?page=1&per_page=2`
- `invalid_token`: the `maven_page1` URL with the header
  `PRIVATE-TOKEN: invalid-venue-test-token-not-a-credential` (a fixed test
  string, not a credential)

The headers kept are `content-type`, `link`, `x-next-page`, `x-page`,
`x-per-page`, `x-prev-page`, `x-total` and `x-total-pages`.
