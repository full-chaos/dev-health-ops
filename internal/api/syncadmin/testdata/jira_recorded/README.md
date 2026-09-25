# Recorded Jira Cloud project search pages

The sync config create venue oracle serves these pages from a fake Jira that both planes call for a Jira config without an explicit project.

## Source

The pages were recorded on 2026-09-24T23:59:52Z from a public Jira Cloud site that answers without credentials:

```sh
for s in 0 50 100 150 200 250; do
  curl -s "https://redhat.atlassian.net/rest/api/3/project/search?startAt=$s&maxResults=50&expand=description%2Clead" > search_$s.json
done
```

The site reported 292 projects; the last page (`startAt` 250) has `isLast: true`.

## Changes to the recordings

- **Project leads.** Each project's `lead` holds a person. Each distinct person is replaced with a stable placeholder:
  - `accountId` becomes `redacted-account-<n>`;
  - `displayName` becomes `Project Lead <n>`;
  - the user `self` URL names the placeholder;
  - `avatarUrls` is `{}`;
  - every other lead field is kept.
- **JSON form.** Each file is rewritten as compact JSON. Every other field is kept as recorded.

## How the fake serves the pages

The fake concatenates the recorded `values` in `startAt` order and answers `/rest/api/3/project/search` with the window that the request's `startAt` and `maxResults` name. The envelope follows the real site's (checked with `maxResults=100` on the same date):
- `self`, `maxResults`, `startAt`, `total` and `isLast`;
- `nextPage` only when the window is not the last.

The Python client asks for 50 projects a page and the Go client for 100, so neither plane sees the recorded page boundaries verbatim.
