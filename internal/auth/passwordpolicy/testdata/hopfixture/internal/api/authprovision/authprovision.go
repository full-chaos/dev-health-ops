// Package authprovision is the fixture's go-api client of the internal routes.
package authprovision

const (
	PathUsersCreate   = "/internal/v1/users/create"
	PathUsersPassword = "/internal/v1/users/password"
)

func PostCreate(hash string) { post(PathUsersCreate, hash) }

func PostPassword(hash string) { post(PathUsersPassword, hash) }

func post(path, hash string) {}
