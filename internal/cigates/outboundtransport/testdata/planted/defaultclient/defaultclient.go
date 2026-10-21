package defaultclient

import (
	. "net/http"
)

func Fetch(url string) (*Response, error) { return DefaultClient.Get(url) }

func Probe(url string) (*Response, error) { return Head(url) }

func Values() []any { return []any{Get, Post, PostForm} }
