package redact

import (
	"net/url"
	"strings"
)

// URL returns scheme://host for RPC URLs, stripping paths, queries, and
// credentials. Invalid URLs are returned unchanged so callers never leak
// the original string on parse failure.
func URL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return raw
	}
	u.User = nil
	u.Path = ""
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.Scheme + "://" + u.Host
}
