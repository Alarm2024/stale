package redact_test

import (
	"testing"

	"github.com/Alarm2024/stale/internal/redact"
)

func TestURL(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"https://api.mainnet-beta.solana.com", "https://api.mainnet-beta.solana.com"},
		{"https://rpc.example.com/v1/abc123?token=secret", "https://rpc.example.com"},
		{"http://127.0.0.1:8899/foo?key=deadbeef", "http://127.0.0.1:8899"},
		{"https://user:pass@host.example/path?q=1", "https://host.example"},
	}
	for _, tc := range tests {
		if got := redact.URL(tc.in); got != tc.want {
			t.Errorf("URL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
