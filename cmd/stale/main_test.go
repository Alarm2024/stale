package main

import (
	"flag"
	"strings"
	"testing"
)

func TestLeftoverArgMessageClean(t *testing.T) {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.String("ref", "", "")
	if err := fs.Parse([]string{"--ref", "https://ref.example"}); err != nil {
		t.Fatal(err)
	}
	if msg := leftoverArgMessage("check", fs); msg != "" {
		t.Fatalf("expected no message for clean parse, got %q", msg)
	}
}

func TestLeftoverArgMessageSuggestsFlag(t *testing.T) {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.String("ref", "", "")
	fs.Int64("max-lag-slots", 0, "")
	// flag parsing stops at the first non-flag argument, so both are left over.
	_ = fs.Parse([]string{"ref=https://ref.example", "max_lag=5"})
	msg := leftoverArgMessage("check", fs)
	if !strings.Contains(msg, `unexpected argument "ref=https://ref.example"`) {
		t.Fatalf("message should name the leftover argument, got %q", msg)
	}
	if !strings.Contains(msg, "did you mean --ref?") {
		t.Fatalf("message should suggest the flag, got %q", msg)
	}
}

func TestLeftoverArgMessageUnknownArg(t *testing.T) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	_ = fs.Parse([]string{"bogus"})
	msg := leftoverArgMessage("serve", fs)
	if !strings.Contains(msg, `unexpected argument "bogus"`) {
		t.Fatalf("message should name the leftover argument, got %q", msg)
	}
	if strings.Contains(msg, "did you mean") {
		t.Fatalf("unknown argument should not get a flag suggestion, got %q", msg)
	}
}

func TestCheckRejectsLeftoverArgs(t *testing.T) {
	// The old docs' copy-paste shape: the settings after the URL survive flag
	// parsing and must fail loudly instead of silently measuring the defaults.
	code := runCheck([]string{"https://target.example", "ref=https://ref.example", "max_lag=5", "for=10s"})
	if code != 2 {
		t.Fatalf("expected exit 2, got %d", code)
	}
}

func TestServeRejectsLeftoverArgs(t *testing.T) {
	code := runServe([]string{"--upstream", "https://upstream.example", "extra"})
	if code != 2 {
		t.Fatalf("expected exit 2, got %d", code)
	}
}
