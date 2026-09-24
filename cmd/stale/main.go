package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Alarm2024/stale/internal/check"
	"github.com/Alarm2024/stale/internal/measure"
	"github.com/Alarm2024/stale/internal/redact"
	"github.com/Alarm2024/stale/internal/serve"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "check":
		os.Exit(runCheck(os.Args[2:]))
	case "serve":
		os.Exit(runServe(os.Args[2:]))
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `stale — measure how old a Solana RPC's answers really are

Usage:
  stale check <rpc-url> [--ref URL] [--max-lag-slots N] [--for DURATION]
  stale serve --upstream URL [--listen ADDR] [--ref URL]

Commands:
  check   Sample getSlot on target vs reference and print a verdict
  serve   Read-only JSON-RPC proxy with stale headers on every response

Verdicts: FRESH (0), STALE (1), UNKNOWN (2)
`)
}

func runCheck(args []string) int {
	targetURL, flagArgs := splitRPCURL(args)
	if targetURL == "" {
		fmt.Fprintln(os.Stderr, "check requires exactly one RPC URL argument")
		return 2
	}

	fs := flag.NewFlagSet("check", flag.ExitOnError)
	ref := fs.String("ref", measure.DefaultRefURL, "reference RPC URL")
	maxLag := fs.Int64("max-lag-slots", measure.DefaultMaxLag, "maximum acceptable lag in slots")
	forDur := fs.Duration("for", measure.DefaultSampleFor, "sampling window")
	_ = fs.Parse(flagArgs)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	verdict, err := check.Run(ctx, check.Options{
		TargetURL: targetURL,
		RefURL:    *ref,
		MaxLag:    *maxLag,
		For:       *forDur,
		Out:       os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "check error: %v\n", err)
		return measure.VerdictUnknown.ExitCode()
	}
	return verdict.ExitCode()
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8899", "listen address")
	upstream := fs.String("upstream", "", "upstream RPC URL (required)")
	ref := fs.String("ref", measure.DefaultRefURL, "reference RPC URL")
	maxLag := fs.Int64("max-lag-slots", measure.DefaultMaxLag, "maximum acceptable lag in slots")
	_ = fs.Parse(args)

	if *upstream == "" {
		fmt.Fprintln(os.Stderr, "serve requires --upstream URL")
		return 2
	}

	s, err := serve.New(serve.Options{
		Listen:   *listen,
		Upstream: *upstream,
		RefURL:   *ref,
		MaxLag:   *maxLag,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "serve error: %v\n", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("stale serve listening on %s upstream=%s\n", *listen, redact.URL(*upstream))
	if err := s.Start(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "serve error: %v\n", err)
		return 2
	}
	return 0
}

// splitRPCURL pulls the RPC endpoint out before flag parsing so hostnames like
// mainnet-beta.solana.com are not mistaken for flags.
func splitRPCURL(args []string) (string, []string) {
	var target string
	var rest []string
	for _, arg := range args {
		if target == "" && (strings.HasPrefix(arg, "http://") || strings.HasPrefix(arg, "https://")) {
			target = arg
			continue
		}
		rest = append(rest, arg)
	}
	return target, rest
}
