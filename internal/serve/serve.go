package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/Alarm2024/stale/internal/measure"
	"github.com/Alarm2024/stale/internal/redact"
)

const readOnlyMessage = "stale proxy is read-only"

type Options struct {
	Listen   string
	Upstream string
	RefURL   string
	MaxLag   int64
}

type Server struct {
	opts    Options
	sampler *measure.Sampler
	proxy   *httputil.ReverseProxy
}

func New(opts Options) (*Server, error) {
	if opts.Listen == "" {
		opts.Listen = "127.0.0.1:8899"
	}
	if opts.RefURL == "" {
		opts.RefURL = measure.DefaultRefURL
	}
	if opts.MaxLag == 0 {
		opts.MaxLag = measure.DefaultMaxLag
	}

	upstream, err := url.Parse(opts.Upstream)
	if err != nil {
		return nil, fmt.Errorf("parse upstream: %w", err)
	}

	s := &Server{
		opts:    opts,
		sampler: measure.NewSampler(opts.Upstream, opts.RefURL, opts.MaxLag),
	}
	s.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = upstream.Scheme
			req.URL.Host = upstream.Host
			req.Host = upstream.Host
		},
		ModifyResponse: s.addStaleHeaders,
	}
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/", s.handleRPC)
	return mux
}

func (s *Server) Start(ctx context.Context) error {
	s.sampler.Start(ctx)

	srv := &http.Server{
		Addr:              s.opts.Listen,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		s.sampler.Stop()
	}()

	return srv.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.writeSnapshot(w)
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	method, err := jsonRPCMethod(body)
	if err != nil {
		http.Error(w, "invalid json-rpc", http.StatusBadRequest)
		return
	}
	if isWriteMethod(method) {
		writeReadOnlyError(w, body)
		return
	}

	r.Body = io.NopCloser(strings.NewReader(string(body)))
	s.proxy.ServeHTTP(w, r)
}

func (s *Server) addStaleHeaders(resp *http.Response) error {
	snap := s.sampler.Current()
	resp.Header.Set("X-Stale-Lag-Slots", fmt.Sprintf("%d", snap.LagSlots))
	resp.Header.Set("X-Stale-Sampled-Ms-Ago", fmt.Sprintf("%d", snap.SampledMsAgo))
	resp.Header.Set("X-Stale-Verdict", string(snap.Verdict))
	return nil
}

func (s *Server) writeSnapshot(w http.ResponseWriter) {
	snap := s.sampler.Current()
	if !snap.Measured && snap.Verdict == measure.VerdictFresh {
		snap.Verdict = measure.VerdictUnknown
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"upstream":        redact.URL(s.opts.Upstream),
		"reference":       redact.URL(s.opts.RefURL),
		"verdict":         snap.Verdict,
		"lag_slots":       snap.LagSlots,
		"sampled_ms_ago":  snap.SampledMsAgo,
		"target_slot":     snap.TargetSlot,
		"ref_slot":        snap.RefSlot,
		"target_advanced": snap.TargetAdvanced,
		"measured":        snap.Measured,
	})
}

type rpcRequest struct {
	Method string `json:"method"`
}

func jsonRPCMethod(body []byte) (string, error) {
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", err
	}
	return req.Method, nil
}

func isWriteMethod(method string) bool {
	switch method {
	case "sendTransaction", "requestAirdrop":
		return true
	default:
		return false
	}
}

func writeReadOnlyError(w http.ResponseWriter, body []byte) {
	var envelope struct {
		JSONRPC string `json:"jsonrpc"`
		ID      any    `json:"id"`
	}
	_ = json.Unmarshal(body, &envelope)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": envelope.JSONRPC,
		"id":      envelope.ID,
		"error": map[string]any{
			"code":    -32000,
			"message": readOnlyMessage,
		},
	})
}
