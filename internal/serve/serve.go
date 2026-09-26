package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	if measure.SameEndpoint(opts.Upstream, opts.RefURL) {
		return nil, measure.ErrSameEndpoint
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

// addStaleHeaders stamps a forwarded response with the background sampler's
// latest reading. The headers describe that reading -- the slot lag of the
// upstream's getSlot against the reference -- not the age of this response.
// A header is only set when there is a number behind it.
func (s *Server) addStaleHeaders(resp *http.Response) error {
	snap := s.sampler.Current()
	resp.Header.Set("X-Stale-Verdict", string(snap.Verdict))
	resp.Header.Set("X-Stale-Degraded", fmt.Sprintf("%t", snap.Degraded))
	if snap.LagKnown {
		resp.Header.Set("X-Stale-Lag-Slots", fmt.Sprintf("%d", snap.LagSlots))
	}
	if snap.HasSample {
		resp.Header.Set("X-Stale-Sampled-Ms-Ago", fmt.Sprintf("%d", snap.SampledMsAgo))
	}
	return nil
}

func (s *Server) writeSnapshot(w http.ResponseWriter) {
	snap := s.sampler.Current()
	var lag, ago any
	if snap.LagKnown {
		lag = snap.LagSlots
	}
	if snap.HasSample {
		ago = snap.SampledMsAgo
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"upstream":        redact.URL(s.opts.Upstream),
		"reference":       redact.URL(s.opts.RefURL),
		"verdict":         snap.Verdict,
		"degraded":        snap.Degraded,
		"lag_slots":       lag,
		"sampled_ms_ago":  ago,
		"target_slot":     snap.TargetSlot,
		"ref_slot":        snap.RefSlot,
		"target_advanced": snap.TargetAdvanced,
		"ref_behind":      snap.RefBehind,
		"measured":        snap.Measured,
	})
}

var (
	errNotObject       = errors.New("json-rpc request must be a single JSON object")
	errNoMethod        = errors.New("json-rpc request has no method")
	errAmbiguousMethod = errors.New("json-rpc request names its method more than once, or in a different case")
	errTrailingData    = errors.New("json-rpc request has data after the object")
)

// jsonRPCMethod reads the method the upstream will act on, and refuses any
// body where that could differ from what this guard sees.
//
// encoding/json matches struct keys case-insensitively and keeps the last
// duplicate, while a strict upstream reads the exact "method" key and may keep
// the first. So {"method":"sendTransaction","Method":"getSlot"} showed this
// guard getSlot and the upstream sendTransaction. Walking the tokens, this
// accepts exactly one key spelled "method", with a string value, in one
// object with nothing after it -- and refuses everything else.
func jsonRPCMethod(body []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return "", err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return "", errNotObject
	}
	var method string
	seen := 0
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return "", err
		}
		key, _ := kt.(string)
		if strings.EqualFold(key, "method") {
			seen++
			if key != "method" || seen > 1 {
				return "", errAmbiguousMethod
			}
			if err := dec.Decode(&method); err != nil {
				return "", err
			}
			continue
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return "", err
		}
	}
	if _, err := dec.Token(); err != nil { // the closing '}'
		return "", err
	}
	if _, err := dec.Token(); err != io.EOF {
		return "", errTrailingData
	}
	if seen == 0 || method == "" {
		return "", errNoMethod
	}
	return method, nil
}

// isWriteMethod names the Solana RPC methods that change chain state. Compared
// without case, so an upstream that is lenient about case cannot be reached.
func isWriteMethod(method string) bool {
	for _, w := range []string{"sendTransaction", "requestAirdrop"} {
		if strings.EqualFold(method, w) {
			return true
		}
	}
	return false
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
