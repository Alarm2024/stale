package serve_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Alarm2024/stale/internal/serve"
)

func TestSendTransactionRefused(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream must not receive sendTransaction")
	}))
	defer upstream.Close()

	s, err := serve.New(serve.Options{Upstream: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}

	reqBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	var out struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error.Message != "stale proxy is read-only" {
		t.Fatalf("error message = %q", out.Error.Message)
	}
}

func TestRequestAirdropRefused(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream must not receive requestAirdrop")
	}))
	defer upstream.Close()

	s, err := serve.New(serve.Options{Upstream: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}

	reqBody := []byte(`{"jsonrpc":"2.0","id":2,"method":"requestAirdrop","params":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	var out struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error.Message != "stale proxy is read-only" {
		t.Fatalf("error message = %q", out.Error.Message)
	}
}

func TestProxyForwardsReadOnlyMethods(t *testing.T) {
	const want = `{"jsonrpc":"2.0","id":1,"result":"ok"}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}` {
			t.Fatalf("unexpected body: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(want))
	}))
	defer upstream.Close()

	s, err := serve.New(serve.Options{Upstream: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}

	reqBody := []byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHealthJSON(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": 100})
	}))
	defer upstream.Close()

	ref := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": 100})
	}))
	defer ref.Close()

	s, err := serve.New(serve.Options{Upstream: upstream.URL, RefURL: ref.URL})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	var out map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["verdict"] != "UNKNOWN" {
		t.Fatalf("before any sample, health must say UNKNOWN, got %#v", out)
	}
	if out["lag_slots"] != nil {
		t.Fatalf("before any sample there is no lag to report, got lag_slots=%v", out["lag_slots"])
	}
}

// Before the sampler has read anything, a forwarded response used to carry an
// empty verdict next to "lag 0" -- the best possible score, for no reading.
func TestForwardedResponseBeforeFirstSampleSaysUnknown(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":1}`))
	}))
	defer upstream.Close()
	ref := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ref.Close()

	s, err := serve.New(serve.Options{Upstream: upstream.URL, RefURL: ref.URL})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot"}`)))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Stale-Verdict"); got != "UNKNOWN" {
		t.Fatalf("X-Stale-Verdict = %q, want UNKNOWN", got)
	}
	if got := rec.Header().Get("X-Stale-Lag-Slots"); got != "" {
		t.Fatalf("X-Stale-Lag-Slots = %q, want absent before any sample", got)
	}
}

func TestServeRefusesToMeasureAnEndpointAgainstItself(t *testing.T) {
	_, err := serve.New(serve.Options{
		Upstream: "https://api.mainnet-beta.solana.com",
		RefURL:   "https://API.mainnet-beta.solana.com/",
	})
	if err == nil {
		t.Fatal("serve must refuse an upstream that is its own reference")
	}
}

// A strict upstream reads the exact "method" key. encoding/json matches keys
// case-insensitively and lets the last one win, so a body carrying both
// "method" and "Method" used to show the guard one method and the upstream
// another. Every ambiguous shape is refused before anything is forwarded.
func TestAmbiguousMethodIsRefusedNotForwarded(t *testing.T) {
	cases := map[string]string{
		"case variant after":  `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","Method":"getSlot","params":[]}`,
		"case variant before": `{"jsonrpc":"2.0","id":1,"METHOD":"getSlot","method":"sendTransaction","params":[]}`,
		"only a case variant": `{"jsonrpc":"2.0","id":1,"Method":"getSlot","params":[]}`,
		"duplicate method":    `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","method":"getSlot","params":[]}`,
		"method not a string": `{"jsonrpc":"2.0","id":1,"method":["sendTransaction"],"params":[]}`,
		"trailing object":     `{"jsonrpc":"2.0","id":1,"method":"getSlot"}{"method":"sendTransaction"}`,
		"batch array":         `[{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":[]}]`,
		"write, other case":   `{"jsonrpc":"2.0","id":1,"method":"SendTransaction","params":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				t.Fatalf("upstream must not receive an ambiguous request: %s", b)
			}))
			defer upstream.Close()
			ref := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
			defer ref.Close()

			s, err := serve.New(serve.Options{Upstream: upstream.URL, RefURL: ref.URL})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
			rec := httptest.NewRecorder()
			s.Handler().ServeHTTP(rec, req)
			if rec.Code == http.StatusOK && !bytes.Contains(rec.Body.Bytes(), []byte("read-only")) {
				t.Fatalf("status %d body %q: ambiguous request was not refused", rec.Code, rec.Body.String())
			}
		})
	}
}
