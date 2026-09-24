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

	s, err := serve.New(serve.Options{Upstream: upstream.URL, RefURL: upstream.URL})
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
	if out["verdict"] == "FRESH" {
		t.Fatalf("health must not report FRESH without measurement: %#v", out)
	}
}
