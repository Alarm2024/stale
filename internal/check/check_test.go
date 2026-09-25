package check

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Alarm2024/stale/internal/measure"
)

func TestRunCleanThenTimeoutPrintsUnknownLag(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if targetCalls.Add(1) >= 3 {
			time.Sleep(measure.CallTimeout + 100*time.Millisecond)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": 100})
	}))
	defer target.Close()
	ref := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": 200})
	}))
	defer ref.Close()

	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	verdict, err := Run(ctx, Options{TargetURL: target.URL, RefURL: ref.URL, MaxLag: 5, For: 2500 * time.Millisecond, Out: &out})
	if err != nil || verdict != measure.VerdictStale {
		t.Fatalf("Run verdict=%s err=%v, want STALE; output:\n%s", verdict, err, out.String())
	}
	var verdictLine string
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "verdict=") {
			verdictLine = line
		}
	}
	if !strings.Contains(verdictLine, "verdict=STALE") || !strings.Contains(verdictLine, "lag=unknown") ||
		!strings.Contains(verdictLine, "degraded=true") || strings.Contains(verdictLine, "lag=0") {
		t.Fatalf("unexpected verdict line: %q", verdictLine)
	}
}
