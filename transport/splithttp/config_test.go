package splithttp

import (
	"testing"

	"github.com/metacubex/http"
)

func TestApplyExtractMetaRoundTrip(t *testing.T) {
	tests := []struct {
		name             string
		sessionPlacement string
		seqPlacement     string
	}{
		{name: "path", sessionPlacement: PlacementPath, seqPlacement: PlacementPath},
		{name: "query", sessionPlacement: PlacementQuery, seqPlacement: PlacementQuery},
		{name: "header", sessionPlacement: PlacementHeader, seqPlacement: PlacementHeader},
		{name: "cookie", sessionPlacement: PlacementCookie, seqPlacement: PlacementCookie},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &SplitHTTPConfig{
				Path:             "/x",
				SessionPlacement: tc.sessionPlacement,
				SeqPlacement:     tc.seqPlacement,
			}

			req, err := http.NewRequest("POST", "https://example.com/x/", nil)
			if err != nil {
				t.Fatalf("new request failed: %v", err)
			}
			req.Header = http.Header{}

			cfg.ApplyMetaToRequest(req, "sess-1", "7")
			session, seq := cfg.ExtractMetaFromRequest(req, cfg.GetNormalizedPath())
			if session != "sess-1" {
				t.Fatalf("session mismatch: got %q want %q", session, "sess-1")
			}
			if seq != "7" {
				t.Fatalf("seq mismatch: got %q want %q", seq, "7")
			}
		})
	}
}

func TestHasTCPFallback(t *testing.T) {
	if !(&SplitHTTPConfig{}).HasTCPFallback() {
		t.Fatalf("empty alpn should allow tcp fallback")
	}
	if !(&SplitHTTPConfig{ALPN: []string{"h2"}}).HasTCPFallback() {
		t.Fatalf("h2 should allow tcp fallback")
	}
	if (&SplitHTTPConfig{ALPN: []string{"h3"}}).HasTCPFallback() {
		t.Fatalf("h3 only should not allow tcp fallback")
	}
}
