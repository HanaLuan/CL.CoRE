package splithttp

import (
	"bytes"
	"strings"
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

func TestFillStreamRequestNoGRPCHeader(t *testing.T) {
	tests := []struct {
		name     string
		config   *SplitHTTPConfig
		wantType string
	}{
		{
			name:     "default grpc header",
			config:   &SplitHTTPConfig{},
			wantType: "application/grpc",
		},
		{
			name:     "disabled grpc header",
			config:   &SplitHTTPConfig{NoGRPCHeader: true},
			wantType: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "https://example.com/xhttp/", bytes.NewReader([]byte("payload")))
			if err != nil {
				t.Fatalf("new request failed: %v", err)
			}

			tc.config.FillStreamRequest(req, "session-1")
			if got := req.Header.Get("Content-Type"); got != tc.wantType {
				t.Fatalf("unexpected content-type: got %q want %q", got, tc.wantType)
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

func TestXPaddingRequestRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		config    *SplitHTTPConfig
		wantPlace string
	}{
		{
			name: "default referer",
			config: &SplitHTTPConfig{
				XPaddingBytes: &RangeConfig{From: 8, To: 8},
			},
			wantPlace: PlacementQueryInHeader,
		},
		{
			name: "obfs header",
			config: &SplitHTTPConfig{
				XPaddingBytes:     &RangeConfig{From: 8, To: 8},
				XPaddingObfsMode:  true,
				XPaddingPlacement: PlacementHeader,
				XPaddingHeader:    "X-Test-Padding",
			},
			wantPlace: PlacementHeader,
		},
		{
			name: "obfs cookie",
			config: &SplitHTTPConfig{
				XPaddingBytes:     &RangeConfig{From: 8, To: 8},
				XPaddingObfsMode:  true,
				XPaddingPlacement: PlacementCookie,
				XPaddingKey:       "x_pad",
			},
			wantPlace: PlacementCookie,
		},
		{
			name: "obfs query in header",
			config: &SplitHTTPConfig{
				XPaddingBytes:     &RangeConfig{From: 8, To: 8},
				XPaddingObfsMode:  true,
				XPaddingPlacement: PlacementQueryInHeader,
				XPaddingHeader:    "Referer",
				XPaddingKey:       "x_pad",
			},
			wantPlace: PlacementQueryInHeader,
		},
		{
			name: "obfs tokenish query",
			config: &SplitHTTPConfig{
				XPaddingBytes:     &RangeConfig{From: 8, To: 8},
				XPaddingObfsMode:  true,
				XPaddingPlacement: PlacementQuery,
				XPaddingKey:       "x_pad",
				XPaddingMethod:    string(PaddingMethodTokenish),
			},
			wantPlace: PlacementQuery,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "https://example.com/xhttp/", nil)
			if err != nil {
				t.Fatalf("new request failed: %v", err)
			}

			tc.config.FillStreamRequest(req, "session-1")
			padding, placement := tc.config.ExtractXPaddingFromRequest(req, tc.config.XPaddingObfsMode)
			if padding == "" {
				t.Fatal("expected padding to round-trip")
			}
			if !strings.Contains(placement, tc.wantPlace) {
				t.Fatalf("unexpected padding placement: got %q want containing %q", placement, tc.wantPlace)
			}
			validRange := tc.config.GetNormalizedXPaddingBytes()
			if !tc.config.IsPaddingValid(padding, validRange.From, validRange.To, PaddingMethod(tc.config.XPaddingMethod)) {
				t.Fatalf("expected padding %q from %s to validate", padding, placement)
			}
		})
	}
}
