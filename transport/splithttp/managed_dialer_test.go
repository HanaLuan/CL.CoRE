package splithttp

import (
	"reflect"
	"testing"
)

func TestCandidateHTTPVersions(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *SplitHTTPConfig
		wanted []string
	}{
		{
			name:   "default to h2",
			cfg:    &SplitHTTPConfig{},
			wanted: []string{"2"},
		},
		{
			name: "h3 and h2 enabled",
			cfg: &SplitHTTPConfig{
				ALPN:    []string{"h3", "h2"},
				TryQUIC: true,
			},
			wanted: []string{"3", "2"},
		},
		{
			name: "h3 only without quic",
			cfg: &SplitHTTPConfig{
				ALPN:    []string{"h3"},
				TryQUIC: false,
			},
			wanted: []string{"2"},
		},
		{
			name: "h1 only",
			cfg: &SplitHTTPConfig{
				ALPN: []string{"http/1.1"},
			},
			wanted: []string{"2", "1.1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := candidateHTTPVersions(tc.cfg)
			if !reflect.DeepEqual(got, tc.wanted) {
				t.Fatalf("versions mismatch: got %v want %v", got, tc.wanted)
			}
		})
	}
}
