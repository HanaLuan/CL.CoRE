package inbound

import "testing"

func TestVlessXHTTPConfigBuildPropagatesPaddingMethod(t *testing.T) {
	config := XHTTPConfig{
		XPaddingMethod:       "tokenish",
		ServerMaxHeaderBytes: 16384,
	}

	built := config.Build()
	if built.XPaddingMethod != config.XPaddingMethod {
		t.Fatalf("unexpected padding method: got %q want %q", built.XPaddingMethod, config.XPaddingMethod)
	}
	if built.ServerMaxHeaderBytes != config.ServerMaxHeaderBytes {
		t.Fatalf("unexpected server max header bytes: got %d want %d", built.ServerMaxHeaderBytes, config.ServerMaxHeaderBytes)
	}
}
