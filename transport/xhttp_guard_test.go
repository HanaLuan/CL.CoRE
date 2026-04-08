package transport_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyXHTTPPackageStaysDeleted(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	legacyDir := filepath.Join(repoRoot, "xhttp")
	if entries, err := os.ReadDir(legacyDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				t.Fatalf("legacy transport/xhttp directory must stay empty, found subdir %q", entry.Name())
			}
			if strings.HasSuffix(entry.Name(), ".go") {
				t.Fatalf("legacy transport/xhttp Go file must stay deleted, found %q", entry.Name())
			}
		}
	}

	err = filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			base := filepath.Base(path)
			if base == ".git" || base == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || filepath.Base(path) == "xhttp_guard_test.go" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), `"github.com/metacubex/mihomo/transport/xhttp"`) {
			t.Fatalf("legacy transport/xhttp import reintroduced in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
}
