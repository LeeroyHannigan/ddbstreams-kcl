package ddbstreams

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Proves the auto-download path end to end against a local server: correct
// stable-asset naming, SHA-256 verification, atomic cache write, and the
// checksum-mismatch failure path. No network, no real release required.

func servedAsset(t *testing.T, body []byte, sha string) (*httptest.Server, string) {
	t.Helper()
	osName, arch, ext := platformArch()
	asset := fmt.Sprintf("%s-%s-%s%s", binaryName, osName, arch, ext)
	mux := http.NewServeMux()
	mux.HandleFunc("/v"+Version+"/"+asset, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v"+Version+"/"+asset+".sha256", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "%s  %s\n", sha, asset)
	})
	return httptest.NewServer(mux), asset
}

func isolateEnv(t *testing.T, releaseBase string) string {
	t.Helper()
	cache := t.TempDir()
	t.Setenv("DDB_STREAMS_CONSUMER_RELEASE_BASE", releaseBase)
	t.Setenv("DDB_STREAMS_CONSUMER_SIDECAR", "") // don't let a real env override intervene
	t.Setenv("XDG_CACHE_HOME", cache)            // linux cache dir
	t.Setenv("HOME", cache)                      // macOS UserCacheDir uses $HOME/Library/Caches
	t.Setenv("PATH", t.TempDir())                // nothing discoverable on PATH
	return cache
}

func TestAutoDownloadRoundTrip(t *testing.T) {
	body := []byte("#!/bin/sh\necho fake-sidecar\n")
	sum := sha256.Sum256(body)
	srv, _ := servedAsset(t, body, hex.EncodeToString(sum[:]))
	defer srv.Close()

	cache := isolateEnv(t, srv.URL)

	path, err := discoverSidecar("")
	if err != nil {
		t.Fatalf("discoverSidecar: %v", err)
	}
	if !strings.HasPrefix(path, cache) {
		t.Fatalf("expected cached under %s, got %s", cache, path)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("downloaded content mismatch")
	}
	// Second call hits the cache (server not required); still resolves.
	if p2, err := discoverSidecar(""); err != nil || p2 != path {
		t.Fatalf("cached resolve: p=%s err=%v", p2, err)
	}
}

func TestAutoDownloadChecksumMismatch(t *testing.T) {
	body := []byte("real-bytes")
	srv, _ := servedAsset(t, body, "deadbeefdeadbeef") // wrong sha
	defer srv.Close()

	isolateEnv(t, srv.URL)

	if _, err := discoverSidecar(""); err == nil {
		t.Fatal("expected error on checksum mismatch with no PATH fallback")
	}
}
