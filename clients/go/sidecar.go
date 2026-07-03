package ddbstreams

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const binaryName = "amazon-dynamodb-streams-consumer-sidecar"

// releaseBaseURL is where per-platform sidecar assets are published. Overridable
// via DDB_STREAMS_CONSUMER_RELEASE_BASE for testing / mirrors.
const releaseBaseURL = "https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/download"

// discoverSidecar resolves the sidecar binary, in order:
//  1. explicit path argument,
//  2. DDB_STREAMS_CONSUMER_SIDECAR env override,
//  3. cached binary from a previous download,
//  4. transparent download from the matching GitHub Release (checksum-verified),
//  5. the binary on PATH.
//
// Unlike Python (whose wheel bundles the binary), Go fetches source, so the
// binary is materialized on first use and cached — from the user's seat it is
// still "import and go".
func discoverSidecar(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if env := os.Getenv("DDB_STREAMS_CONSUMER_SIDECAR"); env != "" {
		return env, nil
	}

	cached := cachePath()
	if fi, err := os.Stat(cached); err == nil && !fi.IsDir() {
		return cached, nil
	}

	if path, err := downloadSidecar(cached); err == nil {
		return path, nil
	} else if p, perr := lookPath(); perr == nil {
		return p, nil // fall back to PATH if download failed
	} else {
		return "", fmt.Errorf(
			"could not obtain the %q sidecar: download failed (%v) and it is not on PATH. "+
				"Set DDB_STREAMS_CONSUMER_SIDECAR=/path/to/sidecar or install it manually",
			binaryName, err)
	}
}

func lookPath() (string, error) {
	// Minimal PATH lookup without importing os/exec's LookPath semantics here.
	name := binaryName
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if dir == "" {
			continue
		}
		p := filepath.Join(dir, name)
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("%s not found on PATH", name)
}

// platformArch maps Go's GOOS/GOARCH to the release asset naming
// (x86_64 / aarch64), matching the wheel platform tags.
func platformArch() (goos, arch, ext string) {
	goos = runtime.GOOS
	switch runtime.GOARCH {
	case "amd64":
		arch = "x86_64"
	case "arm64":
		arch = "aarch64"
	default:
		arch = runtime.GOARCH
	}
	if goos == "windows" {
		ext = ".exe"
	}
	return goos, arch, ext
}

func cachePath() string {
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		base = filepath.Join(os.TempDir(), "ddb-streams-consumer-cache")
	}
	_, _, ext := platformArch()
	return filepath.Join(base, "amazon-dynamodb-streams-consumer", Version, binaryName+ext)
}

func assetURL() (bin, sha string) {
	base := os.Getenv("DDB_STREAMS_CONSUMER_RELEASE_BASE")
	if base == "" {
		base = releaseBaseURL
	}
	goos, arch, ext := platformArch()
	asset := fmt.Sprintf("%s-%s-%s%s", binaryName, goos, arch, ext)
	u := fmt.Sprintf("%s/v%s/%s", strings.TrimRight(base, "/"), Version, asset)
	return u, u + ".sha256"
}

// downloadSidecar fetches the platform sidecar to dst, verifying its SHA-256
// against the published .sha256, and marks it executable.
func downloadSidecar(dst string) (string, error) {
	binURL, shaURL := assetURL()

	wantSum, err := fetchString(shaURL)
	if err != nil {
		return "", fmt.Errorf("fetch checksum %s: %w", shaURL, err)
	}
	wantSum = strings.TrimSpace(strings.Fields(wantSum)[0])

	body, err := fetchBytes(binURL)
	if err != nil {
		return "", fmt.Errorf("fetch sidecar %s: %w", binURL, err)
	}
	h := sha256.Sum256(body)
	gotSum := hex.EncodeToString(h[:])
	if !strings.EqualFold(gotSum, wantSum) {
		return "", fmt.Errorf("checksum mismatch for %s: got %s want %s", binURL, gotSum, wantSum)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	// Write atomically: temp file in the same dir, then rename.
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".sidecar-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return "", err
	}
	return dst, nil
}

func fetchBytes(url string) ([]byte, error) {
	resp, err := http.Get(url) //nolint:gosec // release URL, checksum-verified by caller
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func fetchString(url string) (string, error) {
	b, err := fetchBytes(url)
	return string(b), err
}
