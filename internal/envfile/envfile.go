// Package envfile loads KEY=VALUE pairs from a `.env`-style file into the
// process environment. Real env vars always win — Load never overwrites a
// variable that's already set. This is deliberately a tiny dependency-free
// implementation; fancier needs (interpolation, multi-line values) should
// switch to github.com/joho/godotenv.
package envfile

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"strings"
)

// Load reads path, parses KEY=VALUE lines, and os.Setenv each pair whose
// key is not already set in the environment. Missing files are not an
// error — Load returns nil so callers can always invoke it with a
// conventional path like ".env". Parse errors propagate.
func Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close() //nolint:errcheck // read-side close; no recovery possible

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		// Strip optional surrounding quotes.
		val = strings.Trim(val, `"'`)
		if key == "" {
			continue
		}
		if _, set := os.LookupEnv(key); set {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			return err
		}
	}
	return s.Err()
}
