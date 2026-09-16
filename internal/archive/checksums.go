package archive

import (
	"bufio"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Checksum is one CHECKSUMS line's data: a top-level artifact's name and
// the sha256 of its bytes. Bytes is carried for the manifest but is not
// part of the on-disk line, which keeps the file sha256sum-compatible.
type Checksum struct {
	Name   string
	SHA256 string
	Bytes  int64
}

// WriteChecksums writes sums to path atomically in sha256sum's text
// format — "<hex>  <name>\n" per line, two spaces — sorted by Name
// bytewise, so the file is byte-reproducible regardless of the order the
// artifacts were built in and verifiable with `sha256sum -c`. The caller's
// slice is left untouched.
func WriteChecksums(path string, sums []Checksum) error {
	sorted := slices.Clone(sums)
	slices.SortFunc(sorted, func(a, b Checksum) int { return strings.Compare(a.Name, b.Name) })
	if err := validateChecksums(sorted); err != nil {
		return fmt.Errorf("write checksums %s: %w", path, err)
	}
	_, err := WriteFileAtomic(path, func(w io.Writer) error {
		bw := bufio.NewWriter(w)
		for _, c := range sorted {
			if _, err := fmt.Fprintf(bw, "%s  %s\n", c.SHA256, c.Name); err != nil {
				return err
			}
		}
		return bw.Flush()
	})
	if err != nil {
		return fmt.Errorf("write checksums: %w", err)
	}
	return nil
}

// ParseChecksums reads a CHECKSUMS file in the form WriteChecksums emits:
// "<hex>  <name>" per line. Bytes is not carried by the format and is
// zero in the result. A malformed line or a duplicate name is an error —
// a partially trusted checksum list is worse than none.
func ParseChecksums(r io.Reader) ([]Checksum, error) {
	var sums []Checksum
	seen := map[string]struct{}{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		hexsum, name, ok := strings.Cut(line, "  ")
		if !ok || !isLowerHex64(hexsum) || name == "" {
			return nil, fmt.Errorf("parse checksums: line %d: malformed: %q", n, line)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("parse checksums: line %d: duplicate name %q", n, name)
		}
		seen[name] = struct{}{}
		sums = append(sums, Checksum{Name: name, SHA256: hexsum})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("parse checksums: %w", err)
	}
	return sums, nil
}

// validateChecksums guards the line format: a name that is empty or
// carries a line break cannot be written as one line, a digest that is
// not 64 lowercase hex digits is not a sha256 as this package emits it
// (case is pinned so the file's bytes are), and a duplicate name would
// make the list contradict itself.
func validateChecksums(sums []Checksum) error {
	seen := make(map[string]struct{}, len(sums))
	for _, c := range sums {
		switch {
		case c.Name == "":
			return fmt.Errorf("checksum %q: empty name", c.SHA256)
		case strings.ContainsAny(c.Name, "\r\n"):
			return fmt.Errorf("checksum %q: name contains a line break", c.Name)
		case !isLowerHex64(c.SHA256):
			return fmt.Errorf("checksum %q: sha256 %q is not 64 lowercase hex digits", c.Name, c.SHA256)
		}
		if _, dup := seen[c.Name]; dup {
			return fmt.Errorf("checksum %q: duplicate name", c.Name)
		}
		seen[c.Name] = struct{}{}
	}
	return nil
}

func isLowerHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
