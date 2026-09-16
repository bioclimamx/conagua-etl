package publish

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
)

// appendReal appends v rendered with exactly decimals fraction digits —
// '.' separator, no exponent, no thousands separator — and returns the
// extended buffer. It is the one fixed-decimal implementation, so every
// format renders a value to the same bytes: formatReal is this over a fresh buffer, and the streaming
// daily.json writer calls it directly so a row appends bytes and never
// allocates a string per value. A result whose digits are all zero is
// written as positive zero: "-0.0" would leak the sign of a
// sub-precision negative (-0.04 at one decimal) into a file that claims
// one decimal of precision, and every reader parses "-0.0" and "0.0" to
// the same number — a value that rounds to zero is zero.
func appendReal(dst []byte, v float64, decimals int) []byte {
	start := len(dst)
	dst = strconv.AppendFloat(dst, v, 'f', decimals, 64)
	if dst[start] == '-' && allZeroDigits(dst[start+1:]) {
		dst = append(dst[:start], dst[start+1:]...)
	}
	return dst
}

// allZeroDigits reports whether b is a decimal literal with no non-zero
// digit — only '0' and '.'.
func allZeroDigits(b []byte) bool {
	for _, c := range b {
		if c != '0' && c != '.' {
			return false
		}
	}
	return true
}

// formatReal renders v with exactly decimals fraction digits, through
// appendReal.
func formatReal(v float64, decimals int) string {
	return string(appendReal(nil, v, decimals))
}

// formatInt renders an INTEGER column value (the 0-decimal class).
func formatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// checkFinite refuses a non-finite REAL by the name of the column that
// holds it. SQLite cannot hold NaN, so ±Inf in an exported value is a
// corruption signal, and the strict-integrity line says abort rather
// than write "+Inf" — the CSV cell, the daily.json value, and the
// profile's fixed-shape fields all refuse through this one check, so an
// error names the column whichever path found it.
func checkFinite(name string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("column %s: non-finite value %v", name, v)
	}
	return nil
}

// scanTarget returns the destination a row scanner passes for c —
// nullable on purpose, so an honest NULL reaches the file as an empty
// field and never as a zero.
func scanTarget(c Column) any {
	switch c.Kind {
	case KindInt:
		return new(sql.NullInt64)
	case KindReal:
		return new(sql.NullFloat64)
	default:
		return new(sql.NullString)
	}
}

// formatCell renders one scanned value as its column's CSV text. NULL is
// the empty string; a non-finite REAL is refused (checkFinite).
func formatCell(c Column, target any) (string, error) {
	switch t := target.(type) {
	case *sql.NullString:
		if !t.Valid {
			return "", nil
		}
		return t.String, nil
	case *sql.NullInt64:
		if !t.Valid {
			return "", nil
		}
		return formatInt(t.Int64), nil
	case *sql.NullFloat64:
		if !t.Valid {
			return "", nil
		}
		if err := checkFinite(c.Name, t.Float64); err != nil {
			return "", err
		}
		return formatReal(t.Float64, c.Decimals), nil
	default:
		return "", fmt.Errorf("column %s: unsupported scan target %T", c.Name, target)
	}
}
