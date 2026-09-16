package publish

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
)

// The JSON profile's nullable scalars. Each marshals to the same literal
// the CSV writer emits for the same value — a REAL through formatReal at
// its column's fixed decimals, an INTEGER as digits, TEXT verbatim — or
// to an explicit null, never an omitted key. A REAL is never rendered
// through a float's shortest representation: 31.0 is "31.0",
// 228.472222222222 at two decimals is "228.47", and a value that rounds
// to zero is "0.0" — the fixed export precision that makes JSON
// byte-reproducible. The three types are value receivers on purpose, so they marshal
// the same whether held directly, behind a pointer, or in an interface
// slot of a month series.

// nullLiteral is JSON's null, the rendering of every absent value.
var nullLiteral = []byte("null")

// jsonReal is a nullable REAL rendered at a fixed decimal count. A
// non-finite value is refused at marshal time as the backstop of
// checkFinite: the loaders name the column when they refuse one, and
// this refusal catches any value that reached a marshal unchecked.
type jsonReal struct {
	v        sql.NullFloat64
	decimals int
}

// realOf wraps a scanned REAL.
func realOf(v sql.NullFloat64, decimals int) jsonReal {
	return jsonReal{v: v, decimals: decimals}
}

// realPtr wraps a computed value, nil meaning null.
func realPtr(v *float64, decimals int) jsonReal {
	if v == nil {
		return jsonReal{decimals: decimals}
	}
	return jsonReal{v: sql.NullFloat64{Float64: *v, Valid: true}, decimals: decimals}
}

// MarshalJSON renders the fixed-decimal literal or null.
func (r jsonReal) MarshalJSON() ([]byte, error) {
	if !r.v.Valid {
		return nullLiteral, nil
	}
	if math.IsNaN(r.v.Float64) || math.IsInf(r.v.Float64, 0) {
		return nil, fmt.Errorf("non-finite value %v", r.v.Float64)
	}
	return appendReal(nil, r.v.Float64, r.decimals), nil
}

// jsonInt is a nullable INTEGER rendered as digits.
type jsonInt struct {
	v sql.NullInt64
}

// MarshalJSON renders the digits or null.
func (i jsonInt) MarshalJSON() ([]byte, error) {
	if !i.v.Valid {
		return nullLiteral, nil
	}
	return []byte(formatInt(i.v.Int64)), nil
}

// jsonText is a nullable TEXT rendered as a JSON string, verbatim as
// stored — dates, periods, names, and status alike.
type jsonText struct {
	v sql.NullString
}

// MarshalJSON renders the string or null, with HTML escaping off so a
// name holding '<', '>', or '&' reads as written.
func (t jsonText) MarshalJSON() ([]byte, error) {
	if !t.v.Valid {
		return nullLiteral, nil
	}
	return marshalNoEscape(t.v.String)
}

// cellOf converts one scanned column value — a scanTarget of c — into
// the scalar that renders it. The target is copied, so a scanner may
// reuse its targets across rows.
func cellOf(c Column, target any) (json.Marshaler, error) {
	switch t := target.(type) {
	case *sql.NullString:
		return jsonText{v: *t}, nil
	case *sql.NullInt64:
		return jsonInt{v: *t}, nil
	case *sql.NullFloat64:
		return realOf(*t, c.Decimals), nil
	default:
		return nil, fmt.Errorf("column %s: unsupported scan target %T", c.Name, target)
	}
}

// nullCell is the explicit null of c's kind — what a month slot holds
// until a row fills it, so an empty slot is a rendered null and never a
// nil interface that happens to print the same.
func nullCell(c Column) json.Marshaler {
	switch c.Kind {
	case KindInt:
		return jsonInt{}
	case KindReal:
		return jsonReal{decimals: c.Decimals}
	default:
		return jsonText{}
	}
}

// marshalNoEscape renders v as compact JSON with HTML escaping off —
// the setting the profile writer uses — so a Marshaler nested inside
// the profile cannot reintroduce <-style escapes the outer encoder
// was told not to write.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
