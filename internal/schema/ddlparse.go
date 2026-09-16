package schema

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Table is one CREATE TABLE of the embedded schema.sql as the
// data-dictionary generator reads it: the table name and the columns in
// declaration order.
type Table struct {
	Name    string
	Columns []Column
}

// Column is one column declaration of a Table: the name, the declared
// type token as written (INTEGER, TEXT, REAL), whether the definition
// carries NOT NULL, the 1-based position in the primary key (0 when not
// part of it — pragma_table_info's convention), and the trailing
// '-- comment' on the declaration line. The comment blocks above a
// column or a table are the DDL's own maintenance notes and are not
// read: the dictionary describes a column from its trailing comment or
// from its own text, never from a note written for the schema's
// maintainers.
type Column struct {
	Name    string
	Type    string
	NotNull bool
	PK      int
	Comment string
}

// tables is the embedded DDL parsed once. The DDL is an embedded asset
// whose shape the parity gates and the export specs already hold, so a
// text the parser cannot read is an impossible state guarded at init,
// like the version header.
var tables = mustParseTables(ddl)

func mustParseTables(s string) []Table {
	t, err := parseTables(s)
	if err != nil {
		panic("schema.sql: " + err.Error())
	}
	return t
}

// Tables returns every table of the embedded schema.sql in DDL order,
// each with its columns' trailing comments — the source the data
// dictionary is generated from, paired with Precision for the decimal
// counts. The result is a
// copy, so a caller cannot reorder or edit the parse every later caller
// reads.
func Tables() []Table {
	out := make([]Table, len(tables))
	for i, t := range tables {
		out[i] = t
		out[i].Columns = slices.Clone(t.Columns)
	}
	return out
}

// createTablePrefix is the one CREATE TABLE form schema.sql uses; the
// opening parenthesis closes the line, and ");" alone closes the body.
const createTablePrefix = "CREATE TABLE IF NOT EXISTS "

// columnTypes are the declared types a column line may carry — SQLite's
// storage classes. A body line whose first token is an identifier and
// whose second is one of these declares a column; every other body line
// is a table constraint or a continuation of the column above it.
var columnTypes = map[string]bool{"INTEGER": true, "TEXT": true, "REAL": true, "BLOB": true, "NUMERIC": true}

// ddlParser reads the DDL line by line: outside a body it watches for
// CREATE TABLE; inside a body it collects columns, their trailing
// comments, continuation lines, and the table-level PRIMARY KEY. Blank
// lines and comment-only lines are skipped wherever they sit.
type ddlParser struct {
	tables []Table
	table  *Table   // the body being read; nil between statements
	pk     []string // the table-level PRIMARY KEY's column names, if any
}

// parseTables parses a DDL text in schema.sql's form.
func parseTables(s string) ([]Table, error) {
	p := &ddlParser{}
	for n, raw := range strings.Split(s, "\n") {
		if err := p.line(strings.TrimSpace(raw)); err != nil {
			return nil, fmt.Errorf("line %d: %w", n+1, err)
		}
	}
	if p.table != nil {
		return nil, fmt.Errorf("table %s is not closed", p.table.Name)
	}
	if len(p.tables) == 0 {
		return nil, errors.New("no CREATE TABLE statement")
	}
	return p.tables, nil
}

// line consumes one trimmed line.
func (p *ddlParser) line(line string) error {
	switch {
	case line == "" || strings.HasPrefix(line, "--"):
		return nil
	case p.table == nil:
		return p.statement(line)
	default:
		return p.body(line)
	}
}

// statement handles a code line between bodies: a CREATE TABLE opens a
// body; any other statement (CREATE INDEX) is skipped.
func (p *ddlParser) statement(line string) error {
	if !strings.HasPrefix(line, createTablePrefix) {
		return nil
	}
	rest := strings.Fields(strings.TrimPrefix(line, createTablePrefix))
	if len(rest) != 2 || rest[1] != "(" || !isIdentifier(rest[0]) {
		return fmt.Errorf("unsupported CREATE TABLE form: %q", line)
	}
	if slices.ContainsFunc(p.tables, func(t Table) bool { return t.Name == rest[0] }) {
		return fmt.Errorf("table %s declared twice", rest[0])
	}
	p.table = &Table{Name: rest[0]}
	p.pk = nil
	return nil
}

// body handles a code line inside a CREATE TABLE body.
func (p *ddlParser) body(line string) error {
	if line == ");" {
		return p.close()
	}
	code, comment := splitComment(line)
	code = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(code), ","))
	fields := strings.Fields(code)
	switch {
	case len(fields) == 0:
		return nil
	case len(fields) >= 2 && strings.EqualFold(fields[0], "PRIMARY") && strings.EqualFold(fields[1], "KEY"):
		names, err := parenthesizedNames(code)
		if err != nil {
			return err
		}
		p.pk = names
	case len(fields) >= 2 && isIdentifier(fields[0]) && columnTypes[strings.ToUpper(fields[1])]:
		if slices.ContainsFunc(p.table.Columns, func(c Column) bool { return c.Name == fields[0] }) {
			return fmt.Errorf("table %s: column %s declared twice", p.table.Name, fields[0])
		}
		p.table.Columns = append(p.table.Columns, Column{
			Name:    fields[0],
			Type:    strings.ToUpper(fields[1]),
			Comment: comment,
		})
		p.constraints(code)
	case len(p.table.Columns) > 0:
		// A continuation of the column above (a CHECK, a REFERENCES) or a
		// table constraint (UNIQUE); the flags it may carry belong to the
		// column above, and a table constraint carries none.
		p.constraints(code)
	}
	return nil
}

// constraints folds the NOT NULL and inline PRIMARY KEY flags of a
// definition line into the last column. A table-level PRIMARY KEY never
// reaches here (body handles it), so "PRIMARY KEY" text is the column's
// own.
func (p *ddlParser) constraints(code string) {
	c := &p.table.Columns[len(p.table.Columns)-1]
	upper := strings.ToUpper(code)
	if strings.Contains(upper, "NOT NULL") {
		c.NotNull = true
	}
	if strings.Contains(upper, "PRIMARY KEY") {
		c.PK = 1
	}
}

// close ends the body: the table-level PRIMARY KEY, if any, numbers its
// columns in key order.
func (p *ddlParser) close() error {
	t := p.table
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %s declares no column", t.Name)
	}
	for i, name := range p.pk {
		j := slices.IndexFunc(t.Columns, func(c Column) bool { return c.Name == name })
		if j < 0 {
			return fmt.Errorf("table %s: PRIMARY KEY names unknown column %s", t.Name, name)
		}
		t.Columns[j].PK = i + 1
	}
	p.tables = append(p.tables, *t)
	p.table = nil
	p.pk = nil
	return nil
}

// splitComment separates a line's code from its trailing '-- comment',
// honouring single-quoted literals so a "--" inside one is code.
func splitComment(line string) (code, comment string) {
	inQuote := false
	for i := 0; i+1 < len(line); i++ {
		switch {
		case line[i] == '\'':
			inQuote = !inQuote
		case !inQuote && line[i] == '-' && line[i+1] == '-':
			return line[:i], commentText(line[i:])
		}
	}
	return line, ""
}

// commentText strips the '--' marker and the one space that conventionally
// follows it, keeping any further indentation the author laid out.
func commentText(line string) string {
	return strings.TrimPrefix(strings.TrimPrefix(line, "--"), " ")
}

// parenthesizedNames returns the comma-separated names between the first
// '(' and the last ')' of a constraint.
func parenthesizedNames(code string) ([]string, error) {
	open, closing := strings.IndexByte(code, '('), strings.LastIndexByte(code, ')')
	if open < 0 || closing < open {
		return nil, fmt.Errorf("unparenthesized column list: %q", code)
	}
	var names []string
	for _, n := range strings.Split(code[open+1:closing], ",") {
		n = strings.TrimSpace(n)
		if !isIdentifier(n) {
			return nil, fmt.Errorf("not a column name: %q", n)
		}
		names = append(names, n)
	}
	return names, nil
}

// isIdentifier reports whether s is a bare SQL identifier as schema.sql
// writes them: a letter or underscore, then letters, digits, underscores.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		alpha := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		if !alpha && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}
	return true
}
