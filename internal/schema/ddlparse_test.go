package schema

import (
	"database/sql"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// pragmaColumn is one pragma_table_info row with the fields Column
// mirrors, so the parse is held to the DDL SQLite actually applies.
type pragmaColumn struct {
	Name    string
	Type    string
	NotNull bool
	PK      int
}

// pragmaTables applies the embedded DDL to a fresh database and returns
// every table's columns in declaration order, tables in creation order
// (sqlite_master's rowid order is the DDL's statement order).
func pragmaTables() (names []string, columns map[string][]pragmaColumn) {
	GinkgoHelper()
	db := mustOpen(tempDBPath())
	defer mustClose(db)

	rows, err := db.Query(
		`SELECT name FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY rowid`)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var n string
		Expect(rows.Scan(&n)).To(Succeed())
		names = append(names, n)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())

	columns = make(map[string][]pragmaColumn, len(names))
	for _, table := range names {
		columns[table] = pragmaColumns(db, table)
	}
	return names, columns
}

func pragmaColumns(db *sql.DB, table string) []pragmaColumn {
	GinkgoHelper()
	rows, err := db.Query(`SELECT name, type, "notnull", pk FROM pragma_table_info(?) ORDER BY cid`, table)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()
	var cols []pragmaColumn
	for rows.Next() {
		var c pragmaColumn
		Expect(rows.Scan(&c.Name, &c.Type, &c.NotNull, &c.PK)).To(Succeed())
		cols = append(cols, c)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return cols
}

// columnOf returns table.column from a parsed table list, failing the
// spec when either is missing.
func columnOf(tables []Table, table, column string) Column {
	GinkgoHelper()
	for _, t := range tables {
		if t.Name != table {
			continue
		}
		for _, c := range t.Columns {
			if c.Name == column {
				return c
			}
		}
		Fail("table " + table + " has no column " + column)
	}
	Fail("no table " + table)
	return Column{}
}

var _ = Describe("Tables — the embedded DDL parsed for the data dictionary", func() {
	It("parses every table in DDL order with the columns SQLite reports, name, type, NOT NULL, and PK position alike", func() {
		names, want := pragmaTables()
		got := Tables()
		Expect(got).To(HaveLen(len(names)))
		for i, t := range got {
			Expect(t.Name).To(Equal(names[i]), "table %d", i)
			Expect(t.Columns).To(HaveLen(len(want[t.Name])), t.Name)
			for j, c := range t.Columns {
				w := want[t.Name][j]
				Expect(c.Name).To(Equal(w.Name), "%s column %d", t.Name, j)
				Expect(c.Type).To(Equal(w.Type), "%s.%s", t.Name, c.Name)
				Expect(c.NotNull).To(Equal(w.NotNull), "%s.%s NOT NULL", t.Name, c.Name)
				Expect(c.PK).To(Equal(w.PK), "%s.%s PK position", t.Name, c.Name)
			}
		}
	})

	It("pins the eleven tables and the known column counts", func() {
		got := Tables()
		names := make([]string, len(got))
		counts := map[string]int{}
		for i, t := range got {
			names[i] = t.Name
			counts[t.Name] = len(t.Columns)
		}
		Expect(names).To(Equal([]string{
			"stations", "monthly_normals", "monthly_normals_extras", "daily_observations", "parsing_warnings",
			"ingest_runs", "power_runs", "nasa_power_grid_cells", "station_power_cell",
			"monthly_supplement", "daily_supplement",
		}))
		Expect(counts).To(Equal(map[string]int{
			"stations":               20,
			"monthly_normals":        8,
			"monthly_normals_extras": 22,
			"daily_observations":     6,
			"parsing_warnings":       6,
			"ingest_runs":            14,
			"power_runs":             20,
			"nasa_power_grid_cells":  4,
			"station_power_cell":     3,
			"monthly_supplement":     35,
			"daily_supplement":       34,
		}))
	})

	It("reads the trailing column comments verbatim", func() {
		tables := Tables()
		Expect(columnOf(tables, "monthly_supplement", "t2m_c").Comment).To(Equal("mean air temperature at 2 m"))
		Expect(columnOf(tables, "monthly_supplement", "solar_ghi_wm2").Comment).To(Equal("ALLSKY_SFC_SW_DWN  — global horizontal"))
		Expect(columnOf(tables, "monthly_supplement", "gwet_prof").Comment).To(Equal("profile-integrated soil moisture"))
		Expect(columnOf(tables, "daily_supplement", "date").Comment).To(Equal("ISO 'YYYY-MM-DD'"))
		Expect(columnOf(tables, "monthly_normals_extras", "tmax_daily_extreme_date").Comment).To(Equal("ISO 'YYYY-MM-DD'"))
		Expect(columnOf(tables, "daily_supplement", "t2m_c").Comment).To(BeEmpty(), "daily_supplement carries no per-column comments")
		Expect(columnOf(tables, "stations", "name").Comment).To(BeEmpty())
	})

	It("does not read the comment blocks above a column or a table: a column carries its trailing comment alone", func() {
		tables := Tables()
		// The WMO block above the first completeness column and the group
		// headings of the extras are maintenance notes, not descriptions.
		Expect(columnOf(tables, "stations", "wmo_completeness_bin_1961_1990").Comment).To(BeEmpty())
		Expect(columnOf(tables, "monthly_normals_extras", "tmax_monthly_extreme").Comment).To(BeEmpty())
		Expect(columnOf(tables, "monthly_supplement", "power_run_id").Comment).To(BeEmpty())
		Expect(columnOf(tables, "power_runs", "unit_conversions").Comment).To(BeEmpty())
	})

	It("resolves every Precision entry to a parsed REAL or INTEGER column", func() {
		tables := Tables()
		for table, cols := range Precision {
			for column := range cols {
				c := columnOf(tables, table, column)
				Expect(c.Type).To(BeElementOf("REAL", "INTEGER"), "%s.%s", table, column)
			}
		}
	})

	It("returns a copy each call", func() {
		first := Tables()
		first[0].Name = "edited"
		first[0].Columns[0].Comment = "edited"
		first[0].Columns = first[0].Columns[:1]
		second := Tables()
		Expect(second[0].Name).To(Equal("stations"))
		Expect(second[0].Columns[0].Comment).To(BeEmpty())
		Expect(second[0].Columns).To(HaveLen(20))
	})
})

var _ = Describe("parseTables", func() {
	It("reads a table-level PRIMARY KEY into key positions and an inline one as position 1", func() {
		got, err := parseTables(strings.Join([]string{
			"CREATE TABLE IF NOT EXISTS a (",
			"    id INTEGER PRIMARY KEY AUTOINCREMENT,",
			"    k TEXT NOT NULL",
			");",
			"CREATE TABLE IF NOT EXISTS b (",
			"    x TEXT NOT NULL,",
			"    y INTEGER NOT NULL CHECK (y BETWEEN 1 AND 12),",
			"    z REAL,",
			"    PRIMARY KEY (y, x)",
			");",
		}, "\n"))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]Table{
			{Name: "a", Columns: []Column{
				{Name: "id", Type: "INTEGER", PK: 1},
				{Name: "k", Type: "TEXT", NotNull: true},
			}},
			{Name: "b", Columns: []Column{
				{Name: "x", Type: "TEXT", NotNull: true, PK: 2},
				{Name: "y", Type: "INTEGER", NotNull: true, PK: 1},
				{Name: "z", Type: "REAL"},
			}},
		}))
	})

	It("keeps a '--' inside a quoted literal as code, folds a NOT NULL on a continuation line, and skips the comment block above the table", func() {
		got, err := parseTables(strings.Join([]string{
			"-- doc line one",
			"--",
			"-- doc line three",
			"CREATE TABLE IF NOT EXISTS t (",
			"    a TEXT DEFAULT '--' -- the dash pair is data",
			"      NOT NULL,",
			"    b TEXT",
			");",
		}, "\n"))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]Table{{Name: "t", Columns: []Column{
			{Name: "a", Type: "TEXT", NotNull: true, Comment: "the dash pair is data"},
			{Name: "b", Type: "TEXT"},
		}}}))
	})

	It("skips statements that are not CREATE TABLE and the comment lines around them", func() {
		got, err := parseTables(strings.Join([]string{
			"-- an index",
			"CREATE INDEX IF NOT EXISTS i ON t(a);",
			"-- the table",
			"CREATE TABLE IF NOT EXISTS t (",
			"    a TEXT",
			");",
		}, "\n"))
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]Table{{Name: "t", Columns: []Column{{Name: "a", Type: "TEXT"}}}}))
	})

	DescribeTable("refuses a DDL it cannot read faithfully",
		func(ddl, want string) {
			_, err := parseTables(ddl)
			Expect(err).To(MatchError(ContainSubstring(want)))
		},
		Entry("no table", "CREATE INDEX IF NOT EXISTS i ON t(a);", "no CREATE TABLE statement"),
		Entry("an unclosed body", "CREATE TABLE IF NOT EXISTS t (\n a TEXT\n", "table t is not closed"),
		Entry("an empty body", "CREATE TABLE IF NOT EXISTS t (\n);", "table t declares no column"),
		Entry("a PRIMARY KEY naming an unknown column",
			"CREATE TABLE IF NOT EXISTS t (\n a TEXT,\n PRIMARY KEY (b)\n);", "PRIMARY KEY names unknown column b"),
		Entry("a column declared twice",
			"CREATE TABLE IF NOT EXISTS t (\n a TEXT,\n a REAL\n);", "column a declared twice"),
		Entry("a table declared twice",
			"CREATE TABLE IF NOT EXISTS t (\n a TEXT\n);\nCREATE TABLE IF NOT EXISTS t (\n a TEXT\n);", "table t declared twice"),
		Entry("an unsupported CREATE TABLE form",
			"CREATE TABLE IF NOT EXISTS t (a TEXT);", "unsupported CREATE TABLE form"),
	)
})
