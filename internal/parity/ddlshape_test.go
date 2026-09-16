package parity_test

// The schema shape-equivalence gate: our DDL must reproduce the
// reference DDL's structure exactly — table sets, per-table column
// shapes, and indexes — while comments may differ. Env-guarded because
// it reads the reference schema.sql from outside the repo; our own DDL is read from disk too, so the
// spec pins the committed file, not a possibly stale embedded copy.

import (
	"database/sql"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	// Registers the "sqlite" driver for the raw sql.Open below.
	_ "modernc.org/sqlite"
)

// columnShape is one `pragma table_info` row, minus the positional cid
// (order is carried by the slice).
type columnShape struct {
	Name    string
	Type    string
	NotNull int
	Default sql.NullString
	PKSlot  int
}

// indexShape is one `pragma index_list` row, minus the positional seq
// (index creation order is not shape; the lists compare sorted by name).
type indexShape struct {
	Name    string
	Unique  int
	Origin  string
	Partial int
}

// indexColumn is one `pragma index_info` row.
type indexColumn struct {
	SeqNo int
	CID   int
	Name  string
}

var _ = Describe("Schema shape equivalence", func() {
	It("reproduces the reference DDL's tables, columns, and indexes", func() {
		referencePath := os.Getenv("SCHEMA_PARITY_REFERENCE_DDL")
		if referencePath == "" {
			Skip("SCHEMA_PARITY_REFERENCE_DDL not set — schema shape spec skipped")
		}

		reference := applyDDL("reference", mustReadFile(referencePath))
		ours := applyDDL("ours", mustReadFile(filepath.Join("..", "schema", "schema.sql")))

		referenceTables := tableNames(reference)
		Expect(tableNames(ours)).To(Equal(referenceTables), "table sets diverge")

		for _, table := range referenceTables {
			Expect(tableShape(ours, table)).To(Equal(tableShape(reference, table)),
				"pragma table_info(%s) diverges", table)

			referenceIndexes := indexList(reference, table)
			Expect(indexList(ours, table)).To(Equal(referenceIndexes),
				"pragma index_list(%s) diverges", table)
			for _, idx := range referenceIndexes {
				Expect(indexColumns(ours, idx.Name)).To(Equal(indexColumns(reference, idx.Name)),
					"pragma index_info(%s) diverges", idx.Name)
			}
		}
	})
})

func mustReadFile(path string) string {
	GinkgoHelper()
	body, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	return string(body)
}

// applyDDL executes one DDL script against a fresh temp database and
// returns the handle for pragma inspection.
func applyDDL(label, ddl string) *sql.DB {
	GinkgoHelper()
	db, err := sql.Open("sqlite", filepath.Join(GinkgoT().TempDir(), label+".db"))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { Expect(db.Close()).To(Succeed()) })
	_, err = db.Exec(ddl)
	Expect(err).NotTo(HaveOccurred(), "apply %s DDL", label)
	return db
}

// tableNames lists the schema's tables sorted by name, excluding
// SQLite internals (sqlite_sequence from AUTOINCREMENT).
func tableNames(db *sql.DB) []string {
	GinkgoHelper()
	rows, err := db.Query(
		`SELECT name FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var n string
		Expect(rows.Scan(&n)).To(Succeed())
		names = append(names, n)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return names
}

// tableShape returns the table's columns in declaration order.
func tableShape(db *sql.DB, table string) []columnShape {
	GinkgoHelper()
	rows, err := db.Query(`SELECT name, type, "notnull", dflt_value, pk
		FROM pragma_table_info(?) ORDER BY cid`, table)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()

	var cols []columnShape
	for rows.Next() {
		var c columnShape
		Expect(rows.Scan(&c.Name, &c.Type, &c.NotNull, &c.Default, &c.PKSlot)).To(Succeed())
		cols = append(cols, c)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return cols
}

// indexList returns the table's indexes sorted by name, including the
// UNIQUE-constraint autoindexes.
func indexList(db *sql.DB, table string) []indexShape {
	GinkgoHelper()
	rows, err := db.Query(`SELECT name, "unique", origin, partial
		FROM pragma_index_list(?) ORDER BY name`, table)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()

	var indexes []indexShape
	for rows.Next() {
		var i indexShape
		Expect(rows.Scan(&i.Name, &i.Unique, &i.Origin, &i.Partial)).To(Succeed())
		indexes = append(indexes, i)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return indexes
}

// indexColumns returns one index's columns in key order.
func indexColumns(db *sql.DB, index string) []indexColumn {
	GinkgoHelper()
	rows, err := db.Query(`SELECT seqno, cid, name
		FROM pragma_index_info(?) ORDER BY seqno`, index)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = rows.Close() }()

	var cols []indexColumn
	for rows.Next() {
		var c indexColumn
		Expect(rows.Scan(&c.SeqNo, &c.CID, &c.Name)).To(Succeed())
		cols = append(cols, c)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return cols
}
