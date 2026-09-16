package publish

// In-package lockstep for the state database plan: the plan names
// exactly the DDL's tables, once each, and every scoping subquery reads
// a table the plan copied earlier — so the row set is defined
// parent-first by construction, and a DDL table cannot be added without
// a row-set rule.

import (
	"context"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/schema"
)

var _ = ginkgo.Describe("stateDBPlan", func() {
	ddlTables := func() []string {
		ginkgo.GinkgoHelper()
		db, err := schema.Open(filepath.Join(ginkgo.GinkgoT().TempDir(), "plan.db"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = db.Close() }()
		conn, err := db.Conn(context.Background())
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = conn.Close() }()
		tables, err := schemaTables(context.Background(), conn, "main")
		Expect(err).NotTo(HaveOccurred())
		return tables
	}

	ginkgo.It("names every DDL table exactly once", func() {
		var planned []string
		for _, s := range stateDBPlan {
			planned = append(planned, s.table)
		}
		Expect(planned).To(ConsistOf(ddlTables()))
		Expect(checkPlan(ddlTables())).To(Succeed())
	})

	ginkgo.It("scopes each table only by tables copied before it", func() {
		ref := regexp.MustCompile(`main\.(\w+)`)
		var seen []string
		for _, s := range stateDBPlan {
			for _, m := range ref.FindAllStringSubmatch(s.where, -1) {
				Expect(seen).To(ContainElement(m[1]), "%s is scoped by %s, which the plan has not copied yet", s.table, m[1])
			}
			seen = append(seen, s.table)
		}
	})

	ginkgo.It("binds (state, source) on the stations rule alone and copies the runs whole", func() {
		for _, s := range stateDBPlan {
			switch s.table {
			case "stations":
				Expect(s.byState).To(BeTrue())
				Expect(s.where).To(Equal("WHERE state = ? AND source = ?"))
			case "ingest_runs", "power_runs":
				Expect(s.where).To(BeEmpty(), s.table)
				Expect(s.byState).To(BeFalse(), s.table)
			default:
				Expect(s.byState).To(BeFalse(), s.table)
				Expect(s.where).NotTo(BeEmpty(), s.table)
			}
		}
	})

	ginkgo.It("refuses a DDL table the plan lacks and a plan table the DDL lacks", func() {
		Expect(checkPlan(append(ddlTables(), "future_table"))).To(MatchError(
			"schema table future_table has no row-set rule in the state database plan"))
		without := slices.DeleteFunc(ddlTables(), func(t string) bool { return t == "parsing_warnings" })
		Expect(checkPlan(without)).To(MatchError(
			"state database plan names parsing_warnings, which the schema has no table for"))
	})

	ginkgo.It("attaches the source through a URI SQLite refuses to write, and can read it", func() {
		// The build's read-only guarantee is the ATTACH DSN: the same
		// spelling buildStateDB uses, on a writer connection with no
		// query_only, must let a read through and refuse a write — so a
		// driver taking the URI as a literal filename, or dropping
		// mode=ro, fails here rather than passing a suite that never
		// tries to write.
		ctx := context.Background()
		dir := ginkgo.GinkgoT().TempDir()
		src, err := schema.Open(filepath.Join(dir, "source.db"))
		Expect(err).NotTo(HaveOccurred())
		_, err = src.ExecContext(ctx, `INSERT INTO stations (source, external_id, name) VALUES ('conagua_conventional', '1', 'One')`)
		Expect(err).NotTo(HaveOccurred())
		Expect(src.Close()).To(Succeed())

		build, err := schema.Open(filepath.Join(dir, "build.db"))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = build.Close() }()
		conn, err := build.Conn(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = conn.Close() }()
		_, err = conn.ExecContext(ctx, "ATTACH DATABASE ? AS src", schema.ReadOnlyDSN(filepath.Join(dir, "source.db")))
		Expect(err).NotTo(HaveOccurred())

		var n int
		Expect(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM src.stations").Scan(&n)).To(Succeed())
		Expect(n).To(Equal(1))
		_, err = conn.ExecContext(ctx,
			`INSERT INTO src.stations (source, external_id, name) VALUES ('conagua_conventional', 'x', 'X')`)
		Expect(err).To(MatchError(ContainSubstring("readonly database")))
		Expect(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM src.stations").Scan(&n)).To(Succeed())
		Expect(n).To(Equal(1))
	})
})
