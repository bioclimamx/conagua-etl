package publish_test

// Specs at the Run → archive seam for the tabular group, held to
// TabularEntries itself: each state's archive lists exactly the paths
// TabularEntries returns — the CSV folders, the ten Parquet twins, the
// state database — in that order, with every Parquet entry's bytes as
// the entry renders them and the database's content as the entry
// builds it; the unit events one per unit TabularEntries counts; and,
// across two runs into fresh directories, every Parquet twin
// byte-identical and every state database content-identical, the
// database's byte identity reported rather than claimed.

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/archive"
	"github.com/bioclimamx/conagua-etl/internal/publish"
)

// tabularZip reads the state's tabular archive out of dir.
func tabularZip(dir string, st publish.State) (names []string, contents map[string][]byte) {
	GinkgoHelper()
	return zipContents(filepath.Join(dir, st.Slug+"-tabular.zip"))
}

// dbRows reads every DDL table of the database in data, keyed by table.
func dbRows(data []byte, name string) map[string][][]any {
	GinkgoHelper()
	got := openRO(writeDB(data, name))
	out := map[string][][]any{}
	for _, t := range ddlTableNames {
		out[t] = selectAll(got, t, "")
	}
	return out
}

var _ = Describe("Run's tabular archives against TabularEntries", func() {
	var (
		ctx    context.Context
		db     *sql.DB
		out    string
		rec    *recorder
		states []publish.State
		runs   publish.Runs
	)

	BeforeEach(func() {
		ctx = context.Background()
		db = openTempDB()
		ids := seedTwoStates(db)
		seedPower(db, ids)
		seedRuns(db)
		var err error
		states, err = publish.LoadStates(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		Expect(codesOf(states)).To(Equal([]string{"AGS", "YUC", "ZAC"}))
		runs, err = publish.LoadRuns(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		out = filepath.Join(GinkgoT().TempDir(), "publish")
		rec = &recorder{}
	})

	tabularOnly := func(dir string) publish.Options {
		return publish.Options{OutDir: dir, Now: fixedClock, Only: []string{"tabular"}}
	}

	It("lists, per state, exactly TabularEntries' paths in its order — CSV folders, the ten Parquet twins, the state database — each Parquet twin the entry's bytes, the database the entry's content, one unit event per unit", func() {
		report, err := publish.Run(ctx, db, tabularOnly(out), rec.record)
		Expect(err).NotTo(HaveOccurred())
		Expect(report.Failed).To(BeZero())
		Expect(report.Artifacts).To(HaveLen(len(states)))

		for i, st := range states {
			entries, units, err := publish.TabularEntries(ctx, db, st, runs, GinkgoT().TempDir(), nil)
			Expect(err).NotTo(HaveOccurred())
			names, contents := tabularZip(out, st)
			Expect(names).To(Equal(entryPaths(entries)), st.Code)
			Expect(report.Artifacts[i].Name).To(Equal(st.Slug + "-tabular.zip"))
			Expect(report.Artifacts[i].Entries).To(Equal(len(entries)), st.Code)

			for _, p := range parquetPaths() {
				Expect(names).To(ContainElement(p), st.Code)
			}
			Expect(names).To(ContainElement(st.Slug+".db"), st.Code)
			Expect(names).To(ContainElement("provenance/ingest_runs.csv"), st.Code)
			Expect(names).NotTo(ContainElement(And(HavePrefix("provenance/"), HaveSuffix(".parquet"))), st.Code)

			var dbEntry archive.Entry
			for _, e := range entries {
				switch {
				case strings.HasSuffix(e.Path, ".parquet"):
					Expect(contents[e.Path]).To(Equal(render(e)), "%s: %s", st.Code, e.Path)
				case e.Path == st.Slug+".db":
					dbEntry = e
				}
			}
			Expect(dbEntry.Path).To(Equal(st.Slug + ".db"))
			Expect(dbRows(contents[dbEntry.Path], "shipped.db")).To(Equal(dbRows(render(dbEntry), "rendered.db")), st.Code)

			events := eventsOf(rec, st.Slug+"-tabular.zip")
			Expect(events).To(HaveLen(units+1), st.Code)
			var unitPaths []string
			for _, ev := range events[:units] {
				unitPaths = append(unitPaths, ev.Unit)
			}
			var wantUnits []string
			for _, e := range entries {
				if isTabularUnit(e.Path) {
					wantUnits = append(wantUnits, e.Path)
				}
			}
			Expect(unitPaths).To(Equal(wantUnits), st.Code)
		}
	})

	It("builds, on a second run into a fresh directory, every Parquet twin byte-identical and every state database content-identical (byte-reproducible)", func() {
		_, err := publish.Run(ctx, db, tabularOnly(out), nil)
		Expect(err).NotTo(HaveOccurred())
		again := filepath.Join(GinkgoT().TempDir(), "again")
		_, err = publish.Run(ctx, db, tabularOnly(again), nil)
		Expect(err).NotTo(HaveOccurred())

		for _, st := range states {
			firstNames, first := tabularZip(out, st)
			secondNames, second := tabularZip(again, st)
			Expect(secondNames).To(Equal(firstNames), st.Code)
			parquets := 0
			for _, name := range firstNames {
				if strings.HasSuffix(name, ".parquet") {
					Expect(second[name]).To(Equal(first[name]), "%s: %s", st.Code, name)
					parquets++
				}
			}
			Expect(parquets).To(Equal(len(parquetPaths())), st.Code)
			dbName := st.Slug + ".db"
			Expect(dbRows(second[dbName], dbName)).To(Equal(dbRows(first[dbName], dbName)), st.Code)
			// Reported, not asserted: SQLite is content-reproducible only.
			GinkgoWriter.Printf("%s across two runs: bytes identical = %t (%d bytes)\n",
				dbName, bytes.Equal(first[dbName], second[dbName]), len(first[dbName]))
		}
	})
})
