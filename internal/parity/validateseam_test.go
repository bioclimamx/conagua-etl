package parity_test

// The parity → validate seam, always on: the comparator reads exactly
// what validate.Run writes. Two databases carry the same
// finding-producing fixture under divergent surrogate ids, this repo's
// verb runs on both, and the comparison must come out clean with every
// reference rule's rows equal to Run's own counts for that rule — so a
// change in what Run writes (the 'validate:' namespace, the NULL
// station handling, the severity literals) cannot slip past the
// comparator specs' hand-seeded imitation of it. An anchor finding on
// one side only proves the native rules are reported, never compared.

import (
	"database/sql"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bioclimamx/conagua-etl/internal/parity"
	"github.com/bioclimamx/conagua-etl/internal/validate"
)

// seamIDs are one side's surrogate station ids: London (a bbox warn),
// a (0, 0) station (the bbox error), Calvillo (the WMO and daily-sanity
// findings), Aguascalientes (the cross-period pair).
type seamIDs struct {
	london, zerozero, calvillo, aguascalientes int64
}

// seamCounts is what each reference rule emits over the seam fixture under
// the default period, written by hand: the stranded run; London's warn
// and the (0, 0) error; January 1992 eleven days short; the 35 °C swing
// as a diurnal finding plus a tmax and a tmin z-score finding over the
// 51 January readings; the one normals pair 5 °C apart.
var seamCounts = map[string][2]int{
	"orphan-runs":            {1, 0},
	"bbox":                   {1, 1},
	"wmo-month-completeness": {1, 0},
	"daily-sanity":           {3, 0},
	"cross-period":           {1, 0},
}

// seedSeam populates one side. Run ids are the same on both sides — the
// orphan-runs issue text quotes them — while the station ids diverge.
func seedSeam(db *sql.DB, ids seamIDs, strandedAt string) {
	GinkgoHelper()
	for _, s := range []struct {
		id       int64
		ext      string
		name     string
		lat, lon float64
	}{
		{ids.london, "outside", "London", 51.5, -0.1},
		{ids.zerozero, "zz", "broken", 0, 0},
		{ids.calvillo, "1002", "CALVILLO", 21.85, -102.72},
		{ids.aguascalientes, "1001", "AGUASCALIENTES (OBS)", 21.85027778, -102.2908333},
	} {
		execSQL(db, `INSERT INTO stations (id, source, external_id, name, lat, lon) VALUES (?, ?, ?, ?, ?, ?)`,
			s.id, seedSource, s.ext, s.name, s.lat, s.lon)
	}
	daily := func(year, month, from, to int, tmax, tmin float64) {
		for d := from; d <= to; d++ {
			execSQL(db, `INSERT INTO daily_observations (station_id, date, tmax, tmin) VALUES (?, ?, ?, ?)`,
				ids.calvillo, fmt.Sprintf("%04d-%02d-%02d", year, month, d), tmax, tmin)
		}
	}
	daily(1991, 1, 1, 30, 25.0, 10.0)
	daily(1991, 1, 31, 31, 35.0, 0.0)
	daily(1992, 1, 1, 20, 25.0, 10.0)
	execSQL(db, `INSERT INTO monthly_normals (station_id, period, month, tmax) VALUES (?, '1981-2010', 1, 25.0), (?, '1991-2020', 1, 30.0)`,
		ids.aguascalientes, ids.aguascalientes)
	execSQL(db, `INSERT INTO ingest_runs (id, started_at, snapshot_date, sink_kind, status) VALUES (1, ?, '2026-07-18', 'local', 'running')`, strandedAt)
	execSQL(db, `INSERT INTO ingest_runs (id, started_at, finished_at, snapshot_date, sink_kind, status)
		VALUES (2, '2026-07-18T10:00:00Z', '2026-07-18T10:30:00Z', '2026-07-18', 'local', 'complete')`)
}

var _ = Describe("CompareValidate over validate.Run's own output", func() {
	It("reads what Run writes on both sides as identical per reference rule, the rows equal to Run's counts, a native finding reported and never compared", func(ctx SpecContext) {
		baseDB, newDB := openIngestDB(), openIngestDB()
		strandedAt := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339)
		seedSeam(baseDB, seamIDs{1, 2, 3, 4}, strandedAt)
		seedSeam(newDB, seamIDs{7, 9, 11, 13}, strandedAt)
		// A normals row naming no station on the new side only: a
		// station-refs error with no reference counterpart.
		execSQL(newDB, `INSERT INTO monthly_normals (station_id, period, month, tmax) VALUES (999, '1991-2020', 6, 20.0)`)

		baseReport, err := validate.Run(ctx, baseDB, validate.Options{})
		Expect(err).NotTo(HaveOccurred())
		newReport, err := validate.Run(ctx, newDB, validate.Options{})
		Expect(err).NotTo(HaveOccurred())

		cmp := mustCompareValidate(ctx, baseDB, newDB)

		Expect(cmp.Clean()).To(BeTrue())
		Expect(cmp.Native).To(Equal([]parity.ValidateNativeRule{{ID: "station-refs", BaseRows: 0, NewRows: 1}}))
		total := 0
		for i, id := range parity.ReferenceValidateRules {
			r := validateRule(cmp, id)
			want := seamCounts[id]
			expectValidateRuleClean(r, want[0]+want[1])
			Expect([]int{r.BaseWarnings, r.BaseErrors, r.NewWarnings, r.NewErrors}).To(Equal([]int{want[0], want[1], want[0], want[1]}), id)

			// The first five slots of a Run report are the reference five in
			// the reference order; each side's rows are its own report's counts.
			for _, rep := range []*validate.Report{baseReport, newReport} {
				rr := rep.PerRule[i]
				Expect(rr.ID).To(Equal(id))
				Expect([]int{rr.Warnings, rr.Errors}).To(Equal([]int{want[0], want[1]}), id)
			}
			total += want[0] + want[1]
		}
		Expect(cmp.BaseRows()).To(Equal(total))
		Expect(cmp.NewRows()).To(Equal(total))
		Expect(cmp.Identical()).To(Equal(total))
		Expect(newReport.WarningsTotal + newReport.ErrorsTotal).To(Equal(total + 1))
		assertValidateRowsAnchor(baseDB, baseReport)
		assertValidateRowsAnchor(newDB, newReport)

		// The keys the comparator built are the natural keys, never the
		// surrogates: London's warn sits under its source/external_id on
		// both sides though the ids differ.
		bbox := validateRule(cmp, "bbox")
		Expect(bbox.Identical).To(Equal(2))
		var londonRows int
		Expect(newDB.QueryRow(`SELECT COUNT(*) FROM parsing_warnings w JOIN stations s ON s.id = w.station_id
			WHERE w.source_file = 'validate:bbox' AND s.external_id = 'outside' AND s.id = 7`).Scan(&londonRows)).To(Succeed())
		Expect(londonRows).To(Equal(1))
	})
})
